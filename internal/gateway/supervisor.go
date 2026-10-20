package gateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/aidenwallis/disgord/internal/gateway/event"
	"github.com/aidenwallis/disgord/internal/gateway/opcode"
)

var (
	ErrAlreadyConnected  = errors.New("the shard is already connected or connecting")
	ErrStopping          = errors.New("the shard is still shutting down a previous connection")
	ErrDisconnected      = errors.New("the shard was disconnected")
	ErrNotConnected      = errors.New("the shard is not connected")
	ErrDisconnectTimeout = errors.New("timed out waiting for the shard to disconnect")
)

// timeouts bounds every blocking phase of an event client. Zero fields take their default.
type timeouts struct {
	dial             time.Duration
	hello            time.Duration
	write            time.Duration
	readyIdle        time.Duration // readable time without a dispatch frame while waiting for READY/RESUMED
	readyCeiling     time.Duration // readable time from IDENTIFY to READY
	teardownJoinWarn time.Duration // error log cadence while waiting for session workers to retire
	backoffMin       time.Duration
	backoffMax       time.Duration
	healthyAfter     time.Duration // a session that stayed ready this long resets the backoff
	disconnect       time.Duration

	invalidSessionMin time.Duration
	invalidSessionMax time.Duration

	outboundWindow time.Duration
	outboundLimit  int
}

func defaultTimeouts() timeouts {
	return timeouts{
		dial:              30 * time.Second,
		hello:             20 * time.Second,
		write:             10 * time.Second,
		readyIdle:         60 * time.Second,
		readyCeiling:      10 * time.Minute,
		teardownJoinWarn:  15 * time.Second,
		backoffMin:        time.Second,
		backoffMax:        2 * time.Minute,
		healthyAfter:      60 * time.Second,
		disconnect:        30 * time.Second,
		invalidSessionMin: time.Second,
		invalidSessionMax: 5 * time.Second,
		outboundWindow:    60 * time.Second,
		outboundLimit:     120,
	}
}

func (t timeouts) withDefaults() timeouts {
	d := defaultTimeouts()
	fill := func(v *time.Duration, def time.Duration) {
		if *v <= 0 {
			*v = def
		}
	}
	fill(&t.dial, d.dial)
	fill(&t.hello, d.hello)
	fill(&t.write, d.write)
	fill(&t.readyIdle, d.readyIdle)
	fill(&t.readyCeiling, d.readyCeiling)
	fill(&t.teardownJoinWarn, d.teardownJoinWarn)
	fill(&t.backoffMin, d.backoffMin)
	fill(&t.backoffMax, d.backoffMax)
	fill(&t.healthyAfter, d.healthyAfter)
	fill(&t.disconnect, d.disconnect)
	fill(&t.invalidSessionMin, d.invalidSessionMin)
	fill(&t.invalidSessionMax, d.invalidSessionMax)
	fill(&t.outboundWindow, d.outboundWindow)
	if t.outboundLimit <= 0 {
		t.outboundLimit = d.outboundLimit
	}
	return t
}

// ---------------------------------------------------------------------------
// shard state (observability)
// ---------------------------------------------------------------------------

type shardState int

const (
	stateIdle shardState = iota
	stateConnecting
	stateIdentifying
	stateResuming
	stateReady
	stateBackoff
	stateStopped
)

var shardStateNames = map[shardState]string{
	stateIdle:        "idle",
	stateConnecting:  "connecting",
	stateIdentifying: "identifying",
	stateResuming:    "resuming",
	stateReady:       "ready",
	stateBackoff:     "backoff",
	stateStopped:     "stopped",
}

func (s shardState) String() string {
	return shardStateNames[s]
}

func (c *EvtClient) setState(st shardState, reason string) {
	c.mu.Lock()
	prev, prevReason := c.state, c.stateReason
	c.state, c.stateReason = st, reason
	c.mu.Unlock()
	if prev != st || prevReason != reason {
		msg := fmt.Sprintf("state %s -> %s", prev, st)
		if reason != "" {
			msg += " (" + reason + ")"
		}
		c.log.Info(c.getLogPrefix(), msg)
	}
}

// setDegraded flags back-pressure on the current session.
func (c *EvtClient) setDegraded(s *session, degraded bool) {
	c.mu.Lock()
	if c.current == s {
		c.degraded = degraded
	}
	c.mu.Unlock()
}

// stateSnapshot describes the shard, e.g. "ready", "ready (degraded: backpressure)", "stopped (4004 ...)".
func (c *EvtClient) stateSnapshot() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.state.String()
	if c.stateReason != "" {
		s += " (" + c.stateReason + ")"
	}
	if c.degraded {
		s += " (degraded: backpressure)"
	}
	return s
}

// ---------------------------------------------------------------------------
// run: one Connect() .. Disconnect() lifetime, owned by one supervisor goroutine
// ---------------------------------------------------------------------------

type run struct {
	id     uint64
	ctx    context.Context
	cancel context.CancelFunc

	// first receives the result of the first attempt to reach READY/RESUMED. It is buffered and always written
	// before done is closed, so a fatal error is never masked by ErrDisconnected.
	first     chan error
	firstSent bool // supervisor goroutine only
	done      chan struct{}

	stopping bool // guarded by EvtClient.lifecycleMu
}

func (r *run) sendFirst(err error) {
	if !r.firstSent {
		r.firstSent = true
		r.first <- err
	}
}

func (r *run) waitFirst() error {
	select {
	case err := <-r.first:
		return err
	case <-r.done:
		select {
		case err := <-r.first:
			return err
		default:
			return ErrDisconnected
		}
	}
}

// start registers and launches a new run without blocking.
func (c *EvtClient) start() (*run, error) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.run != nil {
		if c.run.stopping {
			return nil, ErrStopping
		}
		return nil, ErrAlreadyConnected
	}
	if c.evtConf.Endpoint == "" {
		return nil, errors.New("missing websocket endpoint. Must be set before constructing the sockets")
	}
	select {
	case <-c.shutdown:
		return nil, ErrDisconnected
	default:
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &run{
		id:     c.runCounter.Inc(),
		ctx:    ctx,
		cancel: cancel,
		first:  make(chan error, 1),
		done:   make(chan struct{}),
	}
	c.requestedDisconnect.Store(false)
	c.haveConnectedOnce.Store(true)
	c.run = r
	go c.supervise(r)
	return r, nil
}

// Connect connects the shard and blocks until it received READY (or RESUMED). Transient failures are retried
// internally; close codes that can not be recovered from are returned.
func (c *EvtClient) Connect() (err error) {
	r, err := c.start()
	if err != nil {
		return err
	}
	return r.waitFirst()
}

// Disconnect closes the connection (close code 1000) and waits, bounded, for the shard to stop.
func (c *EvtClient) Disconnect() error {
	c.lifecycleMu.Lock()
	r := c.run
	if r == nil {
		c.lifecycleMu.Unlock()
		return ErrNotConnected
	}
	c.markStopping(r)
	c.lifecycleMu.Unlock()
	return c.awaitStop(r)
}

// stopRun stops r if it is still the shard's run. Used by the shard manager, which must never stop a newer run.
func (c *EvtClient) stopRun(r *run) error {
	c.lifecycleMu.Lock()
	if c.run == r {
		c.markStopping(r)
	}
	c.lifecycleMu.Unlock()
	r.cancel()
	return c.awaitStop(r)
}

// markStopping must be called with lifecycleMu held. c.run is only cleared by the supervisor itself, so no new
// run can start until this one has fully retired.
func (c *EvtClient) markStopping(r *run) {
	r.stopping = true
	c.requestedDisconnect.Store(true)
	r.cancel()
}

func (c *EvtClient) awaitStop(r *run) error {
	t := time.NewTimer(c.to.disconnect)
	defer t.Stop()
	select {
	case <-r.done:
		return nil
	case <-t.C:
		c.log.Error(c.getLogPrefix(), "shard did not stop within", c.to.disconnect)
		return ErrDisconnectTimeout
	}
}

func (c *EvtClient) supervise(r *run) {
	defer c.retire(r)
	go func() {
		select {
		case <-c.shutdown:
			r.cancel()
		case <-r.ctx.Done():
		}
	}()

	backoff := c.to.backoffMin
	for attempt := 1; ; attempt++ {
		out := c.attempt(r)
		act := policy(out)
		if act.clearSession {
			c.clearSession()
		}

		switch {
		case act.stop:
			r.sendFirst(ErrDisconnected)
			c.setState(stateStopped, "disconnected")
			return
		case act.fatal:
			err := newCloseError(out.reason.code)
			c.log.Error(c.getLogPrefix(), "shard stopped permanently:", err)
			c.setState(stateStopped, fmt.Sprintf("close code %d", out.reason.code))
			r.sendFirst(err)
			return
		}
		if r.ctx.Err() != nil {
			r.sendFirst(ErrDisconnected)
			c.setState(stateStopped, "disconnected")
			return
		}

		if out.healthyFor >= c.to.healthyAfter {
			backoff = c.to.backoffMin
		} else if backoff *= 2; backoff > c.to.backoffMax {
			backoff = c.to.backoffMax
		}
		delay := backoff/2 + time.Duration(rand.Int63n(int64(backoff/2)+1))
		if act.invalidSession {
			spread := c.to.invalidSessionMax - c.to.invalidSessionMin
			if spread < 0 {
				spread = 0
			}
			if d := c.to.invalidSessionMin + time.Duration(rand.Int63n(int64(spread)+1)); d > delay {
				delay = d
			}
		}

		c.log.Info(c.getLogPrefix(), fmt.Sprintf("connection attempt %d ended while %s: %s; next attempt in %s",
			attempt, out.phase, out.reason, delay.Round(time.Millisecond)))
		c.setState(stateBackoff, out.reason.String())
		if !sleepCtx(r.ctx, delay) {
			r.sendFirst(ErrDisconnected)
			c.setState(stateStopped, "disconnected")
			return
		}
	}
}

// retire runs when the supervisor exits. The run is only marked done once nothing it started can still touch
// the shard: workers are already joined by teardown, and an abandoned dial must have returned.
func (c *EvtClient) retire(r *run) {
	c.detachIdentify(r)
	c.awaitDialRetired(r, nil)
	r.cancel()

	c.lifecycleMu.Lock()
	if c.run == r {
		c.run = nil
	}
	c.lifecycleMu.Unlock()

	r.sendFirst(ErrDisconnected)
	close(r.done)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *EvtClient) clearSession() {
	c.mu.Lock()
	c.sessionID, c.resumeURL = "", ""
	c.seq, c.hasSeq = 0, false
	c.resumeURLFailures = 0
	c.mu.Unlock()
}

// resetSession forgets the session state once the shard has no run anymore (shard manager Disconnect).
func (c *EvtClient) resetSession() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.run != nil {
		return
	}
	c.clearSession()
	c.haveConnectedOnce.Store(false)
}

// endpointFor returns the endpoint for the next connection. RESUMEs use Discord's resume_gateway_url with the
// base endpoint's query parameters, until it failed twice in a row before HELLO.
func (c *EvtClient) endpointFor(resumable bool) (endpoint string, viaResumeURL bool) {
	base := c.evtConf.Endpoint
	c.mu.Lock()
	resumeURL, failures := c.resumeURL, c.resumeURLFailures
	c.mu.Unlock()
	if !resumable || resumeURL == "" || failures >= 2 {
		return base, false
	}

	u, err := url.Parse(resumeURL)
	if err != nil || u.Host == "" {
		return base, false
	}
	if b, err := url.Parse(base); err == nil {
		u.RawQuery = b.RawQuery
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), true
}

func (c *EvtClient) noteResumeURLFailure() {
	c.mu.Lock()
	c.resumeURLFailures++
	c.mu.Unlock()
}

// ---------------------------------------------------------------------------
// attempt: one connection from dial to teardown
// ---------------------------------------------------------------------------

var stopFailure = failReason{kind: failStop, err: ErrDisconnected}

func (c *EvtClient) attempt(r *run) (out outcome) {
	c.mu.Lock()
	resumable := c.sessionID != ""
	c.mu.Unlock()
	endpoint, viaResumeURL := c.endpointFor(resumable)

	out.phase = phaseDial
	if !resumable && !c.awaitAdmissionWindow(r) {
		out.reason = stopFailure
		return out
	}
	if !c.awaitDialRetired(r, r.ctx.Done()) {
		out.reason = stopFailure
		return out
	}

	c.setState(stateConnecting, "")
	conn, err := c.dial(r, endpoint)
	if err != nil {
		if r.ctx.Err() != nil {
			out.reason = stopFailure
			return out
		}
		if viaResumeURL {
			c.noteResumeURLFailure()
		}
		out.reason = failReason{kind: failTransport, err: fmt.Errorf("dial: %w", err)}
		return out
	}

	sess := c.newSession(r)
	sess.conn = conn
	c.mu.Lock()
	c.current = sess
	c.degraded = false
	c.mu.Unlock()

	sess.alive.receiver.Store(true)
	sess.wgRecv.Add(1)
	go sess.receive()
	sess.alive.writer.Store(true)
	sess.wgWork.Add(1)
	go sess.write()

	out = c.establish(r, sess, resumable, viaResumeURL)
	if out.reachedReady {
		select {
		case <-sess.failed:
		case <-r.ctx.Done():
			sess.fail(stopFailure)
		}
		out.healthyFor = time.Since(time.Unix(0, sess.readyAtNano.Load()))
	}

	closeCode := 4000 // keeps the session resumable
	if r.ctx.Err() != nil {
		sess.fail(stopFailure)
		closeCode = 1000
	}
	out.reason = c.teardown(sess, closeCode)
	return out
}

// establish performs HELLO, IDENTIFY/RESUME and waits for READY/RESUMED. On failure the session has failed.
func (c *EvtClient) establish(r *run, sess *session, resumable, viaResumeURL bool) (out outcome) {
	out.phase = phaseHello
	helloTimer := time.NewTimer(c.to.hello)
	var hello helloInfo
	select {
	case hello = <-sess.hello:
	case <-helloTimer.C:
		sess.fail(failReason{kind: failTransport, err: fmt.Errorf("no HELLO within %s", c.to.hello)})
	case <-sess.failed:
	case <-r.ctx.Done():
		sess.fail(stopFailure)
	}
	helloTimer.Stop()
	if sess.isFailed() {
		if viaResumeURL {
			c.noteResumeURLFailure()
		}
		return out
	}
	if viaResumeURL {
		c.mu.Lock()
		c.resumeURLFailures = 0
		c.mu.Unlock()
	}

	sess.interval.Store(int64(hello.interval))
	sess.alive.heartbeat.Store(true)
	sess.wgWork.Add(1)
	go sess.heartbeat(hello.interval)

	var err error
	if resumable {
		out.phase = phaseResume
		c.setState(stateResuming, "")
		c.mu.Lock()
		pkt := &clientPacket{
			Op:      opcode.EventResume,
			Data:    &evtResume{Token: c.evtConf.BotToken, SessionID: c.sessionID, SequenceNr: c.seq},
			CmdName: event.Resume,
		}
		c.mu.Unlock()
		err = sess.writeControl(pkt)
	} else {
		out.phase = phaseIdentify
		c.setState(stateIdentifying, "")
		err = c.awaitIdentify(r, sess)
	}
	if err != nil {
		if r.ctx.Err() != nil {
			sess.fail(stopFailure)
		} else if !errors.Is(err, errWriteNotAccepted) {
			sess.fail(failReason{kind: failTransport, err: err})
		}
		return out
	}

	out.phase = phaseReady
	if !c.awaitReady(r, sess, !resumable) {
		return out
	}

	out.reachedReady = true
	out.phase = phaseSession
	sess.readyAtNano.Store(time.Now().UnixNano())
	c.isConnected.Store(true)
	c.setState(stateReady, "")
	r.sendFirst(nil)
	return out
}

// awaitReady waits for READY/RESUMED with a progress-aware deadline on readable time, so a slow consumer (or a
// long RESUME replay) never causes an abort loop.
func (c *EvtClient) awaitReady(r *run, sess *session, identify bool) bool {
	start := sess.markAt(time.Now())
	sess.setProgress(start)

	every := c.to.readyIdle / 20
	if every < 5*time.Millisecond {
		every = 5 * time.Millisecond
	}
	if every > time.Second {
		every = time.Second
	}
	tick := time.NewTicker(every)
	defer tick.Stop()

	for {
		select {
		case <-sess.ready:
			return true
		case <-sess.failed:
			return false
		case <-r.ctx.Done():
			sess.fail(stopFailure)
			return false
		case <-tick.C:
			now := time.Now()
			if idle := sess.readableSince(sess.lastProgress(), now); idle >= c.to.readyIdle {
				sess.fail(failReason{kind: failReadyTimeout, err: fmt.Errorf("no dispatch for %s", idle.Round(time.Millisecond))})
				return false
			}
			if identify && sess.readableSince(start, now) >= c.to.readyCeiling {
				sess.fail(failReason{kind: failReadyTimeout, err: fmt.Errorf("no READY within %s", c.to.readyCeiling)})
				return false
			}
		}
	}
}

// teardown retires a session: stop the workers, send our close code while the reader is still live, then
// cancel the read and join everything. It returns the arbitrated failure reason.
func (c *EvtClient) teardown(sess *session, closeCode int) failReason {
	sess.fail(failReason{kind: failTransport, err: errTeardown})

	// let the writer finish its current frame so the close frame is not stuck behind it
	waitGroupTimeout(&sess.wgWork, c.to.write+time.Second)

	reason := "reconnecting"
	if closeCode == 1000 {
		reason = "disconnecting"
	}
	sess.sentClose.Store(int32(closeCode))
	if err := sess.conn.CloseWithStatus(closeCode, reason); err != nil {
		c.log.Debug(sess.prefix(), "close handshake:", err)
	}

	sess.readCancel()
	c.joinWorkers(sess)
	final := sess.freeze()

	c.mu.Lock()
	if c.current == sess {
		c.current = nil
		c.degraded = false
	}
	if c.identSess == sess {
		c.identSess = nil
	}
	c.mu.Unlock()
	return final
}

func waitGroupTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// joinWorkers waits for every worker of the session. A straggler is a bug to surface, never a reason to let a
// second session touch the shard state, so this keeps waiting.
func (c *EvtClient) joinWorkers(sess *session) {
	done := make(chan struct{})
	go func() {
		sess.wgRecv.Wait()
		sess.wgWork.Wait()
		close(done)
	}()
	warn := time.NewTicker(c.to.teardownJoinWarn)
	defer warn.Stop()
	start := time.Now()
	for {
		select {
		case <-done:
			return
		case <-warn.C:
			c.log.Error(sess.prefix(), "session workers still running", time.Since(start).Round(time.Second),
				"after teardown started:", sess.aliveWorkers())
		}
	}
}

// ---------------------------------------------------------------------------
// dialing: at most one outstanding Open per shard, across attempts and runs
// ---------------------------------------------------------------------------

// dial opens a fresh connection. The helper goroutine owns the connection until it is handed over; if the
// attempt gives up first, the helper closes a late connection itself and it is never published.
func (c *EvtClient) dial(r *run, endpoint string) (sessionConn, error) {
	conn, err := c.newConn()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(r.ctx, c.to.dial)
	defer cancel()

	retired := make(chan struct{})
	c.mu.Lock()
	c.dialing, c.dialRun = retired, r
	c.mu.Unlock()

	var mu sync.Mutex
	var abandoned, delivered bool
	result := make(chan error, 1)
	go func() {
		defer func() {
			c.mu.Lock()
			if c.dialing == retired {
				c.dialing, c.dialRun = nil, nil
			}
			c.mu.Unlock()
			close(retired)
		}()

		err := conn.Open(ctx, endpoint, nil)

		mu.Lock()
		if abandoned {
			mu.Unlock()
			if err == nil {
				_ = conn.CloseWithStatus(1000, "connection attempt abandoned")
			}
			return
		}
		delivered = true
		mu.Unlock()
		result <- err
	}()

	select {
	case err = <-result:
	case <-ctx.Done():
		mu.Lock()
		if !delivered {
			abandoned = true
			mu.Unlock()
			if r.ctx.Err() != nil {
				return nil, ErrDisconnected
			}
			return nil, fmt.Errorf("no websocket connection within %s", c.to.dial)
		}
		mu.Unlock()
		err = <-result
	}
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// awaitDialRetired waits until no Open is outstanding. With cancel == nil it only waits for an Open started by
// r, logging while it does (used when a run retires).
func (c *EvtClient) awaitDialRetired(r *run, cancel <-chan struct{}) bool {
	warn := time.NewTicker(c.to.teardownJoinWarn)
	defer warn.Stop()
	for {
		c.mu.Lock()
		retired, owner := c.dialing, c.dialRun
		c.mu.Unlock()
		if retired == nil || (cancel == nil && owner != r) {
			return true
		}
		select {
		case <-retired:
		case <-cancel:
			return false
		case <-warn.C:
			c.log.Error(c.getLogPrefix(), "waiting for an abandoned websocket dial to return")
		}
	}
}

// awaitAdmissionWindow delays dialing when the built-in identify queue reports that IDENTIFY is not possible for
// a long time, instead of holding an unidentified socket open for hours.
func (c *EvtClient) awaitAdmissionWindow(r *run) bool {
	if c.identifySync == nil {
		return true
	}
	eta := c.identifySync.admissionETA()
	if eta < time.Minute {
		return true
	}
	delay := eta - (c.to.dial + c.to.hello)
	if delay <= 0 {
		return true
	}
	c.log.Error(c.getLogPrefix(), "identify budget exhausted; delaying the connection by", delay.Round(time.Second))
	c.setState(stateBackoff, "identify budget exhausted")
	return sleepCtx(r.ctx, delay)
}

// ---------------------------------------------------------------------------
// identify slot requests (see ShardConfig.ConnectQueue)
// ---------------------------------------------------------------------------

type ticketState int

const (
	ticketPending ticketState = iota
	ticketExecuting
	ticketDone
	ticketAbandoned
)

// identifyRequest is one outstanding connect queue call. A shard client owns at most one at a time across all
// its runs; it survives session deaths so a shard keeps its place in line while it reconnects.
type identifyRequest struct {
	mu      sync.Mutex
	state   ticketState
	run     *run // attached run; only pending requests can move to another run
	invoked bool

	done chan struct{}
	err  error // result of the queue call, valid after done is closed
}

func (q *identifyRequest) attach(r *run) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.state != ticketPending {
		return false
	}
	q.run = r
	return true
}

// abandoned reports (and latches) that no run wants this identify anymore.
func (q *identifyRequest) abandoned() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.state == ticketPending && q.run == nil {
		q.state = ticketAbandoned
	}
	return q.state == ticketAbandoned
}

func (q *identifyRequest) wasInvoked() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.invoked
}

// requestIdentify registers sess as waiting for IDENTIFY and attaches r to the outstanding request, creating
// one lazily.
func (c *EvtClient) requestIdentify(r *run, sess *session) *identifyRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.identSess = sess
	if q := c.identReq; q != nil && q.attach(r) {
		return q
	}

	q := &identifyRequest{run: r, done: make(chan struct{})}
	c.identReq = q
	go c.runIdentifyRequest(q)
	return q
}

func (c *EvtClient) runIdentifyRequest(q *identifyRequest) {
	cb := func() error { return c.identifyTurn(q) }
	var err error
	if c.identifySync != nil {
		err = c.identifySync.queueIdentify(c.ShardID, cb, q.abandoned)
	} else {
		err = c.connectQueue(c.ShardID, cb)
	}

	q.mu.Lock()
	if q.state == ticketPending {
		q.state = ticketDone
	}
	q.err = err
	q.mu.Unlock()

	c.mu.Lock()
	if c.identReq == q {
		c.identReq = nil
	}
	c.mu.Unlock()
	close(q.done)
}

func (c *EvtClient) detachIdentify(r *run) {
	c.mu.Lock()
	q := c.identReq
	if c.identSess != nil && c.identSess.run == r {
		c.identSess = nil
	}
	c.mu.Unlock()

	if q != nil {
		q.mu.Lock()
		if q.run == r && q.state == ticketPending {
			q.run = nil
		}
		q.mu.Unlock()
	}
}

// identifyTurn is the connect queue callback. It writes IDENTIFY on the attached run's live session and only
// returns once the write finished, so the queue's spacing can never be violated by a late write.
func (c *EvtClient) identifyTurn(q *identifyRequest) error {
	q.mu.Lock()
	if q.state != ticketPending {
		q.mu.Unlock()
		return errIdentifyNotSent
	}
	q.state = ticketExecuting
	q.invoked = true
	r := q.run
	q.mu.Unlock()
	defer func() {
		q.mu.Lock()
		q.state = ticketDone
		q.mu.Unlock()
	}()
	if r == nil {
		return errIdentifyNotSent
	}

	c.mu.Lock()
	sess := c.identSess
	if sess == nil || sess.run != r || c.current != sess || sess.isFailed() {
		c.mu.Unlock()
		return errIdentifyNotSent
	}
	c.identSess = nil
	// Reset before IDENTIFY reaches the writer: READY may be committed before this turn observes the write, and a
	// leftover sequence number would make READY (s=1) look like a duplicate.
	c.sessionID, c.resumeURL = "", ""
	c.seq, c.hasSeq = 0, false
	c.mu.Unlock()

	err := sess.writeControl(c.identifyPacket())
	if errors.Is(err, errWriteNotAccepted) {
		return errIdentifyNotSent
	}
	sess.markIdentified(err)
	return err
}

func (c *EvtClient) identifyPacket() *clientPacket {
	c.idMu.RLock()
	id := *c.identity // copy to avoid a data race with SetPresence
	c.idMu.RUnlock()
	return &clientPacket{Op: opcode.EventIdentify, Data: &id, CmdName: event.Identify}
}

// awaitIdentify waits for this shard's identify turn to write IDENTIFY on sess.
func (c *EvtClient) awaitIdentify(r *run, sess *session) error {
	for regrants := 0; ; regrants++ {
		q := c.requestIdentify(r, sess)
		select {
		case <-sess.identified:
			return sess.identifyErr
		case <-sess.failed:
			return errWriteNotAccepted
		case <-r.ctx.Done():
			return ErrDisconnected
		case <-q.done:
		}

		select {
		case <-sess.identified:
			return sess.identifyErr
		default:
		}
		if !q.wasInvoked() || (q.err != nil && !errors.Is(q.err, errIdentifyNotSent)) || regrants >= 3 {
			if q.err == nil {
				return errors.New("identify queue returned without granting a turn")
			}
			return fmt.Errorf("identify queue: %w", q.err)
		}
		// the turn was granted while no session was ready for it; ask for another one
	}
}

func (c *EvtClient) heartbeatSeq() interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hasSeq {
		return nil
	}
	return c.seq
}
