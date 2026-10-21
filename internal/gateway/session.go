package gateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/aidenwallis/disgord/internal/gateway/event"
	"github.com/aidenwallis/disgord/internal/gateway/opcode"
	"github.com/aidenwallis/disgord/json"
)

// A session is one websocket connection of an event client together with everything that only lives as long as
// that connection: contexts, workers, heartbeat state and handshake signals. Shared shard state (session id,
// sequence number, latency, state) lives on the EvtClient and is only mutated by a session while it
// is the client's current session.
type session struct {
	id  uint64
	c   *EvtClient
	run *run

	// readCtx is only cancelled during teardown, after the close frame was sent; cancelling it earlier makes
	// nhooyr send 1008 itself. workCtx is cancelled by fail() and stops the writer, heartbeats and forwarding.
	readCtx    context.Context
	readCancel context.CancelFunc
	workCtx    context.Context
	workCancel context.CancelFunc

	conn           sessionConn
	wgWork, wgRecv sync.WaitGroup
	alive          struct{ receiver, writer, heartbeat atomic.Bool }

	failed   chan struct{}
	failOnce sync.Once

	reasonMu  sync.Mutex
	reason    failReason
	hasReason bool
	frozen    bool

	hello     chan helloInfo // buffered(1)
	ready     chan struct{}  // closed on READY/RESUMED
	readyOnce sync.Once

	// identified is closed by the identify turn once IDENTIFY was written (or failed to write) on this session
	identified   chan struct{}
	identifyOnce sync.Once
	identifyErr  error

	writeCh chan *writeReq // unbuffered: a completed send means the writer accepted the frame
	beatDue chan struct{}  // buffered(1): scheduled heartbeat
	beatNow chan struct{}  // buffered(1): heartbeat requested by Discord (op 1), coalescing

	interval    atomic.Int64 // heartbeat interval from HELLO, nanoseconds
	readyAtNano atomic.Int64

	stall      stallClock
	lastFrame  atomic.Int64 // unix nano
	progressMu sync.Mutex
	progress   readableMark // last dispatch frame, or the IDENTIFY/RESUME write

	hb hbState

	stallReported bool // heartbeat goroutine only
	lastStallLog  time.Time

	sentClose atomic.Int32 // close code we sent; 0 until teardown sends one
}

type helloInfo struct {
	interval time.Duration
}

type writeReq struct {
	pkt    *clientPacket
	result chan error // buffered(1), always answered once the writer accepted the request
}

var errWriteNotAccepted = errors.New("the session is no longer accepting frames")
var errTeardown = errors.New("session teardown")

func (c *EvtClient) newSession(r *run) *session {
	s := &session{
		id:         c.sessionCounter.Inc(),
		c:          c,
		run:        r,
		failed:     make(chan struct{}),
		hello:      make(chan helloInfo, 1),
		ready:      make(chan struct{}),
		identified: make(chan struct{}),
		writeCh:    make(chan *writeReq),
		beatDue:    make(chan struct{}, 1),
		beatNow:    make(chan struct{}, 1),
	}
	s.readCtx, s.readCancel = context.WithCancel(context.Background())
	s.workCtx, s.workCancel = context.WithCancel(context.Background())
	return s
}

func (s *session) prefix() string {
	return fmt.Sprintf("%s[session:%d]", s.c.getLogPrefix(), s.id)
}

// ---------------------------------------------------------------------------
// failure arbitration
// ---------------------------------------------------------------------------

// fail records a failure reason and stops the session's writer, heartbeat and forwarding. Reasons keep being
// ranked until the receiver has retired, so a fatal close code read right after a write error still wins.
func (s *session) fail(r failReason) {
	s.record(r)
	s.stop()
}

// stop ends the session without recording a reason.
func (s *session) stop() {
	s.failOnce.Do(func() {
		close(s.failed)
		s.workCancel()
		s.c.isConnected.Store(false)
	})
}

func (s *session) record(r failReason) {
	s.reasonMu.Lock()
	defer s.reasonMu.Unlock()
	if s.frozen {
		return
	}
	if !s.hasReason || r.rank() > s.reason.rank() {
		s.reason = r
		s.hasReason = true
	}
}

func (s *session) freeze() failReason {
	s.reasonMu.Lock()
	defer s.reasonMu.Unlock()
	s.frozen = true
	return s.reason
}

func (s *session) isFailed() bool {
	select {
	case <-s.failed:
		return true
	default:
		return false
	}
}

// readFailure classifies a read error. Our own close code echoed back by the peer is not a reason of its own.
func (s *session) readFailure(err error) (r failReason, genuine bool) {
	var closeErr *CloseErr
	if errors.As(err, &closeErr) {
		if sent := s.sentClose.Load(); sent != 0 && int32(closeErr.code) == sent {
			return failReason{}, false
		}
		return failReason{kind: failRemoteClose, code: closeErr.code, err: err}, true
	}
	return failReason{kind: failTransport, err: err}, true
}

func (s *session) markReady() {
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *session) markIdentified(err error) {
	s.identifyOnce.Do(func() {
		s.identifyErr = err
		close(s.identified)
	})
}

// ---------------------------------------------------------------------------
// readable time: wall time minus the time the receiver spent blocked on the event channel
// ---------------------------------------------------------------------------

type stallClock struct {
	mu    sync.Mutex
	total time.Duration
	since time.Time // zero unless currently stalled
}

func (s *stallClock) begin(now time.Time) {
	s.mu.Lock()
	s.since = now
	s.mu.Unlock()
}

func (s *stallClock) end(now time.Time) {
	s.mu.Lock()
	if !s.since.IsZero() {
		s.total += now.Sub(s.since)
		s.since = time.Time{}
	}
	s.mu.Unlock()
}

func (s *stallClock) totalAt(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := s.total
	if !s.since.IsZero() {
		total += now.Sub(s.since)
	}
	return total
}

func (s *stallClock) current(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.since.IsZero() {
		return 0
	}
	return now.Sub(s.since)
}

type readableMark struct {
	at    time.Time
	stall time.Duration
}

func (s *session) markAt(now time.Time) readableMark {
	return readableMark{at: now, stall: s.stall.totalAt(now)}
}

func (s *session) readableSince(m readableMark, now time.Time) time.Duration {
	d := now.Sub(m.at) - (s.stall.totalAt(now) - m.stall)
	if d < 0 {
		return 0
	}
	return d
}

func (s *session) setProgress(m readableMark) {
	s.progressMu.Lock()
	if m.at.After(s.progress.at) {
		s.progress = m
	}
	s.progressMu.Unlock()
}

func (s *session) lastProgress() readableMark {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	return s.progress
}

// ---------------------------------------------------------------------------
// receiver
// ---------------------------------------------------------------------------

func (s *session) receive() {
	defer s.wgRecv.Done()
	defer s.alive.receiver.Store(false)
	c := s.c

	var badFrames int
	for {
		data, err := s.conn.Read(s.readCtx)
		if err != nil {
			if r, genuine := s.readFailure(err); genuine {
				s.fail(r)
			} else {
				s.stop()
			}
			return
		}
		now := time.Now()
		s.lastFrame.Store(now.UnixNano())
		c.lastFrameAt.Store(now.UnixNano())

		p := c.pktPool.Get().(*DiscordPacket)
		p.reset()
		if err = json.Unmarshal(data, p); err != nil {
			c.pktPool.Put(p)
			c.log.Error(s.prefix(), err, "SKIPPED ERRONEOUS PACKET CONTENT:", string(data))
			if badFrames++; badFrames >= 10 {
				s.fail(failReason{kind: failTransport, err: errors.New("10 consecutive packets could not be decoded")})
			}
			continue
		}
		badFrames = 0
		saveIncomingPacker(c.log, s.prefix(), clientTypeEvent, c.ShardID, p, data)

		s.handle(p, now)
		c.pktPool.Put(p)
	}
}

func (s *session) handle(p *DiscordPacket, now time.Time) {
	c := s.c
	switch p.Op {
	case opcode.EventHeartbeatAck:
		s.onHeartbeatAck(now)
	case opcode.EventHeartbeat:
		select {
		case s.beatNow <- struct{}{}:
		default:
		}
	case opcode.EventHello:
		hello := &helloPacket{}
		if err := json.Unmarshal(p.Data, hello); err != nil || hello.HeartbeatInterval == 0 {
			s.fail(failReason{kind: failTransport, err: fmt.Errorf("invalid HELLO payload %q: %v", string(p.Data), err)})
			return
		}
		select {
		case s.hello <- helloInfo{interval: time.Duration(hello.HeartbeatInterval) * time.Millisecond}:
		default:
		}
	case opcode.EventReconnect:
		c.log.Info(s.prefix(), "Discord requested a reconnect")
		s.fail(failReason{kind: failReconnect})
	case opcode.EventInvalidSession:
		var resumable bool
		_ = json.Unmarshal(p.Data, &resumable)
		c.log.Info(s.prefix(), "Discord invalidated the session, resumable:", resumable)
		s.fail(failReason{kind: failInvalidSession, resumable: resumable})
	case opcode.EventDiscordEvent:
		s.onDispatch(p, now)
	default:
		c.log.Debug(s.prefix(), "ignoring unsupported operation code", p.Op)
	}
}

func (s *session) onHeartbeatAck(now time.Time) {
	s.hb.mu.Lock()
	s.hb.lastAck = now
	var latency time.Duration
	if !s.hb.lastBeat.IsZero() {
		latency = now.Sub(s.hb.lastBeat)
	}
	s.hb.outstanding = false
	s.hb.anchor = readableMark{}
	s.hb.mu.Unlock()

	if latency > 0 {
		c := s.c
		c.mu.Lock()
		if c.current == s {
			c.latency = latency
		}
		c.mu.Unlock()
	}
}

func (s *session) onDispatch(p *DiscordPacket, now time.Time) {
	if s.workCtx.Err() != nil {
		// draining after fail(): the checkpoint is not advanced, so a RESUME replays this event
		return
	}
	c := s.c
	s.setProgress(s.markAt(now))

	c.mu.Lock()
	if c.current != s {
		c.mu.Unlock()
		return
	}
	hasSeq, seq := c.hasSeq, c.seq
	c.mu.Unlock()

	// READY/RESUMED end the handshake even if their sequence number is missing or not ahead of ours; dropping
	// them as duplicates would make every RESUME time out.
	handshake := p.EventName == event.Ready || p.EventName == event.Resumed
	if !handshake && hasSeq && p.SequenceNumber <= seq {
		c.log.Debug(s.prefix(), "dropping duplicate event", p.EventName, "seq", p.SequenceNumber, "<=", seq)
		return
	}
	if hasSeq && p.SequenceNumber > seq+1 {
		c.log.Info(s.prefix(), fmt.Sprintf("WARN: sequence gap, got %d after %d (%s)", p.SequenceNumber, seq, p.EventName))
	}

	var ready *evtReadyPacket
	if p.EventName == event.Ready {
		ready = &evtReadyPacket{}
		if err := json.Unmarshal(p.Data, ready); err != nil {
			c.log.Error(s.prefix(), "unable to parse READY, the session can not be resumed:", err)
			ready = &evtReadyPacket{}
		}
	}

	if c.eventOfInterest(p.EventName) {
		evt := &Event{Name: p.EventName, Data: p.Data, ShardID: c.ShardID}
		if !s.forward(evt) {
			return
		}
	}

	// only advance the resume checkpoint once the event was handed over
	c.mu.Lock()
	if c.current == s {
		if p.SequenceNumber > 0 && (!c.hasSeq || p.SequenceNumber > c.seq) {
			c.seq = p.SequenceNumber
			c.hasSeq = true
		}
		if ready != nil {
			c.sessionID = ready.SessionID
			c.ReadyCounter++
		}
	}
	c.mu.Unlock()

	if handshake {
		s.markReady()
	}
}

// forward hands an event to the shared event channel. Time spent blocked here is excluded from readable time:
// a slow consumer must not look like a dead connection.
func (s *session) forward(evt *Event) bool {
	select {
	case s.c.eventChan <- evt:
		return true
	default:
	}

	s.stall.begin(time.Now())
	defer func() { s.stall.end(time.Now()) }()
	select {
	case s.c.eventChan <- evt:
		return true
	case <-s.workCtx.Done():
		return false
	}
}

// ---------------------------------------------------------------------------
// heartbeat
// ---------------------------------------------------------------------------

type hbState struct {
	mu          sync.Mutex
	outstanding bool
	anchor      readableMark // send time of the oldest un-ACKed heartbeat
	lastBeat    time.Time
	lastAck     time.Time
}

// markBeat must run before the heartbeat is handed to the connection, so an ACK racing the write is never lost.
func (s *session) markBeat(now time.Time) {
	s.hb.mu.Lock()
	if !s.hb.outstanding {
		s.hb.outstanding = true
		s.hb.anchor = s.markAt(now)
	}
	s.hb.lastBeat = now
	s.hb.mu.Unlock()
}

func (s *session) isZombie(interval time.Duration, now time.Time) bool {
	s.hb.mu.Lock()
	outstanding, anchor := s.hb.outstanding, s.hb.anchor
	s.hb.mu.Unlock()
	return outstanding && s.readableSince(anchor, now) >= interval
}

func (s *session) heartbeat(interval time.Duration) {
	defer s.wgWork.Done()
	defer s.alive.heartbeat.Store(false)

	checkEvery := interval / 20
	if checkEvery < 5*time.Millisecond {
		checkEvery = 5 * time.Millisecond
	}
	if checkEvery > time.Second {
		checkEvery = time.Second
	}

	next := time.NewTimer(time.Duration(rand.Int63n(int64(interval))))
	defer next.Stop()
	check := time.NewTicker(checkEvery)
	defer check.Stop()

	for {
		select {
		case <-s.workCtx.Done():
			return
		case <-next.C:
			if s.isZombie(interval, time.Now()) {
				s.zombie(interval)
				return
			}
			select {
			case s.beatDue <- struct{}{}:
			default:
			}
			next.Reset(interval)
		case <-check.C:
			now := time.Now()
			if s.isZombie(interval, now) {
				s.zombie(interval)
				return
			}
			s.observeStall(interval, now)
		}
	}
}

func (s *session) zombie(interval time.Duration) {
	s.c.log.Info(s.prefix(), "heartbeat ACK was not received within", interval, "of readable time, reconnecting")
	s.fail(failReason{kind: failZombie})
}

// observeStall reports back-pressure. The session is intentionally never failed because of it: reconnecting
// cannot unblock the consumer and would only lose the session or events.
func (s *session) observeStall(interval time.Duration, now time.Time) {
	c := s.c
	stalled := s.stall.current(now)
	switch {
	case stalled > interval && !s.stallReported:
		s.stallReported = true
		s.lastStallLog = now
		c.setDegraded(s, true)
		c.log.Info(s.prefix(), "WARN: event consumer is not keeping up; the shard has been blocked for", stalled.Round(time.Millisecond),
			"(heartbeats continue, no events are lost)")
	case s.stallReported && stalled > 0 && now.Sub(s.lastStallLog) >= 5*time.Minute:
		s.lastStallLog = now
		c.log.Error(s.prefix(), "event consumer has blocked the shard for", stalled.Round(time.Second))
	case s.stallReported && stalled == 0:
		s.stallReported = false
		c.setDegraded(s, false)
		c.log.Info(s.prefix(), "event consumer caught up again")
	}
}

// ---------------------------------------------------------------------------
// writer
// ---------------------------------------------------------------------------

// outboundBudget is a rolling window over every frame sent on this connection (Discord allows 120 per 60s).
type outboundBudget struct {
	window time.Duration
	sent   []time.Time
}

func (b *outboundBudget) used(now time.Time) int {
	limit := now.Add(-b.window)
	var expired int
	for expired < len(b.sent) && !b.sent[expired].After(limit) {
		expired++
	}
	if expired > 0 {
		n := copy(b.sent, b.sent[expired:])
		b.sent = b.sent[:n]
	}
	return len(b.sent)
}

func (b *outboundBudget) record(now time.Time) {
	b.sent = append(b.sent, now)
}

// freeAt returns when fewer than limit frames will be inside the window.
func (b *outboundBudget) freeAt(now time.Time, limit int) time.Time {
	n := b.used(now)
	if n < limit {
		return now
	}
	if limit <= 0 {
		limit = 1
	}
	return b.sent[n-limit].Add(b.window)
}

// outboundLimits derives how many frames user commands and control frames may use, so that the scheduled
// heartbeats always fit into the hard cap: reserve = ceil(window/interval) + 4.
func outboundLimits(interval, window time.Duration, hardCap int) (user, control int) {
	beats := hardCap / 4 // unknown interval: be conservative
	if interval > 0 {
		beats = int((window + interval - 1) / interval)
	}
	user = hardCap - beats - 4
	control = hardCap - beats - 2
	if user < 0 {
		user = 0
	}
	if control < 1 {
		control = 1
	}
	return user, control
}

func (s *session) write() {
	defer s.wgWork.Done()
	defer s.alive.writer.Store(false)
	c := s.c

	budget := &outboundBudget{window: c.to.outboundWindow}
	hardCap := c.to.outboundLimit
	readyCh := s.ready
	ready := false

	for {
		if s.workCtx.Err() != nil {
			return
		}
		now := time.Now()
		used := budget.used(now)
		userLimit, controlLimit := outboundLimits(time.Duration(s.interval.Load()), budget.window, hardCap)

		// priority 1: heartbeats
		select {
		case <-s.beatDue:
			if !s.sendHeartbeat(budget, true) {
				return
			}
			continue
		default:
		}
		select {
		case <-s.beatNow:
			if !s.sendHeartbeat(budget, false) {
				return
			}
			continue
		default:
		}

		// priority 2: IDENTIFY/RESUME
		var controlCh chan *writeReq
		if used < controlLimit {
			controlCh = s.writeCh
		}
		select {
		case req := <-controlCh:
			if !s.sendControl(budget, req) {
				return
			}
			continue
		default:
		}

		// priority 3: user commands, only once READY/RESUMED was received
		if !ready {
			select {
			case <-readyCh:
				ready, readyCh = true, nil
			default:
			}
		}
		pending := ready && !c.messageQueue.IsEmpty()
		if pending && used < userLimit {
			if !s.sendCommand(budget) {
				return
			}
			continue
		}

		// nothing can be sent right now; wake up for new work or when the budget frees up
		var wake *time.Timer
		var wakeC <-chan time.Time
		if controlCh == nil || pending {
			limit := userLimit
			if controlCh == nil {
				limit = controlLimit
			}
			wake = time.NewTimer(budget.freeAt(now, limit).Sub(now))
			wakeC = wake.C
		}
		var commands <-chan struct{}
		if ready && !pending {
			commands = c.cmdSignal
		}

		ok := true
		select {
		case <-s.workCtx.Done():
			ok = false
		case <-s.beatDue:
			ok = s.sendHeartbeat(budget, true)
		case <-s.beatNow:
			ok = s.sendHeartbeat(budget, false)
		case req := <-controlCh:
			ok = s.sendControl(budget, req)
		case <-readyCh:
			ready, readyCh = true, nil
		case <-commands:
		case <-wakeC:
		}
		if wake != nil {
			wake.Stop()
		}
		if !ok {
			return
		}
	}
}

func (s *session) writeFrame(budget *outboundBudget, pkt *clientPacket) error {
	// the frame counts against the budget even if the write fails: it may have reached Discord
	budget.record(time.Now())
	saveOutgoingPacket(s.c.log, s.prefix(), clientTypeEvent, s.c.ShardID, pkt)
	return s.conn.WriteJSON(pkt)
}

// sendHeartbeat writes a heartbeat. Heartbeats may use the whole budget. A scheduled heartbeat waits for budget
// if that still meets its deadline; a heartbeat requested by Discord (op 1) is never silently ignored: if the hard
// cap is exhausted the session is reconnected instead.
func (s *session) sendHeartbeat(budget *outboundBudget, scheduled bool) bool {
	c := s.c
	interval := time.Duration(s.interval.Load())
	hardCap := c.to.outboundLimit
	now := time.Now()

	if scheduled {
		if budget.used(now) >= hardCap {
			s.hb.mu.Lock()
			deadline := s.hb.lastBeat.Add(interval)
			s.hb.mu.Unlock()
			at := budget.freeAt(now, hardCap)
			if at.After(deadline) {
				c.log.Error(s.prefix(), "outbound rate limit exhausted, the next heartbeat can not be sent in time; reconnecting")
				s.fail(failReason{kind: failOutboundExhausted})
				return false
			}
			t := time.NewTimer(at.Sub(now))
			select {
			case <-t.C:
			case <-s.workCtx.Done():
				t.Stop()
				return false
			}
		}
	} else if budget.used(now) >= hardCap {
		// requested heartbeats may use the reserve meant for scheduled ones; the worst case is this reconnect,
		// never a missed heartbeat deadline or more than hardCap frames per window
		c.log.Error(s.prefix(), "outbound rate limit exhausted, a requested heartbeat can not be answered; reconnecting")
		s.fail(failReason{kind: failOutboundExhausted})
		return false
	}

	pkt := &clientPacket{Op: opcode.EventHeartbeat, Data: c.heartbeatSeq(), CmdName: event.Heartbeat}
	s.markBeat(time.Now())
	if err := s.writeFrame(budget, pkt); err != nil {
		s.fail(failReason{kind: failTransport, err: fmt.Errorf("heartbeat write: %w", err)})
		return false
	}
	return true
}

func (s *session) sendControl(budget *outboundBudget, req *writeReq) bool {
	err := s.writeFrame(budget, req.pkt)
	req.result <- err
	if err != nil {
		s.fail(failReason{kind: failTransport, err: fmt.Errorf("%s write: %w", req.pkt.CmdName, err)})
		return false
	}
	return true
}

func (s *session) sendCommand(budget *outboundBudget) bool {
	q := &s.c.messageQueue
	msg := q.Begin()
	if msg == nil {
		return true
	}
	if err := s.writeFrame(budget, msg); err != nil {
		q.Rollback(msg)
		s.fail(failReason{kind: failTransport, err: fmt.Errorf("%s write: %w", msg.CmdName, err)})
		return false
	}
	q.Commit(msg)
	return true
}

// writeControl hands a control frame to the writer and waits for the write to complete. errWriteNotAccepted
// means the frame was definitely not written and never will be: writeCh is unbuffered, so the hand-over either
// happens (the writer then always writes and answers, bounded by its per-write deadline) or it does not happen
// at all. Acceptance itself is bounded too, e.g. while the outbound budget holds control frames back.
func (s *session) writeControl(pkt *clientPacket, acceptWithin time.Duration) error {
	req := &writeReq{pkt: pkt, result: make(chan error, 1)}
	t := time.NewTimer(acceptWithin)
	defer t.Stop()
	select {
	case s.writeCh <- req:
	case <-s.workCtx.Done():
		return errWriteNotAccepted
	case <-t.C:
		return fmt.Errorf("%w within %s", errWriteNotAccepted, acceptWithin)
	}
	return <-req.result
}

func (s *session) aliveWorkers() string {
	var names []string
	if s.alive.receiver.Load() {
		names = append(names, "receiver")
	}
	if s.alive.writer.Load() {
		names = append(names, "writer")
	}
	if s.alive.heartbeat.Load() {
		names = append(names, "heartbeat")
	}
	return strings.Join(names, ",")
}
