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
// sequence number, latency) lives on the EvtClient and is only mutated by a session while it is the client's
// current session.
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

	lastFrame  atomic.Int64 // unix nano
	progressMu sync.Mutex
	progress   time.Time // last dispatch frame, or when the wait for READY/RESUMED started

	hb hbState

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
// READY/RESUMED progress
// ---------------------------------------------------------------------------

func (s *session) setProgress(t time.Time) {
	s.progressMu.Lock()
	if t.After(s.progress) {
		s.progress = t
	}
	s.progressMu.Unlock()
}

func (s *session) lastProgress() time.Time {
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
	s.hb.anchor = time.Time{}
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
	s.setProgress(now)

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

// forward hands an event to the shared event channel. It gives up once the session failed, so a blocked
// consumer can never keep a retired session alive.
func (s *session) forward(evt *Event) bool {
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
	anchor      time.Time // send time of the oldest un-ACKed heartbeat
	lastBeat    time.Time
	lastAck     time.Time
}

// markBeat must run before the heartbeat is handed to the connection, so an ACK racing the write is never lost.
func (s *session) markBeat(now time.Time) {
	s.hb.mu.Lock()
	if !s.hb.outstanding {
		s.hb.outstanding = true
		s.hb.anchor = now
	}
	s.hb.lastBeat = now
	s.hb.mu.Unlock()
}

func (s *session) isZombie(interval time.Duration, now time.Time) bool {
	s.hb.mu.Lock()
	outstanding, anchor := s.hb.outstanding, s.hb.anchor
	s.hb.mu.Unlock()
	return outstanding && now.Sub(anchor) >= interval
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
			if s.isZombie(interval, time.Now()) {
				s.zombie(interval)
				return
			}
		}
	}
}

func (s *session) zombie(interval time.Duration) {
	s.c.log.Info(s.prefix(), "heartbeat ACK was not received within", interval, "reconnecting")
	s.fail(failReason{kind: failZombie})
}

// ---------------------------------------------------------------------------
// writer
// ---------------------------------------------------------------------------

func (s *session) write() {
	defer s.wgWork.Done()
	defer s.alive.writer.Store(false)
	c := s.c

	readyCh := s.ready
	ready := false

	for {
		if s.workCtx.Err() != nil {
			return
		}

		// priority 1: heartbeats
		select {
		case <-s.beatDue:
			if !s.sendHeartbeat() {
				return
			}
			continue
		default:
		}
		select {
		case <-s.beatNow:
			if !s.sendHeartbeat() {
				return
			}
			continue
		default:
		}

		// priority 2: IDENTIFY/RESUME
		select {
		case req := <-s.writeCh:
			if !s.sendControl(req) {
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
		if ready && !c.messageQueue.IsEmpty() {
			if !s.sendCommand() {
				return
			}
			continue
		}

		// nothing to send right now; wake up for new work
		var commands <-chan struct{}
		if ready {
			commands = c.cmdSignal
		}

		ok := true
		select {
		case <-s.workCtx.Done():
			ok = false
		case <-s.beatDue:
			ok = s.sendHeartbeat()
		case <-s.beatNow:
			ok = s.sendHeartbeat()
		case req := <-s.writeCh:
			ok = s.sendControl(req)
		case <-readyCh:
			ready, readyCh = true, nil
		case <-commands:
		}
		if !ok {
			return
		}
	}
}

func (s *session) writeFrame(pkt *clientPacket) error {
	saveOutgoingPacket(s.c.log, s.prefix(), clientTypeEvent, s.c.ShardID, pkt)
	return s.conn.WriteJSON(pkt)
}

func (s *session) sendHeartbeat() bool {
	pkt := &clientPacket{Op: opcode.EventHeartbeat, Data: s.c.heartbeatSeq(), CmdName: event.Heartbeat}
	s.markBeat(time.Now())
	if err := s.writeFrame(pkt); err != nil {
		s.fail(failReason{kind: failTransport, err: fmt.Errorf("heartbeat write: %w", err)})
		return false
	}
	return true
}

func (s *session) sendControl(req *writeReq) bool {
	err := s.writeFrame(req.pkt)
	req.result <- err
	if err != nil {
		s.fail(failReason{kind: failTransport, err: fmt.Errorf("%s write: %w", req.pkt.CmdName, err)})
		return false
	}
	return true
}

func (s *session) sendCommand() bool {
	q := &s.c.messageQueue
	msg := q.Begin()
	if msg == nil {
		return true
	}
	if err := s.writeFrame(msg); err != nil {
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
// at all. Acceptance itself is bounded too.
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
