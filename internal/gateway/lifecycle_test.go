//go:build !integration
// +build !integration

package gateway

import (
	stdjson "encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/aidenwallis/disgord/internal/gateway/opcode"
	"github.com/aidenwallis/disgord/internal/logger"
)

// Deterministic lifecycle tests on a scripted connection.

func connectScripted(t *testing.T, ss *scriptShard, intervalMs int) *scriptConn {
	t.Helper()
	connected := ss.connectAsync()
	sc := ss.d.await(t)
	sc.hello(intervalMs)
	waitOp(t, sc, 2, 1)
	sc.ready("sess-1")
	if err := awaitErr(t, connected); err != nil {
		t.Fatal(err)
	}
	return sc
}

func ackHeartbeats(sc *scriptConn, p *clientPacket) error {
	if p.Op == opcode.EventHeartbeat {
		sc.send(map[string]interface{}{"op": 11})
	}
	return nil
}

func currentSession(c *EvtClient) *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// 18 (C5): heartbeat payload is the last sequence number, or null before the first dispatch.
func TestLifecycle18_HeartbeatPayload(t *testing.T) {
	ss := newScriptShard(t, nil, func(sc *scriptConn) { sc.onWrite = ackHeartbeats }, nil)
	connected := ss.connectAsync()
	sc := ss.d.await(t)
	sc.hello(30)
	waitOp(t, sc, 1, 1) // READY is held back until a heartbeat was sent
	var b []byte
	for _, p := range sc.writes() {
		if p.Op == opcode.EventHeartbeat {
			b, _ = stdjson.Marshal(p)
			break
		}
	}
	if string(b) != `{"op":1,"d":null}` {
		t.Fatalf("expected a null heartbeat before any dispatch, got %s", b)
	}

	waitOp(t, sc, 2, 1)
	sc.ready("sess-1")
	if err := awaitErr(t, connected); err != nil {
		t.Fatal(err)
	}
	sc.dispatch(2, "MESSAGE_CREATE", `{}`)
	if !waitFor(5*time.Second, func() bool {
		for _, p := range sc.writes() {
			if b, _ := stdjson.Marshal(p); string(b) == `{"op":1,"d":2}` {
				return true
			}
		}
		return false
	}) {
		t.Fatal(`expected a heartbeat with "d":2`)
	}
}

// 25: duplicates are dropped, gaps are accepted.
func TestLifecycle25_SequenceHandling(t *testing.T) {
	ss := newScriptShard(t, nil, nil, nil)
	sc := connectScripted(t, ss, 45000)
	sc.dispatch(2, "MESSAGE_CREATE", `{"id":"2"}`)
	sc.dispatch(2, "MESSAGE_CREATE", `{"id":"dup"}`)
	sc.dispatch(1, "MESSAGE_CREATE", `{"id":"old"}`)
	sc.dispatch(5, "MESSAGE_CREATE", `{"id":"5"}`)
	if !waitFor(5*time.Second, func() bool { return len(ss.events) == 3 }) {
		t.Fatalf("expected READY, 2 and 5 to be delivered, got %d events", len(ss.events))
	}
	time.Sleep(50 * time.Millisecond)
	var names []string
	for len(ss.events) > 0 {
		e := <-ss.events
		names = append(names, e.Name+string(e.Data))
	}
	if len(names) != 3 || names[1] != `MESSAGE_CREATE{"id":"2"}` || names[2] != `MESSAGE_CREATE{"id":"5"}` {
		t.Fatalf("unexpected deliveries %v", names)
	}
	ss.c.mu.Lock()
	seq := ss.c.seq
	ss.c.mu.Unlock()
	if seq != 5 {
		t.Fatalf("expected seq 5, got %d", seq)
	}
}

// 32: IDENTIFY with a leftover sequence number: READY (s=1) must not be dropped as a duplicate.
func TestLifecycle32_LeftoverSeqOnIdentify(t *testing.T) {
	ss := newScriptShard(t, nil, nil, nil)
	ss.c.mu.Lock()
	ss.c.seq, ss.c.hasSeq = 7, true // no session id, so the next connection identifies
	ss.c.mu.Unlock()
	connectScripted(t, ss, 45000)
	if e := <-ss.events; e.Name != "READY" {
		t.Fatalf("expected READY to be delivered, got %s", e.Name)
	}
}

// 41: READY is received and committed before the identify turn observes the write completion.
func TestLifecycle41_ReadyBeforeWriteCompletion(t *testing.T) {
	ss := newScriptShard(t, nil, func(sc *scriptConn) {
		sc.onWrite = func(sc *scriptConn, p *clientPacket) error {
			if p.Op == opcode.EventIdentify && sc.id == 0 {
				sc.ready("sess-early")
				time.Sleep(100 * time.Millisecond) // READY is processed while the write is still "in progress"
			}
			return nil
		}
	}, nil)
	connected := ss.connectAsync()
	sc := ss.d.await(t)
	sc.hello(45000)
	if err := awaitErr(t, connected); err != nil {
		t.Fatal(err)
	}
	ss.c.mu.Lock()
	sid, seq, hasSeq := ss.c.sessionID, ss.c.seq, ss.c.hasSeq
	ss.c.mu.Unlock()
	if sid != "sess-early" || seq != 1 || !hasSeq {
		t.Fatalf("the READY checkpoint was lost: session=%q seq=%d hasSeq=%v", sid, seq, hasSeq)
	}

	sc.send(map[string]interface{}{"op": 7, "d": nil})
	sc2 := ss.d.await(t)
	sc2.hello(45000)
	waitOp(t, sc2, 6, 1)
	if sc2.countOp(2) != 0 {
		t.Fatal("expected a RESUME, not an IDENTIFY")
	}
}

// 23/40: a genuine close code read just after a generic write error wins, in both schedules.
func TestLifecycle40_CloseCodeAfterWriteError(t *testing.T) {
	for _, tc := range []struct {
		name           string
		code           int
		supervisorWins bool // the write error starts teardown before the reader sees the close frame
	}{
		{"4004-reader-first", 4004, false},
		{"4004-supervisor-first", 4004, true},
		{"4014-supervisor-first", 4014, true},
		{"4009-supervisor-first", 4009, true}, // test 23: session-invalidating code beats the write error
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var armed atomic.Bool
			ss := newScriptShard(t, nil, func(sc *scriptConn) {
				if sc.id != 0 {
					sc.onWrite = ackHeartbeats
					return
				}
				sc.holdErrUntilClose = tc.supervisorWins
				sc.onWrite = func(sc *scriptConn, p *clientPacket) error {
					if p.Op == opcode.EventHeartbeat && armed.Load() {
						sc.remoteClose(tc.code)
						if !tc.supervisorWins {
							time.Sleep(50 * time.Millisecond) // the reader records the close first
						}
						return errors.New("broken pipe")
					}
					return ackHeartbeats(sc, p)
				}
			}, nil)
			sc := connectScripted(t, ss, 50)
			armed.Store(true)

			if tc.code == 4009 {
				sc2 := ss.d.await(t)
				sc2.hello(45000)
				waitOp(t, sc2, 2, 1)
				if sc2.countOp(6) != 0 {
					t.Fatal("4009 must clear the session and IDENTIFY")
				}
				return
			}
			if !waitFor(10*time.Second, func() bool {
				return ss.c.stateSnapshot() == "stopped (close code 4004)" ||
					ss.c.stateSnapshot() == "stopped (close code 4014)"
			}) {
				t.Fatalf("expected the shard to stop on the fatal close code, state=%q", ss.c.stateSnapshot())
			}
			time.Sleep(100 * time.Millisecond)
			if n := ss.d.count(); n != 1 {
				t.Fatalf("no reconnect may happen after a fatal close code, got %d connections", n)
			}
			if sc.sentCloseCode() != 4000 && !tc.supervisorWins {
				t.Logf("close code sent: %d", sc.sentCloseCode())
			}
		})
	}
}

// 33: zombie detection from a fixed anchor despite a 10% receiver stall every interval; an ACK racing the
// heartbeat write completion is never lost.
func TestLifecycle33_ZombieWithPeriodicStall(t *testing.T) {
	const interval = 200 * time.Millisecond
	ss := newScriptShard(t, nil, nil, nil)
	ss.events = make(chan *Event) // replaced before Connect: unbuffered so forwarding stalls
	ss.c.eventChan = ss.events
	stopConsumer := make(chan struct{})
	defer close(stopConsumer)
	go func() {
		for {
			select {
			case <-ss.events:
				time.Sleep(interval / 40)
			case <-stopConsumer:
				return
			}
		}
	}()

	sc := connectScripted(t, ss, int(interval/time.Millisecond))
	stopFeed := make(chan struct{})
	defer close(stopFeed)
	go func() {
		// four short stalls per interval (10% in total), so every interval between two beats contains a stall:
		// the second event of each pair blocks the receiver while the consumer is busy with the first
		tick := time.NewTicker(interval / 4)
		defer tick.Stop()
		for seq := 2; ; seq += 2 {
			select {
			case <-stopFeed:
				return
			case <-tick.C:
				sc.dispatch(seq, "MESSAGE_CREATE", `{}`)
				sc.dispatch(seq+1, "MESSAGE_CREATE", `{}`)
			}
		}
	}()

	waitOp(t, sc, 1, 1) // never ACKed
	firstBeat := time.Now()
	sc2 := ss.d.await(t)
	if d := time.Since(firstBeat); d > 4*interval {
		t.Fatalf("zombie detected after %s; expected about 1.11 intervals of readable time", d)
	}
	_ = sc2
}

func TestLifecycle33_AckRacingWriteCompletion(t *testing.T) {
	const interval = 50 * time.Millisecond
	ss := newScriptShard(t, nil, func(sc *scriptConn) {
		sc.onWrite = func(sc *scriptConn, p *clientPacket) error {
			if p.Op == opcode.EventHeartbeat {
				sc.send(map[string]interface{}{"op": 11})
				time.Sleep(5 * time.Millisecond) // the ACK is processed before the write returns
			}
			return nil
		}
	}, nil)
	sc := connectScripted(t, ss, int(interval/time.Millisecond))
	waitOp(t, sc, 1, 10)
	if n := ss.d.count(); n != 1 {
		t.Fatalf("an ACK racing the write completion caused a false zombie reconnect (%d connections)", n)
	}
}

// 34: the first result is always delivered before done, so a fatal error is never masked by ErrDisconnected.
func TestLifecycle34_FirstBeforeDone(t *testing.T) {
	fatal := newCloseError(4004)
	for i := 0; i < 1000; i++ {
		r := &run{first: make(chan error, 1), done: make(chan struct{})}
		r.sendFirst(fatal)
		close(r.done)
		if err := r.waitFirst(); err != fatal {
			t.Fatalf("iteration %d: expected the fatal error, got %v", i, err)
		}
	}
}

// 31: Connect while the previous run is still stopping returns ErrStopping, never a second supervisor.
func TestLifecycle31_ConnectWhileStopping(t *testing.T) {
	to := fastTimeouts()
	to.disconnect = 100 * time.Millisecond
	ss := newScriptShard(t, to, func(sc *scriptConn) {
		if sc.id == 0 {
			sc.readDelayAfterClose = time.Second // the old session retires slowly
		}
	}, nil)
	connectScripted(t, ss, 45000)

	if err := ss.c.Disconnect(); !errors.Is(err, ErrDisconnectTimeout) {
		t.Fatalf("expected ErrDisconnectTimeout while the session retires, got %v", err)
	}
	if err := ss.c.Connect(); !errors.Is(err, ErrStopping) {
		t.Fatalf("expected ErrStopping while the previous run retires, got %v", err)
	}
	if !waitFor(10*time.Second, func() bool {
		ss.c.lifecycleMu.Lock()
		defer ss.c.lifecycleMu.Unlock()
		return ss.c.run == nil
	}) {
		t.Fatal("the run never retired")
	}
	connected := ss.connectAsync()
	sc := ss.d.await(t)
	sc.hello(45000)
	waitOp(t, sc, 6, 1) // a shard-level Disconnect keeps the session (the shard manager resets it)
	sc.dispatch(2, "RESUMED", `{}`)
	if err := awaitErr(t, connected); err != nil {
		t.Fatal(err)
	}
}

// 46: dials that ignore their ctx (legacy proxies) use bounded resources: at most maxAbandonedDials are left
// running, Disconnect returns promptly, attempts resume (and a later Connect works) once a slot frees up, and late
// connections are closed without ever being used. If all of them stay stuck forever, no further dial is made.
func TestLifecycle46_StalledDialsIgnoringCtx(t *testing.T) {
	to := fastTimeouts()
	to.dial = 50 * time.Millisecond
	to.disconnect = 2 * time.Second
	block := make(chan struct{})
	var live, maxLive atomic.Int32
	ss := newScriptShard(t, to, func(sc *scriptConn) {
		sc.openBlock = block
		sc.live, sc.maxLive = &live, &maxLive
	}, nil)

	connected := ss.connectAsync()
	if !waitFor(10*time.Second, func() bool { return ss.d.count() == maxAbandonedDials }) {
		t.Fatalf("expected the shard to keep attempting up to %d stuck dials, got %d", maxAbandonedDials, ss.d.count())
	}
	time.Sleep(20 * to.dial) // many more dial timeouts elapse
	if n := ss.d.count(); n != maxAbandonedDials {
		t.Fatalf("expected at most %d outstanding dials, got %d connection attempts", maxAbandonedDials, n)
	}

	start := time.Now()
	if err := ss.c.Disconnect(); err != nil {
		t.Fatalf("Disconnect must not wait for abandoned dials: %v", err)
	}
	if d := time.Since(start); d > to.disconnect {
		t.Fatalf("Disconnect took %s", d)
	}
	if err := awaitErr(t, connected); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("expected ErrDisconnected, got %v", err)
	}

	// a new run waits for a free dial slot instead of piling up more stuck dials
	connected = ss.connectAsync()
	time.Sleep(10 * to.dial)
	if n := ss.d.count(); n != maxAbandonedDials {
		t.Fatalf("a new run must wait for a free dial slot, got %d connection attempts", n)
	}
	var stuck []*scriptConn
	for i := 0; i < maxAbandonedDials; i++ {
		stuck = append(stuck, ss.d.await(t))
	}
	close(block) // the stuck dials complete late
	sc := ss.d.await(t)
	sc.hello(45000)
	waitOp(t, sc, 2, 1)
	sc.ready("sess-2")
	if err := awaitErr(t, connected); err != nil {
		t.Fatal(err)
	}
	for _, late := range stuck {
		if !waitFor(5*time.Second, late.Disconnected) {
			t.Fatalf("late connection %d was not closed", late.id)
		}
		if len(late.writes()) != 0 {
			t.Fatalf("late connection %d was used", late.id)
		}
	}
	if n := maxLive.Load(); n > maxAbandonedDials {
		t.Fatalf("%d dials were running at once, the cap is %d", n, maxAbandonedDials)
	}
}

// 36: the next session never starts before every worker of the previous one has retired.
func TestLifecycle36_RetirementBeforeReplacement(t *testing.T) {
	t.Run("delayed-writer", func(t *testing.T) {
		var writeReturned atomic.Int64
		var armed atomic.Bool
		ss := newScriptShard(t, nil, func(sc *scriptConn) {
			if sc.id != 0 {
				return
			}
			sc.onWrite = func(sc *scriptConn, p *clientPacket) error {
				if p.Op == opcode.EventHeartbeat && armed.Load() {
					sc.remoteClose(4000)
					time.Sleep(300 * time.Millisecond) // the writer is still busy while the session fails
					writeReturned.Store(time.Now().UnixNano())
				}
				return ackHeartbeats(sc, p)
			}
		}, nil)
		connectScripted(t, ss, 50)
		armed.Store(true)
		ss.d.await(t)
		dialedAt := time.Now().UnixNano()
		if w := writeReturned.Load(); w == 0 || w > dialedAt {
			t.Fatal("the next connection was dialed before the old writer retired")
		}
	})
	t.Run("delayed-reader", func(t *testing.T) {
		var readerDone atomic.Int64
		ss := newScriptShard(t, nil, func(sc *scriptConn) {
			if sc.id == 0 {
				sc.readDelayAfterClose = 300 * time.Millisecond
			}
		}, nil)
		sc := connectScripted(t, ss, 45000)
		sess := currentSession(ss.c)
		go func() {
			sess.wgRecv.Wait()
			readerDone.Store(time.Now().UnixNano())
		}()
		sc.send(map[string]interface{}{"op": 7, "d": nil})
		ss.d.await(t)
		dialedAt := time.Now().UnixNano()
		if r := readerDone.Load(); r == 0 || r > dialedAt {
			t.Fatal("the next connection was dialed before the old reader retired")
		}
		if sess.aliveWorkers() != "" {
			t.Fatalf("workers of the old session still alive: %s", sess.aliveWorkers())
		}
	})
}

// 24: a failed IDENTIFY write is charged; a turn that could not identify is not.
func TestLifecycle24_IdentifyCharging(t *testing.T) {
	shutdown := make(chan interface{})
	defer close(shutdown)
	limiter := newShardSync(&ShardConfig{IdentifiesPer24H: 1000, ShardRateLimit: time.Millisecond}, logger.Empty{}, "", shutdown)
	limiter.pollInterval = 10 * time.Millisecond
	go limiter.process()

	ss := newScriptShard(t, nil, func(sc *scriptConn) {
		if sc.id == 0 {
			sc.onWrite = func(sc *scriptConn, p *clientPacket) error {
				if p.Op == opcode.EventIdentify {
					return errors.New("connection reset")
				}
				return nil
			}
		}
	}, limiter.queueShard)
	ss.c.identifySync = limiter

	connected := ss.connectAsync()
	sc := ss.d.await(t)
	sc.hello(45000)
	sc2 := ss.d.await(t) // the failed identify write ended the first session
	if n := limiter.metric.ReconnectsSince(time.Hour); n != 1 {
		t.Fatalf("a failed IDENTIFY write must be charged, got %d", n)
	}
	sc2.hello(45000)
	waitOp(t, sc2, 2, 1)
	sc2.ready("sess-1")
	if err := awaitErr(t, connected); err != nil {
		t.Fatal(err)
	}
	if n := limiter.metric.ReconnectsSince(time.Hour); n != 2 {
		t.Fatalf("expected 2 charged identifies, got %d", n)
	}
	_ = sc

	// an abandoned request (Disconnect while waiting for admission) is never charged
	if err := ss.c.Disconnect(); err != nil {
		t.Fatal(err)
	}
	limiter.mu.Lock()
	limiter.server = serverBudget{known: true, total: 1, remaining: 0, resetAt: time.Now().Add(30 * time.Second)}
	limiter.mu.Unlock()
	connected = ss.connectAsync()
	sc3 := ss.d.await(t)
	sc3.hello(45000)
	time.Sleep(100 * time.Millisecond) // parked behind the exhausted budget
	if err := ss.c.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if err := awaitErr(t, connected); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("expected ErrDisconnected, got %v", err)
	}
	if !waitFor(5*time.Second, func() bool { return len(libraryGoroutines()) == 0 }) {
		t.Fatalf("the abandoned identify request is still parked:\n%s", summarizeGoroutines())
	}
	if n := limiter.metric.ReconnectsSince(time.Hour); n != 2 {
		t.Fatalf("an abandoned turn must not be charged, got %d", n)
	}
	if sc3.countOp(2) != 0 {
		t.Fatal("no IDENTIFY may be written after Disconnect")
	}
}

// 44: Connect/Disconnect x20 while identify admission is blocked: at most one queue call per shard.
func TestLifecycle44_CyclesWhileAdmissionBlocked(t *testing.T) {
	t.Run("custom-queue", func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int32
		turns := make(chan error, 10)
		queue := func(_ uint, cb func() error) error {
			calls.Inc()
			<-release
			err := cb()
			turns <- err
			return err
		}
		ss := newScriptShard(t, nil, func(sc *scriptConn) {
			go func() {
				select {
				case <-time.After(time.Millisecond):
					sc.hello(45000)
				case <-sc.closed:
				}
			}()
		}, queue)
		for i := 0; i < 20; i++ {
			connected := ss.connectAsync()
			ss.d.await(t)
			if !waitFor(5*time.Second, func() bool { return calls.Load() >= 1 }) {
				t.Fatal("identify was never requested")
			}
			time.Sleep(5 * time.Millisecond)
			if err := ss.c.Disconnect(); err != nil {
				t.Fatal(err)
			}
			if err := awaitErr(t, connected); !errors.Is(err, ErrDisconnected) {
				t.Fatalf("cycle %d: expected ErrDisconnected, got %v", i, err)
			}
		}
		if n := calls.Load(); n != 1 {
			t.Fatalf("expected the parked queue call to be reused, got %d calls", n)
		}
		close(release)
		if err := <-turns; !errors.Is(err, errIdentifyNotSent) {
			t.Fatalf("a turn without an attached run must not identify, got %v", err)
		}
	})
	t.Run("built-in-exhausted-budget", func(t *testing.T) {
		shutdown := make(chan interface{})
		defer close(shutdown)
		limiter := newShardSync(&ShardConfig{
			IdentifiesPer24H: 1000,
			sessionStart:     sessionStartLimit{total: 1000, remaining: 0, resetAfter: time.Hour, observedAt: time.Now()},
		}, logger.Empty{}, "", shutdown)
		go limiter.process()
		ss := newScriptShard(t, nil, nil, limiter.queueShard)
		ss.c.identifySync = limiter
		for i := 0; i < 20; i++ {
			connected := ss.connectAsync()
			time.Sleep(5 * time.Millisecond)
			if err := ss.c.Disconnect(); err != nil {
				t.Fatal(err)
			}
			if err := awaitErr(t, connected); !errors.Is(err, ErrDisconnected) {
				t.Fatalf("cycle %d: expected ErrDisconnected, got %v", i, err)
			}
		}
		if n := ss.d.count(); n != 0 {
			t.Fatalf("no socket should be opened hours before the identify budget resets, got %d dials", n)
		}
		if n := len(limiter.queue); n != 0 {
			t.Fatalf("expected no queued identify requests, got %d", n)
		}
	})
}

// recordFrames records every frame written on scripted connections, per connection.
type frameRecorder struct {
	mu     sync.Mutex
	frames map[int][]frameRecord
}

func (r *frameRecorder) record(sc *scriptConn, p *clientPacket) {
	r.mu.Lock()
	if r.frames == nil {
		r.frames = map[int][]frameRecord{}
	}
	r.frames[sc.id] = append(r.frames[sc.id], frameRecord{at: time.Now(), op: int(p.Op)})
	r.mu.Unlock()
}

func (r *frameRecorder) of(id int) []frameRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]frameRecord(nil), r.frames[id]...)
}

// saturate keeps the command queue full (bypassing the enqueue-time limiter) until stop is closed.
func saturate(c *EvtClient, stop chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := c.messageQueue.Add(&clientPacket{Op: opcode.EventRequestGuildMembers, Data: 1, CmdName: "REQUEST_GUILD_MEMBERS"}); err == nil {
				c.signalCommands()
			} else {
				time.Sleep(time.Millisecond)
			}
		}
	}()
}

// 45 (and 38 without op 1): a short heartbeat interval with saturated user traffic never exceeds the outbound
// limit and keeps the scheduled heartbeat cadence.
func TestLifecycle45_ShortIntervalSaturated(t *testing.T) {
	const interval = 200 * time.Millisecond
	to := fastTimeouts()
	to.outboundWindow = 2 * time.Second
	to.outboundLimit = 120

	var rec frameRecorder
	ss := newScriptShard(t, to, func(sc *scriptConn) {
		sc.onWrite = func(sc *scriptConn, p *clientPacket) error {
			rec.record(sc, p)
			return ackHeartbeats(sc, p)
		}
	}, nil)
	connectScripted(t, ss, int(interval/time.Millisecond))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	saturate(ss.c, stop, &wg)
	time.Sleep(3 * to.outboundWindow)
	close(stop)
	wg.Wait()

	if n := ss.d.count(); n != 1 {
		t.Fatalf("saturation caused a reconnect (%d connections)", n)
	}
	frames := rec.of(0)
	assertOutboundBudget(t, frames, to.outboundWindow, to.outboundLimit)

	var last time.Time
	var maxGap time.Duration
	var commands int
	for _, f := range frames {
		if f.op == int(opcode.EventRequestGuildMembers) {
			commands++
		}
		if f.op != int(opcode.EventHeartbeat) {
			continue
		}
		if !last.IsZero() && f.at.Sub(last) > maxGap {
			maxGap = f.at.Sub(last)
		}
		last = f.at
	}
	if maxGap > interval+interval/2 {
		t.Fatalf("max heartbeat gap %s exceeds interval %s + slack", maxGap, interval)
	}
	if commands < 100 {
		t.Fatalf("expected the user budget to be used, only %d commands sent", commands)
	}
}

// 38: saturated user traffic plus op 1 spam: the outbound limit is never exceeded on any connection, and op 1
// requests are answered or, once the hard limit is exhausted, the session reconnects (never silently ignored).
func TestLifecycle38_Op1SpamRespondsOrReconnects(t *testing.T) {
	const interval = 100 * time.Millisecond
	to := fastTimeouts()
	to.outboundWindow = 2 * time.Second
	to.outboundLimit = 120

	var rec frameRecorder
	ss := newScriptShard(t, to, func(sc *scriptConn) {
		sc.onWrite = func(sc *scriptConn, p *clientPacket) error {
			rec.record(sc, p)
			if p.Op == opcode.EventResume {
				sc.dispatch(2, "RESUMED", `{}`)
			}
			return ackHeartbeats(sc, p)
		}
		if sc.id > 0 {
			sc.hello(int(interval / time.Millisecond))
		}
	}, nil)
	sc := connectScripted(t, ss, int(interval/time.Millisecond))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	saturate(ss.c, stop, &wg)
	var requests atomic.Int32
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-sc.closed:
				return
			case <-tick.C:
				requests.Inc()
				sc.send(map[string]interface{}{"op": 1})
			}
		}
	}()
	time.Sleep(3 * to.outboundWindow)
	close(stop)
	wg.Wait()

	for id := 0; id < ss.d.count(); id++ {
		assertOutboundBudget(t, rec.of(id), to.outboundWindow, to.outboundLimit)
	}
	reconnected := ss.d.count() > 1
	exhausted := ss.log.Count("a requested heartbeat can not be answered") > 0
	var beats int
	for _, f := range rec.of(0) {
		if f.op == int(opcode.EventHeartbeat) {
			beats++
		}
	}
	switch {
	case reconnected && !exhausted:
		t.Fatalf("unexpected reconnect:\n%s", ss.log.Tail(20))
	case !reconnected && beats < int(requests.Load())/10:
		t.Fatalf("op 1 requests were not answered: %d requests, %d heartbeats", requests.Load(), beats)
	}
	t.Logf("op 1 requests=%d heartbeats on the first connection=%d reconnected=%v", requests.Load(), beats, reconnected)
}

// Item 7 of the review: an op 1 request is answered within the hard limit even when the control share is used up,
// and reconnects (outboundExhausted) only when the hard limit is exhausted.
func TestHeartbeatRequestUsesHardLimit(t *testing.T) {
	newSession := func() (*session, *scriptConn) {
		c := &EvtClient{log: newTestLogger("op1"), to: defaultTimeouts()}
		s := c.newSession(nil)
		sc := newScriptConn(0)
		s.conn = sc
		s.interval.Store(int64(time.Second))
		return s, sc
	}
	fill := func(n int) *outboundBudget {
		b := &outboundBudget{window: time.Minute}
		for i := 0; i < n; i++ {
			b.record(time.Now())
		}
		return b
	}
	_, control := outboundLimits(time.Second, time.Minute, 120)

	s, sc := newSession()
	if !s.sendHeartbeat(fill(control), false) || sc.countOp(1) != 1 {
		t.Fatal("an op 1 request above the control share must still be answered")
	}

	s, sc = newSession()
	if s.sendHeartbeat(fill(120), false) || sc.countOp(1) != 0 {
		t.Fatal("an op 1 request at the hard limit must not be written")
	}
	if r := s.freeze(); r.kind != failOutboundExhausted {
		t.Fatalf("expected the session to fail with outboundExhausted, got %s", r)
	}
}

// Review item 6: READY/RESUMED end the handshake even if their sequence number is missing or not ahead of ours.
func TestResumedWithStaleOrMissingSeq(t *testing.T) {
	for _, tc := range []struct {
		name    string
		resumed string
	}{
		{"seq-equal", `{"op":0,"t":"RESUMED","s":3,"d":{}}`},
		{"seq-missing", `{"op":0,"t":"RESUMED","d":{}}`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			to := fastTimeouts()
			to.readyIdle = 200 * time.Millisecond
			ss := newScriptShard(t, to, nil, nil)
			sc := connectScripted(t, ss, 45000)
			sc.dispatch(2, "MESSAGE_CREATE", `{}`)
			sc.dispatch(3, "MESSAGE_CREATE", `{}`)
			if !waitFor(5*time.Second, func() bool { return len(ss.events) == 3 }) {
				t.Fatal("events not delivered")
			}

			sc.send(map[string]interface{}{"op": 7, "d": nil})
			sc2 := ss.d.await(t)
			sc2.hello(45000)
			waitOp(t, sc2, 6, 1)
			sc2.inbox <- []byte(tc.resumed)
			if !waitFor(5*time.Second, func() bool { return ss.c.stateSnapshot() == "ready" }) {
				t.Fatalf("RESUMED did not complete the handshake, state %q", ss.c.stateSnapshot())
			}
			time.Sleep(5 * to.readyIdle)
			if n := ss.d.count(); n != 2 {
				t.Fatalf("the RESUME was retried (%d connections)", n)
			}
			ss.c.mu.Lock()
			seq := ss.c.seq
			ss.c.mu.Unlock()
			if seq != 3 {
				t.Fatalf("expected the checkpoint to stay at 3, got %d", seq)
			}
		})
	}

	t.Run("ready-seq-missing", func(t *testing.T) {
		ss := newScriptShard(t, nil, nil, nil)
		connected := ss.connectAsync()
		sc := ss.d.await(t)
		sc.hello(45000)
		waitOp(t, sc, 2, 1)
		sc.inbox <- []byte(`{"op":0,"t":"READY","d":{"session_id":"sess-1"}}`)
		if err := awaitErr(t, connected); err != nil {
			t.Fatal(err)
		}
	})
}
