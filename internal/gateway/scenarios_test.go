//go:build !integration
// +build !integration

package gateway

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/aidenwallis/disgord/internal/gateway/cmd"
)

// Gateway scenarios against the fake gateway (real nhooyr transport, real event client). Numbers refer to the
// test plan. Scenarios marked "base: FAIL" wedged the shard permanently on 31d7afb.

func newScenario(t *testing.T, hbMs int64) *fakeGateway {
	gw := newFakeGateway(t, newTestLogger(t.Name()))
	gw.hbIntervalMs.Store(hbMs)
	return gw
}

// 1: TCP drop (with and without RST) -> RESUME on a new connection.
func TestScenario01_TCPDropResumes(t *testing.T) {
	t.Parallel()
	for _, rst := range []bool{false, true} {
		rst := rst
		t.Run(fmt.Sprintf("rst=%v", rst), func(t *testing.T) {
			t.Parallel()
			gw := newScenario(t, 500)
			rc := newTestShard(t, gw, shardOpts{})
			fc := rc.connect(t)
			fc.abort(rst)
			next := rc.expectRecovery(t, fc.idx+1, 20*time.Second)
			if next.resumes.Load() != 1 || next.identifies.Load() != 0 {
				t.Fatalf("expected a RESUME, got identifies=%d resumes=%d", next.identifies.Load(), next.resumes.Load())
			}
		})
	}
}

// 2: server close 1000/1001/4000/4008 -> RESUME (4008 used to stop the shard forever, A7).
func TestScenario02_ResumableCloseCodes(t *testing.T) {
	t.Parallel()
	for _, code := range []int{1000, 1001, 4000, 4008} {
		code := code
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			t.Parallel()
			gw := newScenario(t, 500)
			rc := newTestShard(t, gw, shardOpts{})
			fc := rc.connect(t)
			fc.closeWith(code, "injected")
			next := rc.expectRecovery(t, fc.idx+1, 20*time.Second)
			if next.resumes.Load() != 1 || next.identifies.Load() != 0 {
				t.Fatalf("expected a RESUME, got identifies=%d resumes=%d", next.identifies.Load(), next.resumes.Load())
			}
		})
	}
}

// 3: 4007/4009/op 9 d=false -> IDENTIFY a new session on a new connection after the 1-5s invalid session delay.
func TestScenario03_InvalidSessionIdentifies(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"4007", "4009", "op9"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			gw := newScenario(t, 1000)
			to := fastTimeouts()
			to.invalidSessionMin, to.invalidSessionMax = time.Second, 5*time.Second // production values
			rc := newTestShard(t, gw, shardOpts{timeouts: to})
			fc := rc.connect(t)
			fc.invalidateSession()
			start := time.Now()
			if mode == "op9" {
				_ = fc.send(map[string]interface{}{"op": 9, "d": false})
			} else {
				var code int
				fmt.Sscan(mode, &code)
				fc.closeWith(code, "injected")
			}
			if !waitFor(30*time.Second, func() bool { return gw.connCount() > fc.idx+1 }) {
				t.Fatal("no new connection")
			}
			delay := time.Since(start)
			if delay < 900*time.Millisecond || delay > 7*time.Second {
				t.Fatalf("expected the reconnect after the 1-5s invalid session delay, got %s", delay)
			}
			next := rc.expectRecovery(t, fc.idx+1, 30*time.Second)
			if next.identifies.Load() != 1 || next.resumes.Load() != 0 {
				t.Fatalf("expected IDENTIFY on the new connection, got identifies=%d resumes=%d", next.identifies.Load(), next.resumes.Load())
			}
			if fc.identifies.Load() != 1 {
				t.Fatal("IDENTIFY must never be sent on the invalidated connection")
			}
			rc.c.mu.Lock()
			sid := rc.c.sessionID
			rc.c.mu.Unlock()
			if sid == "sess-1" {
				t.Fatal("expected a new session id")
			}
		})
	}
}

// 4: fatal close codes stop the shard: Connect returns the error at startup; at runtime no further dials.
func TestScenario04_FatalCloseCodes(t *testing.T) {
	t.Parallel()
	for _, code := range []int{4004, 4010, 4011, 4012, 4013, 4014} {
		code := code
		t.Run(fmt.Sprintf("startup-%d", code), func(t *testing.T) {
			t.Parallel()
			gw := newScenario(t, 500)
			gw.identifyClose.Store(int32(code))
			rc := newTestShard(t, gw, shardOpts{})
			err := rc.c.Connect()
			var closeErr *GatewayCloseError
			if !errors.As(err, &closeErr) || closeErr.Code != code {
				t.Fatalf("Connect must return the fatal close code %d, got %v", code, err)
			}
			time.Sleep(300 * time.Millisecond)
			if n := gw.dialAttempts.Load(); n != 1 {
				t.Fatalf("expected no further dials after a fatal close, got %d", n)
			}
			if !strings.Contains(rc.c.stateSnapshot(), "stopped") || rc.log.Count("ERROR") == 0 {
				t.Fatalf("expected stopped state and an error log, got %q", rc.c.stateSnapshot())
			}
		})
	}
	t.Run("runtime-4004", func(t *testing.T) {
		gw := newScenario(t, 500)
		rc := newTestShard(t, gw, shardOpts{})
		fc := rc.connect(t)
		fc.closeWith(4004, "Authentication failed.")
		if !waitFor(10*time.Second, func() bool { return strings.Contains(rc.c.stateSnapshot(), "stopped") }) {
			t.Fatalf("shard did not stop, state %q", rc.c.stateSnapshot())
		}
		time.Sleep(500 * time.Millisecond)
		if n := gw.dialAttempts.Load(); n != 1 {
			t.Fatalf("expected no further dials after a fatal close, got %d", n)
		}
		// a stopped shard can be connected again
		gw.mu.Lock()
		gw.sessions = map[string]*fakeSession{}
		gw.mu.Unlock()
		if err := rc.c.Connect(); err != nil {
			t.Fatalf("Connect after a fatal stop: %v", err)
		}
	})
}

// 5: zombie connection (no heartbeat ACKs) -> reconnect + RESUME.
func TestScenario05_Zombie(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 300)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)
	fc.noAck.Store(true)
	start := time.Now()
	next := rc.expectRecovery(t, fc.idx+1, 20*time.Second)
	if next.resumes.Load() != 1 {
		t.Fatal("expected a RESUME after a zombie connection")
	}
	t.Logf("zombie detected and resumed after %s", time.Since(start))
}

// 6: half-open connection (server stops reading and writing) -> reconnect within ~2 heartbeat intervals.
func TestScenario06_HalfOpen(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)
	fc.frozen.Store(true)
	start := time.Now()
	if !waitFor(20*time.Second, func() bool { return gw.connCount() > fc.idx+1 }) {
		t.Fatal("no reconnect from a half-open connection")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("reconnect took %s, expected about 2 heartbeat intervals", d)
	}
	rc.expectRecovery(t, fc.idx+1, 20*time.Second)
}

// half-open with tiny socket buffers and big commands: the writer blocks inside a write; the per-write deadline
// and teardown still recover the shard.
func TestScenario06b_HalfOpenWriterBlocked(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	gw.lis.readBuf.Store(2048)
	rc := newTestShard(t, gw, shardOpts{httpClient: smallBufferHTTPClient(2048)})
	fc := rc.connect(t)

	big := make([]Snowflake, 20000) // ~400KB of JSON per command
	for i := range big {
		big[i] = Snowflake(1<<40 + i)
	}
	fc.frozen.Store(true)
	for i := 0; i < 5; i++ {
		start := time.Now()
		_ = rc.c.Emit(cmd.RequestGuildMembers, &RequestGuildMembersPayload{GuildIDs: []Snowflake{1 << 22}, UserIDs: big})
		if d := time.Since(start); d > time.Second {
			t.Fatalf("Emit blocked for %s", d)
		}
	}
	if !waitFor(30*time.Second, func() bool { return gw.connCount() > fc.idx+1 }) {
		t.Fatal("no reconnect after the writer blocked")
	}
	_ = rc.c.messageQueue.Steal() // drop the giant commands before checking recovery
	rc.expectRecovery(t, fc.idx+1, 30*time.Second)
}

// 7: op 7 -> RESUME, which does not wait for an identify turn (B4).
func TestScenario07_Op7(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	var queueCalls atomic.Int32
	queue := func(_ uint, cb func() error) error {
		queueCalls.Inc()
		return cb()
	}
	rc := newTestShard(t, gw, shardOpts{connectQueue: queue})
	fc := rc.connect(t)
	_ = fc.send(map[string]interface{}{"op": 7, "d": nil})
	next := rc.expectRecovery(t, fc.idx+1, 20*time.Second)
	if next.resumes.Load() != 1 || next.identifies.Load() != 0 {
		t.Fatalf("expected a RESUME, got identifies=%d resumes=%d", next.identifies.Load(), next.resumes.Load())
	}
	if code := fc.clientClose.Load(); code != 4000 {
		t.Fatalf("expected the client to close the old connection with 4000, got %d", code)
	}
	if n := queueCalls.Load(); n != 1 {
		t.Fatalf("only the initial IDENTIFY may go through the connect queue, got %d queue calls", n)
	}
}

// 8: op 9 d=true -> RESUME on a new connection.
func TestScenario08_Op9Resumable(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)
	_ = fc.send(map[string]interface{}{"op": 9, "d": true})
	next := rc.expectRecovery(t, fc.idx+1, 20*time.Second)
	if next.resumes.Load() != 1 || next.identifies.Load() != 0 {
		t.Fatalf("expected a RESUME, got identifies=%d resumes=%d", next.identifies.Load(), next.resumes.Load())
	}
}

// 9 (A1, base: FAIL): the new connection fails before/around HELLO during a reconnect.
func TestScenario09_PreHelloFailureDuringReconnect(t *testing.T) {
	t.Parallel()
	type mode struct {
		name   string
		inject func(gw *fakeGateway)
	}
	modes := []mode{
		{"http500x3", func(gw *fakeGateway) { gw.failDials.Store(3) }},
		{"tcp-rst-x3", func(gw *fakeGateway) { gw.resetDials.Store(3) }},
		{"drop-after-upgrade", func(gw *fakeGateway) { gw.dropAfterWS.Store(1) }},
		{"close4000-after-upgrade", func(gw *fakeGateway) { gw.closeAfterWS.Store(1) }},
		{"hang", func(gw *fakeGateway) { gw.hangDials.Store(1) }},
	}
	for _, m := range modes {
		m := m
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			gw := newScenario(t, 500)
			to := fastTimeouts()
			to.dial = 500 * time.Millisecond
			rc := newTestShard(t, gw, shardOpts{timeouts: to})
			fc := rc.connect(t)
			m.inject(gw)
			fc.abort(true)
			rc.expectRecovery(t, fc.idx+1, 30*time.Second)
		})
	}
}

// 10 (A2): failure right after the reconnect succeeded.
func TestScenario10_FailureRightAfterReconnect(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)
	go func() {
		for nc := range gw.newConn {
			nc := nc
			if nc.idx == fc.idx+1 {
				go func() {
					<-nc.ready
					nc.abort(true)
				}()
				return
			}
		}
	}()
	fc.abort(true)
	rc.expectRecovery(t, fc.idx+2, 30*time.Second)
}

// repeated random-timing drops around HELLO (base: FAIL, client stopped dialing after the first kill).
func TestScenario_RandomTimingDropStorm(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	gw.helloDelayMs.Store(15)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)
	for len(gw.newConn) > 0 {
		<-gw.newConn
	}
	stop := make(chan struct{})
	defer close(stop)
	var killed atomic.Int32
	go func() {
		i := 0
		for {
			select {
			case <-stop:
				return
			case nc := <-gw.newConn:
				i++
				if i > 20 {
					continue
				}
				d := time.Duration((i*7)%30) * time.Millisecond
				go func() { time.Sleep(d); nc.abort(true); killed.Inc() }()
			}
		}
	}()
	fc.abort(true)
	if !waitFor(60*time.Second, func() bool { return killed.Load() >= 20 }) {
		t.Fatalf("client stopped dialing after %d kills", killed.Load())
	}
	rc.expectRecovery(t, gw.connCount()-1, 30*time.Second)
}
