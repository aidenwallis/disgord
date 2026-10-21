//go:build !integration
// +build !integration

package gateway

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/aidenwallis/disgord/internal/gateway/cmd"
)

func dispatchN(t *testing.T, fc *fakeConn, prefix string, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		if err := fc.dispatch("MESSAGE_CREATE", []byte(fmt.Sprintf(`{"id":%q}`, id))); err != nil {
			t.Fatalf("dispatch %s: %v", id, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// assertDeliveredInOrder checks that want appears in delivery order, each exactly once.
func assertDeliveredInOrder(t *testing.T, rc *testShard, want []string, timeout time.Duration) {
	t.Helper()
	contains := func() bool {
		seen := map[string]bool{}
		for _, id := range rc.deliveredIDs() {
			seen[id] = true
		}
		for _, id := range want {
			if !seen[id] {
				return false
			}
		}
		return true
	}
	if !waitFor(timeout, contains) {
		t.Fatalf("not every event was delivered: got %d of %d", len(rc.deliveredIDs()), len(want))
	}
	wanted := map[string]bool{}
	for _, id := range want {
		wanted[id] = true
	}
	var got []string
	for _, id := range rc.deliveredIDs() {
		if wanted[id] {
			got = append(got, id)
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("events delivered out of order or duplicated:\n got %v\nwant %v", got, want)
	}
}

// 12: teardown while the receiver is blocked on the event channel: the blocked event was not checkpointed, so
// the RESUME replays it.
func TestScenario12_BlockedEventRedeliveredAfterResume(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 300)
	rc := newTestShard(t, gw, shardOpts{eventBuf: 1})
	fc := rc.connect(t)

	rc.pause()
	want := dispatchN(t, fc, "cp", 10)
	time.Sleep(200 * time.Millisecond) // receiver is now blocked forwarding
	fc.abort(true)                     // the next heartbeat write fails and tears the session down

	if !waitFor(20*time.Second, func() bool { return gw.connCount() >= 2 && gw.conn(1).resumes.Load() == 1 }) {
		t.Fatal("expected a RESUME on a new connection")
	}
	rc.resume()
	assertDeliveredInOrder(t, rc, want, 20*time.Second)
	rc.expectRecovery(t, 1, 20*time.Second)
}

// 15: 20 rapid drops leave exactly one set of workers running.
func TestScenario15_RapidDropsNoLeak(t *testing.T) {
	gw := newScenario(t, 500)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)

	for i := 0; i < 20; i++ {
		fc.abort(true)
		next, ok := gw.waitHealthy(fc.idx+1, -1, 0, 20*time.Second)
		if !ok {
			t.Fatalf("drop %d/20: no new ready connection", i+1)
		}
		fc = next
	}
	rc.expectRecovery(t, fc.idx, 20*time.Second)

	// steady state: supervisor + its shutdown watcher + receiver + writer + heartbeat
	if !waitFor(10*time.Second, func() bool { return len(libraryGoroutines()) == 5 }) {
		t.Fatalf("expected exactly one session's workers to be running:\n%s", summarizeGoroutines())
	}
}

// 16 (C6): commands emitted while disconnected do not block, are sent only after RESUMED, and never trigger
// 4003.
func TestScenario16_CommandsWhileDisconnected(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 300)
	gw.enforceAuth = true
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)

	gw.failDials.Store(3)
	fc.abort(true)
	if !waitFor(10*time.Second, func() bool { return gw.dialAttempts.Load() >= 2 }) {
		t.Fatal("no reconnect attempt")
	}

	var nonces []chan struct{}
	for i := 0; i < 5; i++ {
		nonce := fmt.Sprintf("offline-%d", i)
		nonces = append(nonces, gw.expectNonce(nonce))
		start := time.Now()
		err := rc.c.Emit(cmd.RequestGuildMembers, &RequestGuildMembersPayload{GuildIDs: []Snowflake{1 << 22}, Nonce: nonce})
		if err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > time.Second {
			t.Fatalf("Emit blocked for %s while disconnected", d)
		}
	}
	if err := rc.c.Emit(cmd.UpdateStatus, &UpdateStatusPayload{Status: StatusIdle}); err != nil {
		t.Fatal(err)
	}

	for i, ch := range nonces {
		select {
		case <-ch:
		case <-time.After(30 * time.Second):
			t.Fatalf("command %d queued while disconnected was never delivered", i)
		}
	}
	for _, c := range gw.allConns() {
		if n := c.cmdBeforeReady.Load(); n != 0 {
			t.Fatalf("conn #%d received %d commands before READY/RESUMED", c.idx, n)
		}
		if c.clientClose.Load() == 4003 {
			t.Fatal("unexpected 4003")
		}
	}
}

// 17 (C3): close codes received by the server: 4000 on reconnect, 1000 on Disconnect (also while the receiver
// is blocked in Read or on the event channel).
func TestScenario17_CloseCodes(t *testing.T) {
	t.Parallel()
	t.Run("reconnect", func(t *testing.T) {
		gw := newScenario(t, 500)
		rc := newTestShard(t, gw, shardOpts{})
		fc := rc.connect(t)
		_ = fc.send(map[string]interface{}{"op": 7, "d": nil})
		rc.expectRecovery(t, fc.idx+1, 20*time.Second)
		if code := fc.clientClose.Load(); code != 4000 {
			t.Fatalf("expected 4000 on reconnect, got %d", code)
		}
	})
	t.Run("disconnect-mid-read", func(t *testing.T) {
		gw := newScenario(t, 500)
		rc := newTestShard(t, gw, shardOpts{})
		fc := rc.connect(t)
		if err := rc.c.Disconnect(); err != nil {
			t.Fatal(err)
		}
		if !waitFor(10*time.Second, fc.isClosed) {
			t.Fatal("connection not closed")
		}
		if code := fc.clientClose.Load(); code != 1000 {
			t.Fatalf("expected 1000 on Disconnect, got %d", code)
		}
	})
	t.Run("disconnect-while-blocked-on-consumer", func(t *testing.T) {
		gw := newScenario(t, 500)
		rc := newTestShard(t, gw, shardOpts{eventBuf: 1})
		fc := rc.connect(t)
		rc.pause()
		dispatchN(t, fc, "x", 5)
		time.Sleep(100 * time.Millisecond)
		start := time.Now()
		if err := rc.c.Disconnect(); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > 10*time.Second {
			t.Fatalf("Disconnect took %s", d)
		}
		if !waitFor(10*time.Second, fc.isClosed) {
			t.Fatal("connection not closed")
		}
		if code := fc.clientClose.Load(); code != 1000 {
			t.Fatalf("expected 1000 on Disconnect, got %d", code)
		}
	})
}

// 20: a READY -> close flapping loop keeps backing off and never re-identifies.
func TestScenario20_FlappingBacksOff(t *testing.T) {
	gw := newScenario(t, 500)
	to := fastTimeouts()
	to.backoffMin, to.backoffMax = 20*time.Millisecond, 2*time.Second
	to.healthyAfter = 10 * time.Second
	rc := newTestShard(t, gw, shardOpts{timeouts: to})
	fc := rc.connect(t)

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case nc := <-gw.newConn:
				go func() {
					select {
					case <-nc.ready:
						time.Sleep(20 * time.Millisecond)
						nc.closeWith(4000, "flap")
					case <-nc.closedCh:
					}
				}()
			}
		}
	}()
	for len(gw.newConn) > 0 {
		<-gw.newConn
	}
	fc.closeWith(4000, "flap")

	if !waitFor(30*time.Second, func() bool { return gw.connCount() >= 8 }) {
		t.Fatalf("expected the shard to keep reconnecting, got %d connections", gw.connCount())
	}
	conns := gw.allConns()
	var gaps []time.Duration
	for i := 2; i < 8; i++ {
		gaps = append(gaps, conns[i].openedAt.Sub(conns[i-1].openedAt))
	}
	if gaps[len(gaps)-1] < 4*gaps[0] {
		t.Fatalf("backoff does not grow while flapping: %v", gaps)
	}
	if n := gw.totalIdentifies(); n != 1 {
		t.Fatalf("a flapping resumable session must not re-identify, got %d identifies", n)
	}
}

// 21: Disconnect during Connect, and while waiting for an identify turn: no orphaned dial or IDENTIFY afterwards.
func TestScenario21_DisconnectDuringConnect(t *testing.T) {
	t.Parallel()
	t.Run("during-dial", func(t *testing.T) {
		gw := newScenario(t, 500)
		gw.hangDials.Store(1)
		rc := newTestShard(t, gw, shardOpts{})
		errCh := make(chan error, 1)
		go func() { errCh <- rc.c.Connect() }()
		if !waitFor(5*time.Second, func() bool { return gw.dialAttempts.Load() == 1 }) {
			t.Fatal("no dial")
		}
		if err := rc.c.Disconnect(); err != nil {
			t.Fatal(err)
		}
		if err := <-errCh; !errors.Is(err, ErrDisconnected) {
			t.Fatalf("expected ErrDisconnected, got %v", err)
		}
		time.Sleep(300 * time.Millisecond)
		if n := gw.dialAttempts.Load(); n != 1 {
			t.Fatalf("no dial may happen after Disconnect, got %d", n)
		}
	})
	t.Run("waiting-for-identify-turn", func(t *testing.T) {
		gw := newScenario(t, 500)
		release := make(chan struct{})
		var calls atomic.Int32
		turn := make(chan error, 1)
		queue := func(_ uint, cb func() error) error {
			calls.Inc()
			<-release
			err := cb()
			turn <- err
			return err
		}
		rc := newTestShard(t, gw, shardOpts{connectQueue: queue})
		errCh := make(chan error, 1)
		go func() { errCh <- rc.c.Connect() }()
		if !waitFor(5*time.Second, func() bool { return calls.Load() == 1 }) {
			t.Fatal("identify turn was never requested")
		}
		if err := rc.c.Disconnect(); err != nil {
			t.Fatal(err)
		}
		if err := <-errCh; !errors.Is(err, ErrDisconnected) {
			t.Fatalf("expected ErrDisconnected, got %v", err)
		}
		close(release)
		if err := <-turn; !errors.Is(err, errIdentifyNotSent) {
			t.Fatalf("a turn granted after Disconnect must not identify, got %v", err)
		}
		time.Sleep(200 * time.Millisecond)
		if n := gw.totalIdentifies(); n != 0 {
			t.Fatalf("no IDENTIFY may be sent after Disconnect, got %d", n)
		}
		if n := gw.dialAttempts.Load(); n != 1 {
			t.Fatalf("no dial may happen after Disconnect, got %d", n)
		}
	})
}

// 22: Connect -> Disconnect x20 leaves no goroutines behind and resets requestedDisconnect.
func TestScenario22_ConnectDisconnectCycles(t *testing.T) {
	gw := newScenario(t, 500)
	rc := newTestShard(t, gw, shardOpts{})
	for i := 0; i < 20; i++ {
		if err := rc.c.Connect(); err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
		if rc.c.requestedDisconnect.Load() {
			t.Fatal("requestedDisconnect must be reset by Connect")
		}
		if err := rc.c.Disconnect(); err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
		if !rc.c.requestedDisconnect.Load() {
			t.Fatal("requestedDisconnect must be set by Disconnect")
		}
	}
	rc.c.lifecycleMu.Lock()
	r := rc.c.run
	rc.c.lifecycleMu.Unlock()
	if r != nil {
		t.Fatal("the run must be cleared after Disconnect")
	}
	waitNoLibraryGoroutines(t, 10*time.Second)
	for _, fc := range gw.allConns() {
		if code := fc.clientClose.Load(); code != 1000 {
			t.Fatalf("conn #%d closed with %d, expected 1000", fc.idx, code)
		}
	}
}

// 28: op 9 d=false followed by the server echoing our 4000 close: the next connection identifies.
func TestScenario28_InvalidSessionThenEchoedClose(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)
	fc.invalidateSession()
	_ = fc.send(map[string]interface{}{"op": 9, "d": false})
	next := rc.expectRecovery(t, fc.idx+1, 20*time.Second)
	if code := fc.clientClose.Load(); code != 4000 {
		t.Fatalf("expected our close code 4000 (echoed by the server), got %d", code)
	}
	if next.identifies.Load() != 1 || next.resumes.Load() != 0 {
		t.Fatalf("expected IDENTIFY, got identifies=%d resumes=%d", next.identifies.Load(), next.resumes.Load())
	}
}

// 29: the session dies while waiting for its identify turn: the shard keeps its single place in line and the
// IDENTIFY goes out on the new connection.
func TestScenario29_SessionDiesWhileWaitingForTurn(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	release := make(chan struct{})
	var calls atomic.Int32
	queue := func(_ uint, cb func() error) error {
		calls.Inc()
		<-release
		return cb()
	}
	rc := newTestShard(t, gw, shardOpts{connectQueue: queue})
	errCh := make(chan error, 1)
	go func() { errCh <- rc.c.Connect() }()

	if !waitFor(5*time.Second, func() bool { return calls.Load() == 1 && gw.connCount() == 1 }) {
		t.Fatal("identify turn was never requested")
	}
	gw.conn(0).abort(true)
	if !waitFor(10*time.Second, func() bool { return gw.connCount() == 2 }) {
		t.Fatal("no reconnect")
	}
	time.Sleep(200 * time.Millisecond) // the new session reached the identify step
	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("expected a single queue call, got %d", n)
	}
	if gw.conn(0).identifies.Load() != 0 || gw.conn(1).identifies.Load() != 1 {
		t.Fatal("expected the IDENTIFY on the new connection only")
	}
}

// Disconnect between failed dials must not panic (base: FAIL, nil pointer dereference).
func TestScenario_DisconnectDuringFailedDials(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)
	gw.failDials.Store(100)
	fc.abort(true)
	time.Sleep(500 * time.Millisecond)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Disconnect panicked: %v\n%s", r, debug.Stack())
			}
		}()
		if err := rc.c.Disconnect(); err != nil {
			t.Fatal(err)
		}
	}()
}

// Emit after a pre-HELLO drop (base: FAIL, commands silently queued forever).
func TestScenario_EmitAfterPreHelloDrop(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)
	gw.dropAfterWS.Store(1)
	fc.abort(true)
	time.Sleep(time.Second)
	var nonces []chan struct{}
	for i := 0; i < 10; i++ {
		nonce := fmt.Sprintf("after-drop-%d", i)
		nonces = append(nonces, gw.expectNonce(nonce))
		if err := rc.c.Emit(cmd.RequestGuildMembers, &RequestGuildMembersPayload{GuildIDs: []Snowflake{1 << 22}, Nonce: nonce}); err != nil {
			t.Fatal(err)
		}
	}
	for i, ch := range nonces {
		select {
		case <-ch:
		case <-time.After(20 * time.Second):
			t.Fatalf("command %d never reached the gateway", i)
		}
	}
}

// op 7 queued behind a blocked receiver (base: FAIL): handled once the consumer catches up.
func TestScenario_Op7WhileReceiverBlocked(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 5000)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)
	rc.pause()
	want := dispatchN(t, fc, "k3", 120)
	_ = fc.send(map[string]interface{}{"op": 7, "d": nil})
	time.Sleep(time.Second)
	rc.resume()
	assertDeliveredInOrder(t, rc, want, 20*time.Second)
	if !waitFor(20*time.Second, func() bool { return gw.connCount() >= 2 }) {
		t.Fatal("op 7 was never handled")
	}
	fc2, ok := gw.waitHealthy(1, -1, 1, 30*time.Second)
	if !ok || fc2.resumes.Load() != 1 {
		t.Fatal("expected a RESUME after op 7")
	}
}

// 100 KiB dispatch is delivered without a reconnect.
func TestScenario_LargeMessage(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	rc := newTestShard(t, gw, shardOpts{})
	fc := rc.connect(t)
	pad := strings.Repeat("x", 100*1024)
	if err := fc.dispatch("MESSAGE_CREATE", []byte(fmt.Sprintf(`{"id":"big","pad":%q}`, pad))); err != nil {
		t.Fatal(err)
	}
	assertDeliveredInOrder(t, rc, []string{"big"}, 10*time.Second)
	if fc.isClosed() || gw.connCount() != 1 {
		t.Fatal("large message caused a reconnect")
	}
}
