//go:build !integration
// +build !integration

package gateway

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/aidenwallis/disgord/internal/gateway/cmd"
)

// Shard manager scenarios on the fake gateway.

type testManager struct {
	*testConsumer
	t        *testing.T
	mngr     *shardMngr
	gw       *fakeGateway
	shutdown chan interface{}
	closed   atomic.Bool
}

func newTestManager(t *testing.T, gw *fakeGateway, ids []uint, mutate func(conf *ShardManagerConfig)) *testManager {
	shutdown := make(chan interface{})
	cons := newTestConsumer(2, shutdown) // production-sized buffer
	conf := ShardManagerConfig{
		ShardConfig: ShardConfig{
			ShardIDs:         ids,
			ShardCount:       uint(len(ids)),
			ShardRateLimit:   50 * time.Millisecond,
			IdentifiesPer24H: 1000,
			URL:              gw.URL,
		},
		BotToken:     "test-token",
		Logger:       gw.log,
		ShutdownChan: shutdown,
		EventChan:    cons.eChan,
		DisgordInfo:  "disgord-test",
		ProjectName:  "test",
		timeouts:     fastTimeouts(),
	}
	if mutate != nil {
		mutate(&conf)
	}
	tm := &testManager{testConsumer: cons, t: t, mngr: NewShardMngr(conf), gw: gw, shutdown: shutdown}
	t.Cleanup(func() {
		cons.resume()
		_ = tm.mngr.Disconnect()
		tm.closeShutdown()
		if t.Failed() {
			t.Logf("server:\n%s\nlog tail:\n%s", gw.summary(), gw.log.Tail(80))
		}
	})
	return tm
}

func (tm *testManager) closeShutdown() {
	if tm.closed.CAS(false, true) {
		close(tm.shutdown)
	}
}

func (tm *testManager) waitAll(minIdx int, minHB int, timeout time.Duration) map[uint]*fakeConn {
	tm.t.Helper()
	out := map[uint]*fakeConn{}
	deadline := time.Now().Add(timeout)
	for _, id := range tm.mngr.conf.ShardIDs {
		fc, ok := tm.gw.waitHealthy(minIdx, int(id), minHB, time.Until(deadline))
		if !ok {
			tm.t.Fatalf("shard %d did not become healthy (conn idx >= %d) within %s", id, minIdx, timeout)
		}
		out[id] = fc
	}
	return out
}

func (tm *testManager) checkProbes(conns map[uint]*fakeConn) {
	tm.t.Helper()
	for id, fc := range conns {
		if err := tm.probeEvent(fc, 10*time.Second); err != nil {
			tm.t.Fatalf("shard %d: %v", id, err)
		}
		shard, _ := tm.mngr.GetShard(id)
		if _, err := probeCommand(tm.gw, shard, 10*time.Second); err != nil {
			tm.t.Fatalf("shard %d: %v", id, err)
		}
	}
}

func (tm *testManager) running() []uint {
	var ids []uint
	for _, id := range tm.mngr.ShardIDs() {
		shard, _ := tm.mngr.GetShard(id)
		shard.lifecycleMu.Lock()
		if shard.run != nil {
			ids = append(ids, id)
		}
		shard.lifecycleMu.Unlock()
	}
	return ids
}

func openConns(gw *fakeGateway) int {
	var n int
	for _, fc := range gw.allConns() {
		if !fc.isClosed() {
			n++
		}
	}
	return n
}

// S1: both shards drop at once and recover.
func TestManager_TwoShardsRecover(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	tm := newTestManager(t, gw, []uint{0, 1}, nil)
	if err := tm.mngr.Connect(); err != nil {
		t.Fatal(err)
	}
	conns := tm.waitAll(0, 2, 20*time.Second)
	minIdx := gw.connCount()
	conns[0].abort(true)
	conns[1].closeWith(1001, "going away")
	tm.checkProbes(tm.waitAll(minIdx, 3, 30*time.Second))
}

// S2 (A4, base: FAIL): slow consumer shared by 2 shards with the production event buffer.
func TestManager_SlowConsumer(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	tm := newTestManager(t, gw, []uint{0, 1}, nil)
	if err := tm.mngr.Connect(); err != nil {
		t.Fatal(err)
	}
	conns := tm.waitAll(0, 2, 20*time.Second)
	tm.pause()
	var wg sync.WaitGroup
	for _, fc := range conns {
		fc := fc
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				if err := fc.dispatch("MESSAGE_CREATE", []byte(fmt.Sprintf(`{"id":"s%d-%d"}`, fc.shardID.Load(), i))); err != nil {
					return
				}
			}
		}()
	}
	time.Sleep(4 * time.Second)
	tm.resume()
	wg.Wait()
	tm.checkProbes(tm.waitAll(0, 3, 30*time.Second))
	if n := gw.connCount(); n != 2 {
		t.Fatalf("back-pressure must not cause reconnects, got %d connections", n)
	}
}

// S3 (A1, base: FAIL): one shard's reconnect dies before HELLO; neither shard stays stuck.
func TestManager_DropBeforeHello(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	tm := newTestManager(t, gw, []uint{0, 1}, nil)
	if err := tm.mngr.Connect(); err != nil {
		t.Fatal(err)
	}
	conns := tm.waitAll(0, 2, 20*time.Second)
	minIdx := gw.connCount()
	gw.dropAfterWS.Store(1)
	conns[0].abort(true)
	time.Sleep(time.Second)
	conns[1].abort(true)
	tm.checkProbes(tm.waitAll(minIdx, 3, 30*time.Second))
}

// 14 (B1): one shard's upgrade is never answered; the other shard's IDENTIFY is not blocked behind it.
func TestManager14_HangingUpgradeDoesNotBlockOtherShard(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	tm := newTestManager(t, gw, []uint{0, 1}, func(conf *ShardManagerConfig) {
		conf.timeouts.dial = time.Second
	})
	if err := tm.mngr.Connect(); err != nil {
		t.Fatal(err)
	}
	conns := tm.waitAll(0, 2, 20*time.Second)
	minIdx := gw.connCount()
	for _, fc := range conns {
		fc.invalidateSession() // both shards must IDENTIFY again
	}
	gw.hangDials.Store(1)
	conns[0].closeWith(4009, "session timed out")
	time.Sleep(200 * time.Millisecond) // shard 0's dial is hanging now
	start := time.Now()
	conns[1].closeWith(4009, "session timed out")
	if _, ok := gw.waitHealthy(minIdx, 1, 1, 20*time.Second); !ok {
		t.Fatal("shard 1 did not recover while shard 0 was stuck dialing")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("shard 1 was blocked for %s", d)
	}
	tm.checkProbes(tm.waitAll(minIdx, 2, 30*time.Second))
}

// 26: three shards start concurrently; one fatal close at startup stops all of them and Connect returns it.
func TestManager26_FatalAtStartup(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	gw.mu.Lock()
	gw.identifyCloseShard[1] = 4004
	gw.mu.Unlock()
	tm := newTestManager(t, gw, []uint{0, 1, 2}, nil)
	err := tm.mngr.Connect()
	var closeErr *GatewayCloseError
	if !errors.As(err, &closeErr) || closeErr.Code != 4004 {
		t.Fatalf("expected the fatal close code from shard 1, got %v", err)
	}
	if ids := tm.running(); len(ids) != 0 {
		t.Fatalf("every shard must be stopped after a failed startup, running: %v", ids)
	}
	dials := gw.dialAttempts.Load()
	time.Sleep(500 * time.Millisecond)
	if n := gw.dialAttempts.Load(); n != dials || openConns(gw) != 0 {
		t.Fatalf("no shard may connect after a failed startup (dials %d -> %d, open conns %d)", dials, n, openConns(gw))
	}

	// the manager can be connected again
	if err := tm.mngr.Connect(); err != nil {
		t.Fatal(err)
	}
	tm.checkProbes(tm.waitAll(int(gw.connCount())-3, 1, 30*time.Second))
}

func TestManager26_DisconnectDuringStartup(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	release := make(chan struct{})
	queue := func(_ uint, cb func() error) error {
		<-release
		return cb()
	}
	tm := newTestManager(t, gw, []uint{0, 1, 2}, func(conf *ShardManagerConfig) { conf.ConnectQueue = queue })
	errCh := make(chan error, 1)
	go func() { errCh <- tm.mngr.Connect() }()
	if !waitFor(10*time.Second, func() bool { return gw.connCount() == 3 }) {
		t.Fatal("shards did not dial")
	}
	if err := tm.mngr.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; !errors.Is(err, ErrDisconnected) {
		t.Fatalf("expected ErrDisconnected, got %v", err)
	}
	close(release)
	dials := gw.dialAttempts.Load()
	time.Sleep(500 * time.Millisecond)
	if n := gw.dialAttempts.Load(); n != dials || gw.totalIdentifies() != 0 {
		t.Fatalf("no shard may start after Disconnect (dials %d -> %d, identifies %d)", dials, n, gw.totalIdentifies())
	}
	if ids := tm.running(); len(ids) != 0 {
		t.Fatalf("shards still running: %v", ids)
	}
}

// 35: Disconnect racing Connect never leaves a shard running without a manager run.
func TestManager35_DisconnectRacingRegistration(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	tm := newTestManager(t, gw, []uint{0, 1, 2}, nil)
	for i := 0; i < 10; i++ {
		connectErr := make(chan error, 1)
		go func() { connectErr <- tm.mngr.Connect() }()
		time.Sleep(time.Duration(i) * 5 * time.Millisecond)
		if err := tm.mngr.Disconnect(); err != nil {
			t.Fatal(err)
		}
		err := <-connectErr
		if err == nil {
			// Disconnect ran before the registration; stop the started run
			if err := tm.mngr.Disconnect(); err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, ErrDisconnected) && !errors.Is(err, ErrStopping) {
			t.Fatalf("iteration %d: unexpected Connect error %v", i, err)
		}
		if ids := tm.running(); len(ids) != 0 {
			t.Fatalf("iteration %d: shards running after Disconnect: %v", i, ids)
		}
		tm.mngr.mu.RLock()
		m := tm.mngr.mrun
		tm.mngr.mu.RUnlock()
		if m != nil {
			t.Fatalf("iteration %d: manager run not cleared after Disconnect", i)
		}
	}
}

// 35: a shard that became READY and then stopped while another shard is still starting fails the startup.
func TestManager35_ReadyThenFatalWhileOthersStart(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	releaseShard1 := make(chan struct{})
	queue := func(shardID uint, cb func() error) error {
		if shardID == 1 {
			<-releaseShard1
		}
		return cb()
	}
	tm := newTestManager(t, gw, []uint{0, 1}, func(conf *ShardManagerConfig) { conf.ConnectQueue = queue })
	errCh := make(chan error, 1)
	go func() { errCh <- tm.mngr.Connect() }()
	fc0, ok := gw.waitHealthy(0, 0, 0, 20*time.Second)
	if !ok {
		t.Fatal("shard 0 never became ready")
	}
	fc0.closeWith(4014, "Disallowed intent(s).")
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "shard 0") {
			t.Fatalf("expected the startup to fail because of shard 0, got %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Connect did not return")
	}
	close(releaseShard1)
	if ids := tm.running(); len(ids) != 0 {
		t.Fatalf("every shard must be stopped, running: %v", ids)
	}
}

// 30: with a long identify turn wait every shard has exactly one queue call, also across session deaths.
func TestManager30_LongTurnWaitSingleQueueCall(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	release := make(chan struct{})
	var mu sync.Mutex
	calls := map[uint]int{}
	queue := func(shardID uint, cb func() error) error {
		mu.Lock()
		calls[shardID]++
		mu.Unlock()
		<-release
		return cb()
	}
	tm := newTestManager(t, gw, []uint{0, 1, 2}, func(conf *ShardManagerConfig) { conf.ConnectQueue = queue })
	errCh := make(chan error, 1)
	go func() { errCh <- tm.mngr.Connect() }()
	if !waitFor(10*time.Second, func() bool { return gw.connCount() == 3 }) {
		t.Fatal("shards did not dial")
	}
	time.Sleep(200 * time.Millisecond)
	for _, fc := range gw.allConns() {
		fc.abort(true) // sessions die while waiting for their turn
	}
	if !waitFor(10*time.Second, func() bool { return openConns(gw) == 3 && gw.connCount() == 6 }) {
		t.Fatal("shards did not reconnect")
	}
	time.Sleep(200 * time.Millisecond)
	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, id := range []uint{0, 1, 2} {
		if calls[id] != 1 {
			t.Fatalf("shard %d: expected exactly one queue call, got %d", id, calls[id])
		}
	}
}

// 42: startup-failure cleanup, concurrent Disconnects and a new Connect: the cleanup is never overlapped by a new
// run and never stops it; concurrent Disconnects wait for the same cleanup.
func TestManager42_CleanupOwnership(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 300)
	gw.mu.Lock()
	gw.identifyCloseShard[1] = 4004
	gw.mu.Unlock()
	releaseShard1 := make(chan struct{})
	queue := func(shardID uint, cb func() error) error {
		if shardID == 1 {
			<-releaseShard1
		}
		return cb()
	}
	tm := newTestManager(t, gw, []uint{0, 1}, func(conf *ShardManagerConfig) { conf.ConnectQueue = queue })

	errCh := make(chan error, 1)
	go func() { errCh <- tm.mngr.Connect() }()
	fc0, ok := gw.waitHealthy(0, 0, 0, 20*time.Second)
	if !ok {
		t.Fatal("shard 0 never became ready")
	}
	// shard 0's server stops reading once the next heartbeat arrived, so its close handshake takes ~5s and the
	// startup cleanup stays in progress meanwhile
	fc0.frozen.Store(true)
	if !waitFor(10*time.Second, func() bool { return gw.log.Count("frozen: dropping frame") > 0 }) {
		t.Fatal("no frame after freezing")
	}
	close(releaseShard1)

	// while the old startup cleans up: Disconnects coalesce and Connect can not overlap
	if !waitFor(10*time.Second, func() bool {
		tm.mngr.mu.RLock()
		defer tm.mngr.mu.RUnlock()
		m := tm.mngr.mrun
		if m == nil {
			t.Fatal("the cleanup finished too early for this test")
		}
		select {
		case <-m.stopping:
			return true
		default:
			return false
		}
	}) {
		t.Fatal("startup failure never started the cleanup")
	}
	if err := tm.mngr.Connect(); !errors.Is(err, ErrStopping) {
		t.Fatalf("expected ErrStopping while the old run is cleaned up, got %v", err)
	}
	var wg sync.WaitGroup
	disconnectErrs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			disconnectErrs <- tm.mngr.Disconnect()
		}()
	}
	wg.Wait()
	close(disconnectErrs)
	for err := range disconnectErrs {
		if err != nil {
			t.Fatalf("concurrent Disconnect: %v", err)
		}
	}
	var closeErr *GatewayCloseError
	if err := <-errCh; !errors.As(err, &closeErr) {
		t.Fatalf("expected the startup to fail with 4004, got %v", err)
	}

	// a new run after the cleanup completed is never touched by it
	minIdx := gw.connCount()
	if err := tm.mngr.Connect(); err != nil {
		t.Fatal(err)
	}
	conns := tm.waitAll(minIdx, 3, 30*time.Second)
	time.Sleep(500 * time.Millisecond)
	if ids := tm.running(); len(ids) != 2 {
		t.Fatalf("the new run was stopped, running: %v", ids)
	}
	tm.checkProbes(conns)
}

// E1: Emit holds the read lock and must never take it again; a pending writer would deadlock it.
func TestManager_EmitDoesNotDeadlockWithWriters(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	tm := newTestManager(t, gw, []uint{0, 1, 2}, nil)
	if err := tm.mngr.initShards(); err != nil {
		t.Fatal(err)
	}
	for _, shard := range tm.mngr.shards {
		shard.haveConnectedOnce.Store(true)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		stop := time.Now().Add(time.Second)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) {
				p := &RequestGuildMembersPayload{GuildIDs: []Snowflake{1 << 22, 2 << 22, 3 << 22}}
				_, _ = tm.mngr.Emit(cmd.RequestGuildMembers, p)
				_, _ = tm.mngr.Emit(cmd.UpdateVoiceState, &UpdateVoiceStatePayload{GuildID: 1 << 22})
			}
		}()
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) {
				tm.mngr.mu.Lock() // e.g. Connect/Disconnect/scaling
				tm.mngr.mu.Unlock()
			}
		}()
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Emit deadlocked with a pending writer")
	}
}

// E3: ShardIDs and HeartbeatLatencies are safe while the shard map changes (run with -race).
func TestManager_ShardMapAccessorsLocked(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	tm := newTestManager(t, gw, []uint{0}, nil)
	if err := tm.mngr.initShards(); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = tm.mngr.ShardIDs()
			_, _ = tm.mngr.HeartbeatLatencies()
		}
	}()
	for i := uint(1); i < 20; i++ {
		tm.mngr.mu.Lock()
		tm.mngr.conf.ShardIDs = append(tm.mngr.conf.ShardIDs, i)
		tm.mngr.conf.ShardCount++
		if err := tm.mngr.initShards(); err != nil {
			t.Fatal(err)
		}
		tm.mngr.mu.Unlock()
	}
	close(stop)
	wg.Wait()
	if n := len(tm.mngr.ShardIDs()); n != 20 {
		t.Fatalf("expected 20 shards, got %d", n)
	}
}

// S4 (B3, base: FAIL with "send on closed channel"): system shutdown while shards are starting.
func TestManager_ShutdownWhileStarting(t *testing.T) {
	t.Parallel()
	gw := newScenario(t, 500)
	gw.dropAfterWS.Store(2)
	tm := newTestManager(t, gw, []uint{0, 1}, nil)
	errCh := make(chan error, 1)
	go func() { errCh <- tm.mngr.Connect() }()
	if !waitFor(10*time.Second, func() bool { return gw.dialAttempts.Load() >= 2 }) {
		t.Fatal("shards did not dial")
	}
	tm.closeShutdown()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected Connect to fail after the system shutdown")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Connect did not return after the system shutdown")
	}
	if !waitFor(10*time.Second, func() bool { return len(tm.running()) == 0 }) {
		t.Fatalf("shards still running after shutdown: %v", tm.running())
	}
}

// Review item 2: READY and termination are separate results; the barrier must never report success when a shard
// already retired, and must report its terminal error.
func TestManagerStartupRejectsRetiredShard(t *testing.T) {
	fatal := newCloseError(4014)
	for i := 0; i < 1000; i++ {
		// A reaches READY and stops fatally before B reaches READY
		a := &run{first: make(chan error, 1), done: make(chan struct{}), terminal: fatal}
		a.first <- nil
		close(a.done)
		b := &run{first: make(chan error, 1), done: make(chan struct{})}
		b.first <- nil
		m := &mrun{
			runs:     map[shardID]*run{0: a, 1: b},
			stopping: make(chan struct{}),
			stopped:  make(chan struct{}),
		}
		err := (&shardMngr{}).awaitStartup(m)
		var closeErr *GatewayCloseError
		if !errors.As(err, &closeErr) || closeErr.Code != 4014 {
			t.Fatalf("schedule %d: expected the retired shard's fatal error, got %v", i, err)
		}
	}
}
