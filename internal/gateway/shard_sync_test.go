//go:build !integration
// +build !integration

package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/aidenwallis/disgord/internal/logger"
)

func newTestShardSync(conf ShardConfig) (*shardSync, chan interface{}) {
	shutdown := make(chan interface{})
	return newShardSync(&conf, logger.Empty{}, "[test]", shutdown), shutdown
}

// admitAt simulates the process loop with explicit timestamps: admission at admitAt, write completion at doneAt.
func admitAt(t *testing.T, s *shardSync, admitAt, doneAt time.Time) bool {
	t.Helper()
	if s.admissionWait(admitAt) > 0 {
		return false
	}
	s.charge(doneAt)
	return true
}

// Astra R3#3: Total=2, Remaining=1, reset at t=10s; admissions at t=11, 17, 23 must admit exactly 2 in the new window.
func TestShardSync_ServerBudgetRollover(t *testing.T) {
	base := time.Now()
	at := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

	s, shutdown := newTestShardSync(ShardConfig{
		IdentifiesPer24H: 1000,
		sessionStart:     sessionStartLimit{total: 2, remaining: 1, resetAfter: 10 * time.Second, observedAt: base},
	})
	defer close(shutdown)

	var admitted int
	for _, sec := range []int{11, 17, 23} {
		if admitAt(t, s, at(sec), at(sec)) {
			admitted++
		}
	}
	if admitted != 2 {
		t.Fatalf("expected exactly 2 identifies in the new window, got %d", admitted)
	}

	wait := s.admissionWait(at(23))
	if want := 24*time.Hour - 13*time.Second; wait != want {
		t.Fatalf("expected to wait for the next reset (%s), got %s", want, wait)
	}

	// multiple elapsed windows are applied at once
	if !admitAt(t, s, at(10+3*24*3600), at(10+3*24*3600)) {
		t.Fatal("expected admission after several elapsed windows")
	}
	if s.server.remaining != 1 {
		t.Fatalf("expected the budget to be refilled once and charged once, got remaining=%d", s.server.remaining)
	}
}

// §10.1: an identify admitted in the old window but completed in the new one is charged against the new window.
func TestShardSync_ChargeInCompletionWindow(t *testing.T) {
	base := time.Now()
	at := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

	s, shutdown := newTestShardSync(ShardConfig{
		IdentifiesPer24H: 1000,
		sessionStart:     sessionStartLimit{total: 2, remaining: 1, resetAfter: 10 * time.Second, observedAt: base},
	})
	defer close(shutdown)

	// admit t=9 (old window), write at t=11, finish t=12
	if !admitAt(t, s, at(9), at(12)) {
		t.Fatal("expected the straddling identify to be admitted in the old window")
	}
	writesInNewWindow := 1 // the straddling write landed at t=11
	for _, sec := range []int{13, 14, 15, 16} {
		if admitAt(t, s, at(sec), at(sec)) {
			writesInNewWindow++
		}
	}
	if writesInNewWindow > 2 {
		t.Fatalf("at most 2 writes may land in the new window, got %d", writesInNewWindow)
	}
}

func TestShardSync_ZeroRemaining(t *testing.T) {
	base := time.Now()
	s, shutdown := newTestShardSync(ShardConfig{
		IdentifiesPer24H: 1000,
		sessionStart:     sessionStartLimit{total: 1000, remaining: 0, resetAfter: time.Hour, observedAt: base},
	})
	defer close(shutdown)

	if wait := s.admissionWait(base); wait != time.Hour {
		t.Fatalf("expected to wait until reset (1h), got %s", wait)
	}
	if wait := s.admissionWait(base.Add(time.Hour - time.Millisecond)); wait != time.Millisecond {
		t.Fatalf("near reset: expected 1ms, got %s", wait)
	}
	if wait := s.admissionWait(base.Add(time.Hour)); wait != 0 {
		t.Fatalf("at reset: expected no wait, got %s", wait)
	}
}

func TestShardSync_LocalCapLowerThanServer(t *testing.T) {
	base := time.Now()
	s, shutdown := newTestShardSync(ShardConfig{
		IdentifiesPer24H: 2,
		sessionStart:     sessionStartLimit{total: 1000, remaining: 1000, resetAfter: time.Hour, observedAt: base},
	})
	defer close(shutdown)

	if !admitAt(t, s, base, base) || !admitAt(t, s, base.Add(time.Second), base.Add(time.Second)) {
		t.Fatal("expected the first two identifies to be admitted")
	}
	if wait := s.admissionWait(base.Add(2 * time.Second)); wait != 24*time.Hour-2*time.Second {
		t.Fatalf("expected the local cap to hold the third identify until the first leaves the window, got %s", wait)
	}
}

// Zero remaining at startup: the queue must not run a single identify before the reset.
func TestShardSync_ZeroRemainingBlocksUntilReset(t *testing.T) {
	reset := 400 * time.Millisecond
	s, shutdown := newTestShardSync(ShardConfig{
		IdentifiesPer24H: 1000,
		ShardRateLimit:   time.Millisecond,
		sessionStart:     sessionStartLimit{total: 5, remaining: 0, resetAfter: reset, observedAt: time.Now()},
	})
	s.pollInterval = 20 * time.Millisecond
	defer close(shutdown)
	go s.process()

	start := time.Now()
	var ranAt time.Duration
	if err := s.queueShard(0, func() error { ranAt = time.Since(start); return nil }); err != nil {
		t.Fatal(err)
	}
	if ranAt < reset-50*time.Millisecond {
		t.Fatalf("identify ran after %s, before the server budget reset (%s)", ranAt, reset)
	}
}

func TestShardSync_AbandonedItemIsSkipped(t *testing.T) {
	s, shutdown := newTestShardSync(ShardConfig{
		IdentifiesPer24H: 1000,
		ShardRateLimit:   time.Hour, // a charged turn would block the next item for an hour
		sessionStart:     sessionStartLimit{total: 5, remaining: 0, resetAfter: time.Hour, observedAt: time.Now()},
	})
	s.pollInterval = 10 * time.Millisecond
	defer close(shutdown)
	go s.process()

	var abandoned atomic.Bool
	errCh := make(chan error, 1)
	var ran atomic.Bool
	go func() {
		errCh <- s.queueIdentify(0, func() error { ran.Store(true); return nil }, abandoned.Load)
	}()

	time.Sleep(50 * time.Millisecond) // parked behind the exhausted server budget
	abandoned.Store(true)

	select {
	case err := <-errCh:
		if !errors.Is(err, errIdentifyNotSent) {
			t.Fatalf("expected errIdentifyNotSent, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an abandoned item must not sit out the budget reset")
	}
	if ran.Load() {
		t.Fatal("abandoned item must never run")
	}
	if n := s.metric.ReconnectsSince(24 * time.Hour); n != 0 {
		t.Fatalf("abandoned item must not be charged, got %d", n)
	}
}

func TestShardSync_NotSentIsUnchargedAndUnspaced(t *testing.T) {
	s, shutdown := newTestShardSync(ShardConfig{IdentifiesPer24H: 1000, ShardRateLimit: time.Hour})
	defer close(shutdown)
	go s.process()

	if err := s.queueShard(0, func() error { return errIdentifyNotSent }); !errors.Is(err, errIdentifyNotSent) {
		t.Fatalf("expected errIdentifyNotSent, got %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.queueShard(1, func() error { return nil }) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an uncharged turn must not be followed by identify spacing")
	}
	if n := s.metric.ReconnectsSince(24 * time.Hour); n != 1 {
		t.Fatalf("expected exactly one charged identify, got %d", n)
	}
}

func TestShardSync_FailedWriteIsCharged(t *testing.T) {
	s, shutdown := newTestShardSync(ShardConfig{IdentifiesPer24H: 1000, ShardRateLimit: time.Hour})
	defer close(shutdown)
	go s.process()

	writeErr := errors.New("write failed")
	if err := s.queueShard(0, func() error { return writeErr }); !errors.Is(err, writeErr) {
		t.Fatalf("expected the write error, got %v", err)
	}
	if n := s.metric.ReconnectsSince(24 * time.Hour); n != 1 {
		t.Fatalf("an ambiguous write must be charged, got %d", n)
	}
	if eta := s.admissionETA(); eta != 0 {
		t.Fatalf("spacing is not part of the budget admission, got eta %s", eta)
	}
}

// B3: shutting down while a turn is running must not panic with "send on closed channel".
func TestShardSync_ShutdownWhileRunningNoPanic(t *testing.T) {
	s, shutdown := newTestShardSync(ShardConfig{IdentifiesPer24H: 1000, ShardRateLimit: time.Millisecond})
	processDone := make(chan struct{})
	go func() {
		defer close(processDone)
		s.process()
	}()

	release := make(chan struct{})
	running := make(chan struct{})
	queueErr := make(chan error, 1)
	go func() {
		queueErr <- s.queueShard(0, func() error {
			close(running)
			<-release
			return nil
		})
	}()

	<-running
	close(shutdown)
	if err := <-queueErr; !errors.Is(err, errShardSyncShutdown) {
		t.Fatalf("expected shutdown error, got %v", err)
	}
	close(release) // process now reports the result of the running turn
	select {
	case <-processDone:
	case <-time.After(2 * time.Second):
		t.Fatal("process did not exit after shutdown")
	}

	// enqueueing after shutdown never blocks
	if err := s.queueShard(1, func() error { return nil }); !errors.Is(err, errShardSyncShutdown) {
		t.Fatalf("expected shutdown error, got %v", err)
	}
}

func TestConfigureShardConfig_SessionStartLimit(t *testing.T) {
	gb := &GatewayBot{Shards: 1, Gateway: Gateway{"localhost:6060"}}
	gb.SessionStartLimit.Total = 2000
	gb.SessionStartLimit.Remaining = 3
	gb.SessionStartLimit.ResetAfter = 60000
	mock := &GatewayBotGetterMock{get: func() (*GatewayBot, error) { return gb, nil }}

	conf := ShardConfig{}
	if err := ConfigureShardConfig(context.Background(), mock, &conf); err != nil {
		t.Fatal(err)
	}
	if conf.IdentifiesPer24H != 2000 {
		t.Fatalf("IdentifiesPer24H should be seeded from session_start_limit.total before the default, got %d", conf.IdentifiesPer24H)
	}
	if conf.sessionStart.total != 2000 || conf.sessionStart.remaining != 3 || conf.sessionStart.resetAfter != time.Minute {
		t.Fatalf("session start limit not stored: %+v", conf.sessionStart)
	}

	conf = ShardConfig{IdentifiesPer24H: 10}
	if err := ConfigureShardConfig(context.Background(), mock, &conf); err != nil {
		t.Fatal(err)
	}
	if conf.IdentifiesPer24H != 10 {
		t.Fatalf("a configured IdentifiesPer24H must be kept, got %d", conf.IdentifiesPer24H)
	}

	s, shutdown := newTestShardSync(conf)
	defer close(shutdown)
	if !s.server.known || s.server.remaining != 3 {
		t.Fatalf("shard sync must track the server budget, got %+v", s.server)
	}
}

func TestShardSync_ConcurrentQueueing(t *testing.T) {
	s, shutdown := newTestShardSync(ShardConfig{IdentifiesPer24H: 1000, ShardRateLimit: time.Millisecond})
	defer close(shutdown)
	go s.process()

	var running atomic.Int32
	var overlap atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id uint) {
			defer wg.Done()
			_ = s.queueShard(id, func() error {
				if running.Inc() > 1 {
					overlap.Store(true)
				}
				time.Sleep(time.Millisecond)
				running.Dec()
				return nil
			})
		}(uint(i))
	}
	wg.Wait()
	if overlap.Load() {
		t.Fatal("identify turns must never overlap")
	}
	if n := s.metric.ReconnectsSince(time.Hour); n != 10 {
		t.Fatalf("expected 10 charged identifies, got %d", n)
	}
}
