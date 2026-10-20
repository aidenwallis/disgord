//go:build !integration
// +build !integration

package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/atomic"
)

func ackSetup(sc *scriptConn) { sc.onWrite = ackHeartbeats }

// Review item 3: a custom queue may keep its call open after the callback returned (e.g. to hold a distributed
// lease through the spacing). The slot stays owned until the call returns: no further queue call meanwhile.
func TestIdentifyOneOutstandingQueueCallAcrossRuns(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	returnedFromCallback := make(chan struct{}, 10)
	queue := func(_ uint, cb func() error) error {
		calls.Inc()
		err := cb()
		returnedFromCallback <- struct{}{}
		<-release
		return err
	}
	ss := newScriptShard(t, nil, ackSetup, queue)
	for i := 0; i < 4; i++ {
		connected := ss.connectAsync()
		sc := ss.d.await(t)
		sc.hello(45000)
		if i == 0 {
			select {
			case <-returnedFromCallback:
			case <-time.After(10 * time.Second):
				t.Fatal("initial IDENTIFY callback did not return")
			}
		} else {
			time.Sleep(50 * time.Millisecond) // the outstanding queue call has still not returned
		}
		if err := ss.c.Disconnect(); err != nil {
			t.Fatal(err)
		}
		_ = awaitErr(t, connected)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("%d queue calls after 4 runs; want 1 while the first one is still outstanding", got)
	}

	// once the outstanding call returns, a new run gets a new turn and identifies
	close(release)
	connected := ss.connectAsync()
	sc := ss.d.await(t)
	sc.hello(45000)
	waitOp(t, sc, 2, 1)
	sc.ready("sess-2")
	if err := awaitErr(t, connected); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected a second queue call after the first returned, got %d", got)
	}
}

// identifyFixture prepares a current session waiting for its identify turn, without a supervisor.
func identifyFixture(t *testing.T, to *timeouts) (*scriptShard, *session, *scriptConn, *run) {
	ss := newScriptShard(t, to, nil, nil)
	rctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r := &run{ctx: rctx, cancel: cancel}
	s := ss.c.newSession(r)
	s.interval.Store(int64(2 * time.Second))
	sc := newScriptConn(1)
	s.conn = sc
	ss.c.mu.Lock()
	ss.c.current = s
	ss.c.identSess = s
	ss.c.mu.Unlock()
	t.Cleanup(func() {
		s.fail(failReason{kind: failStop})
		s.wgWork.Wait()
		s.readCancel()
	})
	return ss, s, sc, r
}

// Review item 4: an admitted identify turn is bounded before the writer accepted IDENTIFY, and a frame that was
// not accepted is never written later.
func TestIdentifyTurnBoundedBeforeAcceptance(t *testing.T) {
	to := fastTimeouts()
	to.write = 25 * time.Millisecond
	to.outboundWindow = 2 * time.Second
	to.outboundLimit = 10
	ss, s, sc, r := identifyFixture(t, to)

	// use up the control share with heartbeats requested by Discord just before the turn is granted
	s.alive.writer.Store(true)
	s.wgWork.Add(1)
	go s.write()
	_, controlLimit := outboundLimits(2*time.Second, to.outboundWindow, to.outboundLimit)
	for i := 0; i < controlLimit; i++ {
		s.beatNow <- struct{}{}
		waitOp(t, sc, 1, i+1)
	}

	q := &identifyRequest{run: r, done: make(chan struct{})}
	turnDone := make(chan error, 1)
	go func() { turnDone <- ss.c.identifyTurn(q) }()
	select {
	case err := <-turnDone:
		if !errors.Is(err, errIdentifyNotSent) {
			t.Fatalf("expected errIdentifyNotSent, got %v", err)
		}
	case <-time.After(10 * to.write):
		t.Fatal("the identify turn was not bounded before writer acceptance")
	}

	time.Sleep(to.outboundWindow + 200*time.Millisecond) // the budget frees up again
	if n := sc.countOp(2); n != 0 {
		t.Fatalf("an IDENTIFY that was not accepted must never be written, got %d", n)
	}
}

// Review item 9: no IDENTIFY goes out once Disconnect cancelled the run, even before the session failed.
func TestIdentifyTurnAfterRunCancelled(t *testing.T) {
	ss, s, sc, r := identifyFixture(t, nil)
	s.alive.writer.Store(true)
	s.wgWork.Add(1)
	go s.write()

	r.cancel()
	q := &identifyRequest{run: r, done: make(chan struct{})}
	if err := ss.c.identifyTurn(q); !errors.Is(err, errIdentifyNotSent) {
		t.Fatalf("expected errIdentifyNotSent after the run was cancelled, got %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := sc.countOp(2); n != 0 {
		t.Fatalf("IDENTIFY written after Disconnect started: %d", n)
	}
}
