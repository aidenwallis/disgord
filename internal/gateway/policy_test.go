//go:build !integration
// +build !integration

package gateway

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestPolicy(t *testing.T) {
	retry := action{}
	invalidKeep := action{invalidSession: true}
	invalidClear := action{clearSession: true, invalidSession: true}
	fatal := action{fatal: true}
	stop := action{stop: true}

	remote := func(code int) failReason { return failReason{kind: failRemoteClose, code: code} }
	testCases := []struct {
		name   string
		reason failReason
		phase  attemptPhase
		want   action
	}{
		{"dial error", failReason{kind: failTransport, err: errors.New("dial")}, phaseDial, retry},
		{"HELLO timeout", failReason{kind: failTransport}, phaseHello, retry},
		{"pre-HELLO EOF", failReason{kind: failTransport}, phaseHello, retry},
		{"write error", failReason{kind: failTransport}, phaseSession, retry},
		{"1005 no status", remote(1005), phaseSession, retry},
		{"1006 abnormal", remote(1006), phaseSession, retry},
		{"1000", remote(1000), phaseSession, retry},
		{"1001", remote(1001), phaseSession, retry},
		{"1011", remote(1011), phaseSession, retry},
		{"4000", remote(4000), phaseSession, retry},
		{"4001", remote(4001), phaseSession, retry},
		{"4002", remote(4002), phaseSession, retry},
		{"4003", remote(4003), phaseSession, retry},
		{"4005", remote(4005), phaseSession, retry},
		{"4008", remote(4008), phaseSession, retry},
		{"unknown 4xxx", remote(4999), phaseSession, retry},
		{"op 7", failReason{kind: failReconnect}, phaseSession, retry},
		{"zombie", failReason{kind: failZombie}, phaseSession, retry},
		{"ready timeout", failReason{kind: failReadyTimeout}, phaseReady, retry},
		{"op 9 d=true", failReason{kind: failInvalidSession, resumable: true}, phaseSession, invalidKeep},
		{"op 9 d=false", failReason{kind: failInvalidSession}, phaseReady, invalidClear},
		{"4007", remote(4007), phaseSession, invalidClear},
		{"4009", remote(4009), phaseSession, invalidClear},
		{"4004", remote(4004), phaseReady, fatal},
		{"4010", remote(4010), phaseReady, fatal},
		{"4011", remote(4011), phaseSession, fatal},
		{"4012", remote(4012), phaseReady, fatal},
		{"4013", remote(4013), phaseReady, fatal},
		{"4014", remote(4014), phaseReady, fatal},
		{"stop", failReason{kind: failStop}, phaseSession, stop},
		{"stop while dialing", failReason{kind: failStop}, phaseDial, stop},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := policy(outcome{phase: tc.phase, reason: tc.reason}); got != tc.want {
				t.Fatalf("policy(%s) = %+v, want %+v", tc.reason, got, tc.want)
			}
		})
	}
}

func newTestSession() *session {
	c := &EvtClient{log: newTestLogger("session")}
	return c.newSession(nil)
}

func TestFailReasonArbitration(t *testing.T) {
	transport := failReason{kind: failTransport, err: errors.New("write: broken pipe")}
	remote := func(code int) failReason { return failReason{kind: failRemoteClose, code: code} }
	testCases := []struct {
		name     string
		observed []failReason
		want     failReason
	}{
		{"fatal close after write error", []failReason{transport, remote(4004)}, remote(4004)},
		{"write error after fatal close", []failReason{remote(4014), transport}, remote(4014)},
		{"invalid session beats a later close", []failReason{{kind: failInvalidSession}, remote(1000)}, failReason{kind: failInvalidSession}},
		{"4009 after write error", []failReason{transport, remote(4009)}, remote(4009)},
		{"genuine close upgrades transport", []failReason{transport, remote(1001)}, remote(1001)},
		{"zombie is not overwritten by a close", []failReason{{kind: failZombie}, remote(1000)}, failReason{kind: failZombie}},
		{"stop beats everything", []failReason{remote(4004), {kind: failStop}}, failReason{kind: failStop}},
		{"ties keep the first", []failReason{{kind: failReconnect}, {kind: failZombie}}, failReason{kind: failReconnect}},
		{"fatal beats invalid session", []failReason{{kind: failInvalidSession}, remote(4004)}, remote(4004)},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSession()
			for _, r := range tc.observed {
				s.fail(r)
			}
			got := s.freeze()
			if got.kind != tc.want.kind || got.code != tc.want.code {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}

	t.Run("frozen after receiver join", func(t *testing.T) {
		s := newTestSession()
		s.fail(transport)
		s.freeze()
		s.fail(remote(4004))
		if got := s.freeze(); got.kind != failTransport {
			t.Fatalf("reasons after the freeze must be ignored, got %s", got)
		}
	})
}

func TestEchoRule(t *testing.T) {
	s := newTestSession()
	echo := &CloseErr{code: 4000, info: "sent close frame"}

	// before we sent anything a 4000 is genuine
	if r, genuine := s.readFailure(echo); !genuine || r.code != 4000 {
		t.Fatalf("a remote 4000 before our close must be genuine, got %v %v", r, genuine)
	}

	s.sentClose.Store(4000)
	if _, genuine := s.readFailure(echo); genuine {
		t.Fatal("our own close code echoed back must be ignored")
	}
	if r, genuine := s.readFailure(&CloseErr{code: 4004}); !genuine || r.code != 4004 {
		t.Fatal("a different remote code after our close is genuine")
	}
	if r, genuine := s.readFailure(errors.New("EOF")); !genuine || r.kind != failTransport {
		t.Fatal("a plain read error is a transport failure")
	}
	if r, _ := s.readFailure(fmt.Errorf("wrapped: %w", &CloseErr{code: 4014})); r.code != 4014 {
		t.Fatal("wrapped close errors must be recognised")
	}
}

// nhooyr wrapper methods must not panic before Open succeeded (base: nil pointer dereference on Disconnect).
func TestNhooyrWrapperNilSafe(t *testing.T) {
	g := newSessionConn(nil, time.Second, time.Second)
	if err := g.WriteJSON(map[string]int{"op": 1}); err == nil {
		t.Fatal("expected an error writing to an unopened connection")
	}
	if _, err := g.Read(context.Background()); err == nil {
		t.Fatal("expected an error reading from an unopened connection")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.CloseWithStatus(4000, ""); err != nil {
		t.Fatal(err)
	}
	if !g.Disconnected() {
		t.Fatal("expected disconnected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Open(ctx, "ws://127.0.0.1:1", nil); err == nil {
		t.Fatal("expected the dial to fail")
	}
	_ = g.Close()
	if _, err := g.Read(ctx); err == nil {
		t.Fatal("expected an error after a failed Open")
	}
}

// An ACK processed while the heartbeat write is still in progress must clear the outstanding beat: the beat is
// marked before it is handed to the connection.
func TestHeartbeatMarkedBeforeWrite(t *testing.T) {
	c := &EvtClient{log: newTestLogger("hb"), to: defaultTimeouts()}
	s := c.newSession(nil)
	sc := newScriptConn(0)
	sc.onWrite = func(_ *scriptConn, p *clientPacket) error {
		s.onHeartbeatAck(time.Now()) // the receiver handles the ACK before WriteJSON returns
		return nil
	}
	s.conn = sc
	s.interval.Store(int64(time.Second))

	if !s.sendHeartbeat() {
		t.Fatal("heartbeat write failed")
	}
	s.hb.mu.Lock()
	outstanding := s.hb.outstanding
	s.hb.mu.Unlock()
	if outstanding {
		t.Fatal("the ACK that raced the write completion was lost")
	}
}
