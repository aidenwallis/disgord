//go:build !integration
// +build !integration

package gateway

import (
	"testing"

	"github.com/aidenwallis/disgord/json"

	"github.com/aidenwallis/disgord/internal/constant"
	"github.com/aidenwallis/disgord/internal/gateway/cmd"
)

func TestEvtIdentify(t *testing.T) {
	i := &evtIdentity{
		Intents: IntentGuildMessageReactions,
	}
	var fields map[string]interface{}

	raw, err := json.Marshal(i)
	if err != nil {
		t.Error(err)
	}

	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Error(err)
	}

	if constant.DiscordVersion == 8 {
		if _, ok := fields["intents"]; !ok {
			t.Error("discord gateway 8 states that intents are mandatory")
			// https://discord.com/developers/docs/topics/gateway#gateway-intents
		}
	}
}

func TestEvtClient_communication(t *testing.T) {
	ss := newScriptShard(t, nil, nil, nil)
	connected := ss.connectAsync()

	sc := ss.d.await(t)
	sc.hello(45000)
	waitOp(t, sc, 2, 1) // IDENTIFY after HELLO
	sc.ready("sess-1")
	if err := awaitErr(t, connected); err != nil {
		t.Fatal(err)
	}
	sc.dispatch(2, "MESSAGE_CREATE", `{"id":"1"}`)
	if !waitFor(5e9, func() bool { return len(ss.events) == 2 }) {
		t.Fatalf("expected READY and MESSAGE_CREATE to be delivered, got %d events", len(ss.events))
	}

	// op 7: reconnect on a new connection with close code 4000 and RESUME from the last delivered event
	sc.send(map[string]interface{}{"op": 7, "d": nil})
	sc2 := ss.d.await(t)
	if code := sc.sentCloseCode(); code != 4000 {
		t.Fatalf("reconnects must close with 4000 to keep the session resumable, got %d", code)
	}
	sc2.hello(45000)
	waitOp(t, sc2, 6, 1)
	resume := sc2.writes()[0].Data.(*evtResume)
	if resume.SessionID != "sess-1" || resume.SequenceNr != 2 {
		t.Fatalf("unexpected RESUME %+v", resume)
	}
	sc2.dispatch(3, "RESUMED", `{}`)

	// op 9 d=false: never IDENTIFY on the same socket; a new connection identifies a new session
	sc2.send(map[string]interface{}{"op": 9, "d": false})
	sc3 := ss.d.await(t)
	if n := sc2.countOp(2); n != 0 {
		t.Fatalf("IDENTIFY must not be sent on the invalidated connection, got %d", n)
	}
	sc3.hello(45000)
	waitOp(t, sc3, 2, 1)
	if n := sc3.countOp(6); n != 0 {
		t.Fatal("an invalidated session must not be resumed")
	}
	sc3.ready("sess-2")

	// user commands are sent once READY was received
	if err := ss.c.Emit(cmd.UpdateStatus, &UpdateStatusPayload{Status: StatusOnline}); err != nil {
		t.Fatal(err)
	}
	waitOp(t, sc3, 3, 1)

	ss.c.mu.Lock()
	sessionID, seq := ss.c.sessionID, ss.c.seq
	ss.c.mu.Unlock()
	if sessionID != "sess-2" || seq != 1 {
		t.Fatalf("expected session sess-2 at seq 1, got %q at %d", sessionID, seq)
	}
}
