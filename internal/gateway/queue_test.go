//go:build !integration
// +build !integration

package gateway

import (
	"sync"
	"testing"

	"github.com/aidenwallis/disgord/internal/gateway/opcode"
)

func TestClientPktQueue_Add(t *testing.T) {
	limit := 5
	q := newClientPktQueue(limit)
	if !q.IsEmpty() {
		t.Error("should be empty")
	}

	if err := q.Add(&clientPacket{}); err != nil {
		t.Error(err)
	}

	if q.IsEmpty() {
		t.Error("should not be empty")
	}

	for i := 1; i < limit; i++ {
		_ = q.Add(&clientPacket{})
	}
	if len(q.messages) != q.limit {
		t.Fatal("number of entries in queue was less than the limit")
	}

	if err := q.Add(&clientPacket{}); err == nil {
		t.Error("expected an error when trying to add to a full queue")
	}

	q.Steal()
	_ = q.Add(&clientPacket{Op: opcode.EventStatusUpdate})
	if len(q.messages) != 1 {
		t.Fatal("number of entries in queue should be 1")
	}
	_ = q.Add(&clientPacket{Op: opcode.EventStatusUpdate})
	if len(q.messages) != 1 {
		t.Fatal("number of entries in queue should be 1")
	}
	_ = q.Add(&clientPacket{})
	if len(q.messages) != 2 {
		t.Fatal("number of entries in queue should be 2")
	}
}

func TestClientPktQueue_AddByOverwrite(t *testing.T) {
	q := newClientPktQueue(10)
	if !q.IsEmpty() {
		t.Error("should be empty")
	}

	if err := q.AddByOverwrite(&clientPacket{}); err == nil {
		t.Error("should complain that no similar entry to be overwritten was found")
	}

	if !q.IsEmpty() {
		t.Error("should be empty")
	}

	pkt := &clientPacket{}
	if err := q.Add(pkt); err != nil {
		t.Error(err)
	}

	if q.IsEmpty() {
		t.Error("should not be empty")
	}

	if err := q.AddByOverwrite(pkt); err != nil {
		t.Error(err)
	}

	if q.IsEmpty() {
		t.Error("should not be empty")
	}

	if len(q.messages) != 1 {
		t.Error("there should only be one item in the queue")
	}
}

func TestClientPktQueue_Steal(t *testing.T) {
	q := newClientPktQueue(10)
	_ = q.Add(&clientPacket{})
	if len(q.messages) != 1 {
		t.Error("there should only be one item in the queue")
	}

	q.Steal()
	if !q.IsEmpty() {
		t.Error("should be empty")
	}
}

func TestClientPktQueue_BeginCommitRollback(t *testing.T) {
	q := newClientPktQueue(10)

	if q.Begin() != nil {
		t.Fatal("Begin on an empty queue must return nil")
	}

	first, second := &clientPacket{CmdName: "1"}, &clientPacket{CmdName: "2"}
	_ = q.Add(first)
	_ = q.Add(second)

	msg := q.Begin()
	if msg != first {
		t.Fatal("Begin must return the head of the queue")
	}
	if q.Begin() != nil {
		t.Fatal("only one message can be inflight")
	}
	q.Rollback(msg)
	if len(q.messages) != 2 || q.messages[0] != first {
		t.Fatal("a rolled back message must return to the head of the queue")
	}

	msg = q.Begin()
	q.Commit(msg)
	if len(q.messages) != 1 || q.messages[0] != second || q.inflight != nil {
		t.Fatal("a committed message must leave the queue")
	}

	// commit/rollback by a stale owner is ignored
	msg = q.Begin()
	q.Rollback(first)
	q.Commit(first)
	if q.inflight != msg {
		t.Fatal("only the owner of the inflight message may commit or roll it back")
	}
}

// Astra R2#9 / test 37: the inflight message counts against the limit, so concurrent enqueues during a failed
// write can never overflow the queue.
func TestClientPktQueue_InflightCountsAgainstLimit(t *testing.T) {
	limit := 3
	q := newClientPktQueue(limit)
	for i := 0; i < limit; i++ {
		if err := q.Add(&clientPacket{Op: opcode.EventRequestGuildMembers}); err != nil {
			t.Fatal(err)
		}
	}
	msg := q.Begin()
	if err := q.Add(&clientPacket{Op: opcode.EventRequestGuildMembers}); err == nil {
		t.Fatal("queue must be full while a message is inflight")
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Add(&clientPacket{Op: opcode.EventRequestGuildMembers})
		}()
	}
	q.Rollback(msg)
	wg.Wait()
	if n := len(q.messages); n > limit {
		t.Fatalf("queue exceeded its limit: %d > %d", n, limit)
	}
}

func TestClientPktQueue_PresenceNewestWins(t *testing.T) {
	q := newClientPktQueue(10)
	older := &clientPacket{Op: opcode.EventStatusUpdate, CmdName: "older"}
	newer := &clientPacket{Op: opcode.EventStatusUpdate, CmdName: "newer"}

	_ = q.Add(older)
	inflight := q.Begin()
	_ = q.Add(newer) // queued while the older presence is being written
	q.Rollback(inflight)

	if len(q.messages) != 1 || q.messages[0] != newer {
		t.Fatalf("expected only the newest presence to remain, got %d messages", len(q.messages))
	}

	// without a newer presence, a failed presence write is retried
	q = newClientPktQueue(10)
	_ = q.Add(older)
	inflight = q.Begin()
	q.Rollback(inflight)
	if len(q.messages) != 1 || q.messages[0] != older {
		t.Fatal("a failed presence write must be retried when nothing newer was queued")
	}
}
