package gateway

import (
	"errors"
	"sync"

	"github.com/aidenwallis/disgord/internal/gateway/opcode"
)

func newClientPktQueue(limit int) clientPktQueue {
	if limit == 0 {
		limit = -1 // no limit
	}
	return clientPktQueue{
		limit: limit,
	}
}

// clientPktQueue is an ordered queue. Entries are not removed unless they are successfully written to the websocket.
//
// Writing happens outside the lock: Begin moves the head into an inflight slot (which still counts against the
// limit), and the writer then either Commits it or Rolls it back to the head of the queue. Delivery is therefore
// at-least-once: a write that fails ambiguously is sent again on the next connection.
type clientPktQueue struct {
	sync.RWMutex
	messages []*clientPacket
	inflight *clientPacket
	limit    int
}

func (c *clientPktQueue) IsEmpty() bool {
	c.RLock()
	defer c.RUnlock()

	return len(c.messages) == 0
}

func (c *clientPktQueue) AddByOverwrite(msg *clientPacket) error {
	c.Lock()
	defer c.Unlock()

	for i := range c.messages {
		if c.messages[i].Op == msg.Op {
			c.messages[i] = msg
			return nil
		}
	}
	return errors.New("no entry with existing operation code")
}

func (c *clientPktQueue) Add(msg *clientPacket) error {
	if msg.Op == opcode.EventStatusUpdate {
		if err := c.AddByOverwrite(msg); err == nil {
			return nil
		}
	}

	c.Lock()
	defer c.Unlock()
	if c.limit >= 0 && c.size() >= c.limit {
		return errors.New("can not send anymore messages, queue is full")
	}

	c.messages = append(c.messages, msg)
	return nil
}

func (c *clientPktQueue) size() int {
	if c.inflight != nil {
		return len(c.messages) + 1
	}
	return len(c.messages)
}

// Begin removes the head of the queue and marks it as inflight. Returns nil if the queue is empty or another
// message is already inflight.
func (c *clientPktQueue) Begin() *clientPacket {
	c.Lock()
	defer c.Unlock()
	if c.inflight != nil || len(c.messages) == 0 {
		return nil
	}

	c.inflight = c.messages[0]
	// shift to avoid re-allocations
	copy(c.messages, c.messages[1:])
	c.messages[len(c.messages)-1] = nil
	c.messages = c.messages[:len(c.messages)-1]
	return c.inflight
}

// Commit drops the inflight message after it was written.
func (c *clientPktQueue) Commit(msg *clientPacket) {
	c.Lock()
	defer c.Unlock()
	if c.inflight == msg {
		c.inflight = nil
	}
}

// Rollback puts the inflight message back at the head of the queue. A presence update is dropped instead when a
// newer one was queued meanwhile; the newest presence wins.
func (c *clientPktQueue) Rollback(msg *clientPacket) {
	c.Lock()
	defer c.Unlock()
	if c.inflight != msg {
		return
	}
	c.inflight = nil

	if msg.Op == opcode.EventStatusUpdate {
		for i := range c.messages {
			if c.messages[i].Op == opcode.EventStatusUpdate {
				return
			}
		}
	}

	c.messages = append(c.messages, nil)
	copy(c.messages[1:], c.messages)
	c.messages[0] = msg
}

func (c *clientPktQueue) Steal() (m []*clientPacket) {
	c.Lock()
	defer c.Unlock()

	m = make([]*clientPacket, len(c.messages))
	copy(m, c.messages)
	for i := range c.messages {
		c.messages[i] = nil // redundant?
	}
	c.messages = c.messages[:0]
	return m
}
