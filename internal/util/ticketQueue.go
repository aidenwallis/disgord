package util

import (
	"sync"
)

type Ticket int

const (
	NoTicket Ticket = -1
)

type TicketQueue struct {
	mu         sync.Mutex
	tickets    []Ticket
	nextTicket Ticket
}

func (q *TicketQueue) NewTicket() (ticket Ticket) {
	q.mu.Lock()
	defer q.mu.Unlock()
	defer func() {
		q.nextTicket++
	}()

	ticket = q.nextTicket
	q.tickets = append(q.tickets, ticket)

	return ticket
}

func (q *TicketQueue) Delete(ticket Ticket) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var i int
	var ok bool
	for i = range q.tickets {
		if q.tickets[i] == ticket {
			ok = true
			break
		}
	}
	if !ok {
		return
	}

	q.tickets = append(q.tickets[:i], q.tickets[i+1:]...)
}

// Len returns the number of tickets waiting in the queue.
func (q *TicketQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tickets)
}

func (q *TicketQueue) Next(ticket Ticket, cb func() bool) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.tickets) == 0 {
		return false
	}

	if q.tickets[0] != ticket {
		return false
	}

	if ok := cb(); !ok {
		return false
	}

	if len(q.tickets) > 1 {
		q.tickets = q.tickets[1:]
	} else {
		q.tickets = q.tickets[:0]
	}
	return true
}
