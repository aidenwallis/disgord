//go:build !integration
// +build !integration

package util

import (
	"reflect"
	"testing"
)

func TestTicketQueue_Delete(t *testing.T) {
	table := []struct {
		name   string
		delete Ticket
		wants  []Ticket
	}{
		{"head", 0, []Ticket{1, 2, 3, 4}},
		{"second", 1, []Ticket{0, 2, 3, 4}},
		{"middle", 2, []Ticket{0, 1, 3, 4}},
		{"tail", 4, []Ticket{0, 1, 2, 3}},
		{"unknown", 7, []Ticket{0, 1, 2, 3, 4}},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			var q TicketQueue
			for i := 0; i < 5; i++ {
				q.NewTicket()
			}

			q.Delete(tt.delete)
			if !reflect.DeepEqual(q.tickets, tt.wants) {
				t.Fatalf("got %v, wants %v", q.tickets, tt.wants)
			}

			// the remaining tickets must be served in order
			for _, ticket := range tt.wants {
				if !q.Next(ticket, func() bool { return true }) {
					t.Fatalf("ticket %d was not next in line, queue: %v", ticket, q.tickets)
				}
			}
			if q.Len() != 0 {
				t.Fatalf("queue should be empty, got %v", q.tickets)
			}
		})
	}
}

func TestTicketQueue_DeleteAll(t *testing.T) {
	var q TicketQueue
	tickets := make([]Ticket, 0, 4)
	for i := 0; i < 4; i++ {
		tickets = append(tickets, q.NewTicket())
	}

	// delete out of order
	for _, i := range []int{1, 3, 0, 2} {
		q.Delete(tickets[i])
	}
	if q.Len() != 0 {
		t.Fatalf("queue should be empty, got %v", q.tickets)
	}

	ticket := q.NewTicket()
	if !q.Next(ticket, func() bool { return true }) {
		t.Fatalf("new ticket should be next in line, queue: %v", q.tickets)
	}
}
