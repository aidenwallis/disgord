//go:build !integration
// +build !integration

package httd

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

func normalizedResponse(statusCode int, header http.Header) *http.Response {
	if header == nil {
		header = make(http.Header)
	}
	header.Set(DisgordNormalizedHeader, "true")
	return &http.Response{StatusCode: statusCode, Header: header}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// A request that gives up while queued must not prevent the requests behind it, nor later requests, from
// getting the bucket.
func TestLtBucket_QueuedTimeoutDoesNotWedgeBucket(t *testing.T) {
	table := []struct {
		name     string
		timesOut []bool // requests queued behind the bucket holder, in order
	}{
		{"head", []bool{true, false}},
		{"middle", []bool{false, true, false}},
		{"several", []bool{true, false, true, true, false}},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			bucket := newLeakyBucket(newLeakyBucket(nil))

			var mu sync.Mutex
			var served []int
			do := func(id int) bucketTransaction {
				return func() (*http.Response, []byte, error) {
					mu.Lock()
					served = append(served, id)
					mu.Unlock()
					return normalizedResponse(http.StatusOK, nil), nil, nil
				}
			}

			holding := make(chan struct{})
			release := make(chan struct{})
			holderDone := make(chan error, 1)
			go func() {
				_, _, err := bucket.Transaction(context.Background(), func() (*http.Response, []byte, error) {
					close(holding)
					<-release
					return normalizedResponse(http.StatusOK, nil), nil, nil
				})
				holderDone <- err
			}()
			select {
			case <-holding:
			case <-time.After(5 * time.Second):
				t.Fatal("holder never got the bucket")
			}

			results := make([]chan error, len(tt.timesOut))
			cancels := make([]context.CancelFunc, len(tt.timesOut))
			for i := range tt.timesOut {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				cancels[i] = cancel
				results[i] = make(chan error, 1)

				i := i
				go func() {
					_, _, err := bucket.Transaction(ctx, do(i))
					results[i] <- err
				}()
				waitFor(t, "request to be queued", func() bool { return bucket.queue.Len() == i+1 })
			}

			var wantServed []int
			for i, timesOut := range tt.timesOut {
				if !timesOut {
					wantServed = append(wantServed, i)
					continue
				}
				cancels[i]()
				if err := <-results[i]; err == nil {
					t.Fatalf("request %d should have timed out", i)
				}
			}

			close(release)
			if err := <-holderDone; err != nil {
				t.Fatal(err)
			}
			for _, i := range wantServed {
				if err := <-results[i]; err != nil {
					t.Fatalf("request %d queued behind a timed out request never got the bucket: %s", i, err)
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, _, err := bucket.Transaction(ctx, do(-1)); err != nil {
				t.Fatalf("new request never got the bucket: %s", err)
			}
			wantServed = append(wantServed, -1)

			mu.Lock()
			defer mu.Unlock()
			if len(served) != len(wantServed) {
				t.Fatalf("served %v, wants %v", served, wantServed)
			}
			for i := range served {
				if served[i] != wantServed[i] {
					t.Fatalf("served %v, wants %v", served, wantServed)
				}
			}
		})
	}
}
