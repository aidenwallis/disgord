//go:build !integration
// +build !integration

package httd

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(t *testing.T, doer HttpClientDoer, requestTimeout time.Duration) *Client {
	t.Helper()
	c, err := NewClient(&Config{
		APIVersion:         9,
		BotToken:           "test",
		HttpClient:         doer,
		UserAgentSourceURL: "https://example.com",
		UserAgentVersion:   "test",
		RequestTimeout:     requestTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type doerFunc func(req *http.Request) (*http.Response, error)

func (f doerFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRequestTimeout(t *testing.T) {
	table := map[time.Duration]time.Duration{
		0:                DefaultRequestTimeout,
		-1:               0,
		-time.Second:     0,
		3 * time.Second:  3 * time.Second,
		20 * time.Minute: 20 * time.Minute,
	}
	for configured, wants := range table {
		if got := requestTimeout(configured); got != wants {
			t.Errorf("requestTimeout(%s) = %s, wants %s", configured, got, wants)
		}
	}
}

func TestClient_ExchangeDeadline(t *testing.T) {
	callerDeadline := time.Now().Add(time.Hour)
	table := []struct {
		name           string
		requestTimeout time.Duration
		ctx            func() (context.Context, context.CancelFunc)
		check          func(t *testing.T, deadline time.Time, ok bool)
	}{
		{
			name:           "default",
			requestTimeout: 0,
			ctx:            func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			check: func(t *testing.T, deadline time.Time, ok bool) {
				if left := time.Until(deadline); !ok || left > DefaultRequestTimeout || left < DefaultRequestTimeout-time.Minute {
					t.Errorf("expected a deadline in about %s, got %t %s", DefaultRequestTimeout, ok, left)
				}
			},
		},
		{
			name:           "disabled",
			requestTimeout: -1,
			ctx:            func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			check: func(t *testing.T, deadline time.Time, ok bool) {
				if ok {
					t.Errorf("expected no deadline, got %s", deadline)
				}
			},
		},
		{
			name:           "caller deadline",
			requestTimeout: time.Second,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), callerDeadline)
			},
			check: func(t *testing.T, deadline time.Time, ok bool) {
				if !ok || !deadline.Equal(callerDeadline) {
					t.Errorf("expected the caller deadline %s, got %t %s", callerDeadline, ok, deadline)
				}
			},
		},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			var deadline time.Time
			var ok bool
			c := newTestClient(t, doerFunc(func(req *http.Request) (*http.Response, error) {
				deadline, ok = req.Context().Deadline()
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       ioutil.NopCloser(bytes.NewBufferString(`{}`)),
				}, nil
			}), tt.requestTimeout)

			ctx, cancel := tt.ctx()
			defer cancel()
			if _, _, err := c.Do(ctx, &Request{Endpoint: "/users/@me"}); err != nil {
				t.Fatal(err)
			}
			tt.check(t, deadline, ok)
		})
	}
}

// A request to a server that stops responding must give up the bucket, so the requests queued behind it proceed.
func TestClient_StalledExchangeReleasesBucket(t *testing.T) {
	for _, stall := range []string{"headers", "body"} {
		stall := stall
		t.Run(stall, func(t *testing.T) {
			var calls int32
			stalled := make(chan struct{})
			unblock := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if atomic.AddInt32(&calls, 1) > 1 {
					w.Header().Set(ContentType, ContentTypeJSON)
					_, _ = w.Write([]byte(`{"id":"2"}`))
					return
				}

				if stall == "body" {
					w.Header().Set("Content-Length", "100")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"id":`))
					w.(http.Flusher).Flush()
				}
				close(stalled)
				select {
				case <-unblock:
				case <-r.Context().Done():
				}
			}))
			defer srv.Close()
			defer close(unblock)

			const timeout = 200 * time.Millisecond
			c := newTestClient(t, srv.Client(), timeout)
			c.url = srv.URL

			type result struct {
				body []byte
				err  error
			}
			do := func() chan result {
				ch := make(chan result, 1)
				go func() {
					_, body, err := c.Do(context.Background(), &Request{Method: http.MethodPost, Endpoint: "/channels/1/messages"})
					ch <- result{body, err}
				}()
				return ch
			}

			first := do()
			select {
			case <-stalled:
			case <-time.After(5 * time.Second):
				t.Fatal("first request never reached the server")
			}
			second := do()

			select {
			case res := <-first:
				if res.err == nil {
					t.Fatal("stalled request should have failed")
				}
			case <-time.After(25 * timeout):
				t.Fatal("stalled request was not aborted")
			}

			select {
			case res := <-second:
				if res.err != nil {
					t.Fatal(res.err)
				}
				if string(res.body) != `{"id":"2"}` {
					t.Fatalf("unexpected body %s", string(res.body))
				}
			case <-time.After(25 * timeout):
				t.Fatal("request queued behind the stalled request never completed")
			}
		})
	}
}
