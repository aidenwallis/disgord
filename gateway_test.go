//go:build !integration
// +build !integration

package disgord

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"
	"nhooyr.io/websocket"

	"github.com/aidenwallis/disgord/internal/gateway"
)

// rootFakeGateway is a minimal Discord gateway: HELLO, IDENTIFY -> READY, heartbeat ACKs.
type rootFakeGateway struct {
	srv        *httptest.Server
	url        string
	identifies atomic.Int32
}

func newRootFakeGateway(t *testing.T) *rootFakeGateway {
	gw := &rootFakeGateway{}
	gw.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		ctx := context.Background()
		send := func(v interface{}) {
			b, _ := stdjson.Marshal(v)
			_ = ws.Write(ctx, websocket.MessageText, b)
		}
		send(map[string]interface{}{"op": 10, "d": map[string]interface{}{"heartbeat_interval": 45000}})
		for {
			_, data, err := ws.Read(ctx)
			if err != nil {
				return
			}
			var p struct {
				Op int `json:"op"`
				D  struct {
					Shard []uint `json:"shard"`
				} `json:"d"`
			}
			_ = stdjson.Unmarshal(data, &p)
			switch p.Op {
			case 1:
				send(map[string]interface{}{"op": 11})
			case 2:
				gw.identifies.Inc()
				send(map[string]interface{}{"op": 0, "t": "READY", "s": 1, "d": map[string]interface{}{
					"v": 10, "session_id": "sess", "resume_gateway_url": gw.url, "shard": p.D.Shard, "guilds": []interface{}{},
					"user":        map[string]interface{}{"id": "1", "username": "bot", "discriminator": "0001"},
					"application": map[string]interface{}{"id": "1", "flags": 0},
				}})
			}
		}
	}))
	gw.url = "ws://" + strings.TrimPrefix(gw.srv.URL, "http://")
	t.Cleanup(gw.srv.Close)
	return gw
}

// restStub answers GET /gateway/bot with the fake gateway's url.
type restStub struct {
	gatewayURL string
}

func (s restStub) RoundTrip(r *http.Request) (*http.Response, error) {
	if !strings.HasSuffix(r.URL.Path, "/gateway/bot") {
		return &http.Response{StatusCode: http.StatusNotFound, Body: ioutil.NopCloser(strings.NewReader(`{}`)), Request: r, Header: http.Header{}}, nil
	}
	body := fmt.Sprintf(`{"url":%q,"shards":1,"session_start_limit":{"total":1000,"remaining":1000,"reset_after":0,"max_concurrency":1}}`, s.gatewayURL)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       ioutil.NopCloser(strings.NewReader(body)),
		Request:    r,
	}, nil
}

func newGatewayTestClient(t *testing.T, gatewayURL string) *Client {
	client, err := NewClient(context.Background(), Config{
		BotToken:            "test-token",
		HTTPClient:          &http.Client{Transport: restStub{gatewayURL: gatewayURL}},
		WebsocketHttpClient: &http.Client{},
		Intents:             IntentGuilds,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// 27: handlers of the first READY (BotReady dereferences the shard manager) run while Connect is still waiting
// for READY, so the manager must be published before connecting.
func TestGatewayConnect_BotReadyCanUseShardManager(t *testing.T) {
	gw := newRootFakeGateway(t)
	client := newGatewayTestClient(t, gw.url)

	ready := make(chan struct{})
	var once sync.Once
	client.Gateway().BotReady(func() { once.Do(func() { close(ready) }) })

	if err := client.Gateway().Connect(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("BotReady was never triggered")
	}

	if err := client.Gateway().Disconnect(); err != nil {
		t.Fatal(err)
	}
	if err := client.Gateway().Disconnect(); err != nil {
		t.Fatalf("a repeated Disconnect must not fail or panic: %v", err)
	}
}

// 43: overlapping Connect calls create a single shard manager; the second call returns an error.
func TestGatewayConnect_RejectsOverlappingConnect(t *testing.T) {
	gw := newRootFakeGateway(t)
	client := newGatewayTestClient(t, gw.url)
	defer func() { _ = client.Gateway().Disconnect() }()

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { errs <- client.Gateway().Connect() }()
	}
	var failed, succeeded int
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			failed++
		} else {
			succeeded++
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("expected exactly one successful Connect, got %d successes and %d failures", succeeded, failed)
	}
	if n := gw.identifies.Load(); n != 1 {
		t.Fatalf("expected a single shard manager (1 IDENTIFY), got %d", n)
	}
}

type disconnectOnlyShardManager struct {
	gateway.ShardManager
	calls atomic.Int32
	first error
	delay time.Duration
}

func (m *disconnectOnlyShardManager) Disconnect() error {
	time.Sleep(m.delay)
	if m.calls.Inc() == 1 {
		return m.first
	}
	return nil
}

// §10.3: a Disconnect that timed out can be retried, and concurrent Disconnects share one shutdown; the shutdown
// channels are closed exactly once (no "close of closed channel").
func TestGatewayDisconnect_RetryAndConcurrent(t *testing.T) {
	t.Run("retry-after-timeout", func(t *testing.T) {
		client := newGatewayTestClient(t, "ws://127.0.0.1:1")
		client.shardManager = &disconnectOnlyShardManager{first: gateway.ErrDisconnectTimeout}

		if err := client.Gateway().Disconnect(); !errors.Is(err, gateway.ErrDisconnectTimeout) {
			t.Fatalf("expected the manager timeout to be returned, got %v", err)
		}
		select {
		case <-client.shutdownChan:
			t.Fatal("the system shutdown must wait until the shards stopped")
		default:
		}
		if err := client.Gateway().Disconnect(); err != nil {
			t.Fatalf("retry: %v", err)
		}
		select {
		case <-client.shutdownChan:
		default:
			t.Fatal("expected the system shutdown after the shards stopped")
		}
		if err := client.Gateway().Disconnect(); err != nil {
			t.Fatalf("Disconnect after a completed shutdown: %v", err)
		}
	})
	t.Run("concurrent", func(t *testing.T) {
		client := newGatewayTestClient(t, "ws://127.0.0.1:1")
		client.shardManager = &disconnectOnlyShardManager{delay: 50 * time.Millisecond}
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := client.Gateway().Disconnect(); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
	})
	t.Run("never-connected", func(t *testing.T) {
		client := newGatewayTestClient(t, "ws://127.0.0.1:1")
		if err := client.Gateway().Disconnect(); err != nil {
			t.Fatal(err)
		}
	})
}

type blockedDiscovery struct {
	started     chan struct{}
	startedOnce sync.Once
	release     chan struct{}
}

func (rt *blockedDiscovery) RoundTrip(r *http.Request) (*http.Response, error) {
	rt.startedOnce.Do(func() { close(rt.started) })
	select {
	case <-r.Context().Done():
		return nil, r.Context().Err()
	case <-rt.release:
		return nil, context.Canceled
	}
}

// Review item 8: gateway discovery (GET /gateway/bot) runs outside the client lock; Disconnect cancels a startup
// that has not published a shard manager yet, and overlapping Connects are still rejected meanwhile.
func TestGatewayDisconnectCancelsDiscovery(t *testing.T) {
	rt := &blockedDiscovery{started: make(chan struct{}), release: make(chan struct{})}
	defer close(rt.release)
	client, err := NewClient(context.Background(), Config{
		BotToken:           "test-token",
		HTTPClient:         &http.Client{Transport: rt},
		RESTRequestTimeout: -1, // the transport honours context cancellation
	})
	if err != nil {
		t.Fatal(err)
	}

	connectDone := make(chan error, 1)
	go func() { connectDone <- client.Gateway().Connect() }()
	<-rt.started

	if err := client.Gateway().Connect(); err == nil {
		t.Fatal("an overlapping Connect must be rejected while the first one discovers the gateway")
	}

	disconnectDone := make(chan error, 1)
	go func() { disconnectDone <- client.Gateway().Disconnect() }()
	select {
	case err := <-disconnectDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Disconnect blocked behind gateway discovery")
	}
	select {
	case err := <-connectDone:
		if err == nil {
			t.Fatal("Connect must fail after Disconnect cancelled the startup")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cancelled startup did not return")
	}
	if client.getShardManager() != nil {
		t.Fatal("a cancelled startup must not publish a shard manager")
	}
}
