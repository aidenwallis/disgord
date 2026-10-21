//go:build !integration
// +build !integration

package disgord

import (
	"context"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aidenwallis/disgord/internal/httd"
	"nhooyr.io/websocket"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

func currentUserResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       ioutil.NopCloser(strings.NewReader(`{"id":"1","username":"test"}`)),
	}
}

func getCurrentUser(ctx context.Context, c *Client) error {
	_, err := c.CurrentUser().WithContext(ctx).WithFlags(IgnoreCache).Get()
	return err
}

func TestClient_RESTHttpClientPrecedence(t *testing.T) {
	var modern, deprecated int32
	modernClient := doerFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&modern, 1)
		return currentUserResponse(), nil
	})
	deprecatedClient := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&deprecated, 1)
		return currentUserResponse(), nil
	})}

	t.Run("HttpClient wins", func(t *testing.T) {
		atomic.StoreInt32(&modern, 0)
		atomic.StoreInt32(&deprecated, 0)
		c, err := NewClient(context.Background(), Config{
			BotToken:   "test",
			HttpClient: modernClient,
			HTTPClient: deprecatedClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = getCurrentUser(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		if m, d := atomic.LoadInt32(&modern), atomic.LoadInt32(&deprecated); m != 1 || d != 0 {
			t.Fatalf("expected the request on HttpClient, got HttpClient=%d HTTPClient=%d", m, d)
		}
	})

	t.Run("HTTPClient is honoured", func(t *testing.T) {
		atomic.StoreInt32(&modern, 0)
		atomic.StoreInt32(&deprecated, 0)
		c, err := NewClient(context.Background(), Config{
			BotToken:   "test",
			HTTPClient: deprecatedClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = getCurrentUser(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		if d := atomic.LoadInt32(&deprecated); d != 1 {
			t.Fatalf("expected the request on HTTPClient, got %d", d)
		}
	})
}

func TestClient_HTTPClientTimeoutIsAllowed(t *testing.T) {
	c, err := NewClient(context.Background(), Config{
		BotToken:   "test",
		HTTPClient: &http.Client{Timeout: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.WebsocketHttpClient != DefaultHttpClient {
		t.Error("websocket should use the default client")
	}
}

func TestClient_RESTRequestTimeout(t *testing.T) {
	table := []struct {
		name       string
		configured time.Duration
		min, max   time.Duration // 0 => no deadline
	}{
		{"default", 0, httd.DefaultRequestTimeout - time.Minute, httd.DefaultRequestTimeout},
		{"custom", 30 * time.Second, 20 * time.Second, 30 * time.Second},
		{"disabled", -1, 0, 0},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			var deadline time.Time
			var ok bool
			c, err := NewClient(context.Background(), Config{
				BotToken: "test",
				HttpClient: doerFunc(func(req *http.Request) (*http.Response, error) {
					deadline, ok = req.Context().Deadline()
					return currentUserResponse(), nil
				}),
				RESTRequestTimeout: tt.configured,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = getCurrentUser(context.Background(), c); err != nil {
				t.Fatal(err)
			}

			if tt.max == 0 {
				if ok {
					t.Fatalf("expected no deadline, got %s", time.Until(deadline))
				}
				return
			}
			if left := time.Until(deadline); !ok || left < tt.min || left > tt.max {
				t.Fatalf("expected a deadline in [%s, %s], got %t %s", tt.min, tt.max, ok, left)
			}
		})
	}
}

func TestDefaultHttpClient_Transport(t *testing.T) {
	transport, ok := DefaultHttpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected transport %T", DefaultHttpClient.Transport)
	}
	if transport == http.DefaultTransport {
		t.Fatal("http.DefaultTransport must not be modified")
	}
	if transport.ResponseHeaderTimeout != defaultResponseHeaderTimeout {
		t.Errorf("ResponseHeaderTimeout: got %s", transport.ResponseHeaderTimeout)
	}
	if transport.TLSHandshakeTimeout != 10*time.Second || transport.IdleConnTimeout != 90*time.Second {
		t.Errorf("expected the http.DefaultTransport timeouts, got TLS %s idle %s", transport.TLSHandshakeTimeout, transport.IdleConnTimeout)
	}
	if transport.DialContext == nil || transport.Proxy == nil {
		t.Error("expected the http.DefaultTransport dialer and environment proxy")
	}
}

// the websocket handshake goes through the transport, so it is bounded by ResponseHeaderTimeout
func TestTransport_WebsocketUpgradeIsBounded(t *testing.T) {
	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-unblock:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(unblock)

	transport := newTransport(nil)
	transport.ResponseHeaderTimeout = 200 * time.Millisecond
	client := &http.Client{Transport: transport}

	errs := make(chan error, 1)
	go func() {
		_, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http"), &websocket.DialOptions{
			HTTPClient: client,
		})
		errs <- err
	}()
	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("expected the dial to fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("websocket dial was not bounded by the transport")
	}
}

var errTestProxy = errors.New("test proxy refused the connection")

type proxyCtxKey struct{}

type testDialer struct {
	mu    sync.Mutex
	addrs []string
}

func (d *testDialer) Dial(network, addr string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.addrs = append(d.addrs, addr)
	return nil, errTestProxy
}

func (d *testDialer) dialed() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.addrs...)
}

type testContextDialer struct {
	testDialer // Dial must not be used

	ctxMu    sync.Mutex
	ctxAddrs []string
	values   []interface{}
}

func (d *testContextDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	d.ctxMu.Lock()
	defer d.ctxMu.Unlock()
	d.ctxAddrs = append(d.ctxAddrs, addr)
	d.values = append(d.values, ctx.Value(proxyCtxKey{}))
	return nil, errTestProxy
}

// dialThroughDefaults sends a REST request and dials the gateway using the clients of c. Both must fail.
func dialThroughDefaults(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), proxyCtxKey{}, "rest"), 10*time.Second)
	defer cancel()
	if err := getCurrentUser(ctx, c); err == nil || !strings.Contains(err.Error(), errTestProxy.Error()) {
		t.Errorf("expected the REST request to go through the proxy, got %v", err)
	}

	ctx, cancel = context.WithTimeout(context.WithValue(context.Background(), proxyCtxKey{}, "gateway"), 10*time.Second)
	defer cancel()
	_, _, err := websocket.Dial(ctx, "wss://gateway.discord.gg/?v=9&encoding=json", &websocket.DialOptions{
		HTTPClient: c.WebsocketHttpClient,
	})
	if err == nil || !strings.Contains(err.Error(), errTestProxy.Error()) {
		t.Errorf("expected the websocket dial to go through the proxy, got %v", err)
	}
}

func TestClient_Proxy(t *testing.T) {
	defaultTransport := DefaultHttpClient.Transport
	defer func() {
		DefaultHttpClient.Transport = defaultTransport
	}()
	checkDefaults := func(t *testing.T, c *Client) {
		t.Helper()
		if DefaultHttpClient.Transport != defaultTransport {
			t.Error("DefaultHttpClient was modified")
		}
		if c.config.HttpClient == HttpClientDoer(DefaultHttpClient) || c.WebsocketHttpClient == DefaultHttpClient {
			t.Error("expected dedicated clients when a proxy is configured")
		}
	}
	wantAddrs := []string{"discord.com:443", "gateway.discord.gg:443"}
	checkAddrs := func(t *testing.T, got []string) {
		t.Helper()
		if strings.Join(got, ",") != strings.Join(wantAddrs, ",") {
			t.Errorf("proxy dialed %v, wants %v", got, wantAddrs)
		}
	}

	t.Run("Dialer", func(t *testing.T) {
		dialer := &testDialer{}
		c, err := NewClient(context.Background(), Config{BotToken: "test", Proxy: dialer})
		if err != nil {
			t.Fatal(err)
		}
		checkDefaults(t, c)
		dialThroughDefaults(t, c)
		checkAddrs(t, dialer.dialed())
	})

	t.Run("ContextDialer", func(t *testing.T) {
		dialer := &testContextDialer{}
		c, err := NewClient(context.Background(), Config{BotToken: "test", Proxy: dialer})
		if err != nil {
			t.Fatal(err)
		}
		checkDefaults(t, c)
		dialThroughDefaults(t, c)

		dialer.ctxMu.Lock()
		defer dialer.ctxMu.Unlock()
		checkAddrs(t, dialer.ctxAddrs)
		if len(dialer.values) != 2 || dialer.values[0] != "rest" || dialer.values[1] != "gateway" {
			t.Errorf("DialContext did not receive the request context: %v", dialer.values)
		}
		if legacy := dialer.testDialer.dialed(); len(legacy) > 0 {
			t.Errorf("Dial should not be used when DialContext is available: %v", legacy)
		}
	})

	t.Run("explicit clients win", func(t *testing.T) {
		dialer := &testDialer{}
		var rest int32
		ws := &http.Client{}
		c, err := NewClient(context.Background(), Config{
			BotToken: "test",
			Proxy:    dialer,
			HttpClient: doerFunc(func(req *http.Request) (*http.Response, error) {
				atomic.AddInt32(&rest, 1)
				return currentUserResponse(), nil
			}),
			WebsocketHttpClient: ws,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = getCurrentUser(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		if atomic.LoadInt32(&rest) != 1 {
			t.Error("expected the REST request on HttpClient")
		}
		if c.WebsocketHttpClient != ws {
			t.Error("expected the configured websocket client")
		}
		if addrs := dialer.dialed(); len(addrs) > 0 {
			t.Errorf("proxy should not be used, dialed %v", addrs)
		}
	})
}
