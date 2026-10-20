//go:build !integration
// +build !integration

package disgord

import (
	"context"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

// newH2Server starts a TLS server that supports HTTP/2 and returns a transport (built by newTransport) trusting it.
func newH2Server(t *testing.T, handler http.Handler, readIdle, ping time.Duration) (*httptest.Server, *http.Transport) {
	t.Helper()
	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	transport := newTransportWithHealthCheck(nil, readIdle, ping)
	transport.TLSClientConfig.RootCAs = srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	t.Cleanup(transport.CloseIdleConnections)
	return srv, transport
}

// rewriteHost sends every request to host, eg. requests for discord.com to a test server.
type rewriteHost struct {
	base http.RoundTripper
	host string
}

func (r rewriteHost) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Host = r.host
	req.Host = ""
	return r.base.RoundTrip(req)
}

func TestTransport_RESTOverHTTP2(t *testing.T) {
	var proto int32
	srv, transport := newH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.StoreInt32(&proto, int32(r.ProtoMajor))
		if r.URL.Path != "/api/v9/users/@me" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","username":"test"}`))
	}), defaultH2ReadIdleTimeout, defaultH2PingTimeout)

	c, err := NewClient(context.Background(), Config{
		BotToken:   "test",
		HttpClient: &http.Client{Transport: rewriteHost{base: transport, host: srv.Listener.Addr().String()}},
	})
	if err != nil {
		t.Fatal(err)
	}

	usr, err := c.CurrentUser().WithFlags(IgnoreCache).Get()
	if err != nil {
		t.Fatal(err)
	}
	if usr.ID != 1 || usr.Username != "test" {
		t.Fatalf("unexpected user %+v", usr)
	}
	if p := atomic.LoadInt32(&proto); p != 2 {
		t.Fatalf("expected the request over HTTP/2, got HTTP/%d", p)
	}
}

func TestTransport_WebsocketUpgradeOverHTTP1(t *testing.T) {
	srv, transport := newH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			_, _ = w.Write([]byte("ok"))
			return
		}
		if r.ProtoMajor != 1 {
			http.Error(w, "websocket over "+r.Proto, http.StatusBadRequest)
			return
		}

		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		typ, msg, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		_ = conn.Write(r.Context(), typ, msg)
	}), defaultH2ReadIdleTimeout, defaultH2PingTimeout)
	client := &http.Client{Transport: transport}

	// make sure a HTTP/2 connection to the host is cached before dialing
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("expected HTTP/2, got %s", resp.Proto)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "wss://"+srv.Listener.Addr().String()+"/ws", &websocket.DialOptions{
		HTTPClient: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if err = conn.Write(ctx, websocket.MessageText, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	_, msg, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "hello" {
		t.Fatalf("unexpected echo %q", msg)
	}
}

// tcpRelay forwards connections to target. Blackholed connections stay open but silently drop all traffic.
type tcpRelay struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	conns []*relayConn
}

type relayConn struct {
	blackholed int32
	clientGone chan struct{}
}

func newTCPRelay(t *testing.T, target string) *tcpRelay {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &tcpRelay{ln: ln, target: target}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}

			rc := &relayConn{clientGone: make(chan struct{})}
			r.mu.Lock()
			r.conns = append(r.conns, rc)
			r.mu.Unlock()

			var once sync.Once
			closeBoth := func() {
				once.Do(func() {
					_ = client.Close()
					_ = server.Close()
				})
			}
			go func() {
				rc.pipe(server, client)
				close(rc.clientGone)
				closeBoth()
			}()
			go func() {
				rc.pipe(client, server)
				closeBoth()
			}()
		}
	}()
	return r
}

func (rc *relayConn) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if err != nil {
			return
		}
		if atomic.LoadInt32(&rc.blackholed) == 1 {
			continue
		}
		if _, err = dst.Write(buf[:n]); err != nil {
			return
		}
	}
}

func (r *tcpRelay) connections() []*relayConn {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*relayConn(nil), r.conns...)
}

// A HTTP/2 connection that silently stops delivering data must be detected and replaced, instead of hanging every
// request multiplexed over it.
func TestTransport_HTTP2HealthCheckEvictsDeadConnection(t *testing.T) {
	const readIdle, ping = 200 * time.Millisecond, 200 * time.Millisecond
	srv, transport := newH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}), readIdle, ping)
	relay := newTCPRelay(t, srv.Listener.Addr().String())
	client := &http.Client{Transport: transport}
	url := "https://" + relay.ln.Addr().String() + "/"

	get := func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.ProtoMajor != 2 {
			t.Errorf("expected HTTP/2, got %s", resp.Proto)
		}
		_, err = io.Copy(ioutil.Discard, resp.Body)
		return err
	}

	if err := get(context.Background()); err != nil {
		t.Fatal(err)
	}
	conns := relay.connections()
	if len(conns) != 1 {
		t.Fatalf("expected one connection, got %d", len(conns))
	}
	dead := conns[0]
	atomic.StoreInt32(&dead.blackholed, 1)

	// a request on the dead connection fails instead of hanging
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	inflight := make(chan error, 1)
	go func() { inflight <- get(ctx) }()
	select {
	case <-inflight:
	case <-time.After(25 * (readIdle + ping)):
		t.Fatal("request on a dead HTTP/2 connection was not aborted")
	}

	select {
	case <-dead.clientGone:
	case <-time.After(25 * (readIdle + ping)):
		t.Fatal("dead HTTP/2 connection was not closed")
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := get(ctx); err != nil {
		t.Fatalf("request after the dead connection was evicted failed: %s", err)
	}
	if n := len(relay.connections()); n < 2 {
		t.Fatalf("expected a fresh connection, got %d connections", n)
	}
}

func TestTransport_TLSNextProtos(t *testing.T) {
	transport := newTransport(nil)
	if transport.TLSClientConfig == nil || !strings.Contains(strings.Join(transport.TLSClientConfig.NextProtos, ","), "h2") {
		t.Fatal("expected HTTP/2 to be negotiated")
	}
	if _, ok := transport.TLSNextProto["h2"]; !ok {
		t.Fatal("expected golang.org/x/net/http2 to handle h2 connections")
	}
}
