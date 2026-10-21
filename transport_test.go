//go:build !integration
// +build !integration

package disgord

import (
	"context"
	"crypto/tls"
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

// initDefaultTransport makes http.DefaultTransport set up HTTP/2 like its first request would, which adds h2 to
// the TLS config newTransport clones.
func initDefaultTransport(t *testing.T) {
	t.Helper()
	dt, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Skipf("unexpected http.DefaultTransport %T", http.DefaultTransport)
	}
	dt.CloseIdleConnections()
}

// newTLSServer starts a TLS server that also supports HTTP/2 and returns a transport (built by newTransport after
// http.DefaultTransport was used) trusting it.
func newTLSServer(t *testing.T, handler http.Handler) (*httptest.Server, *http.Transport) {
	t.Helper()
	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	initDefaultTransport(t)
	transport := newTransport(nil)
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}
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

// Discord supports HTTP/2, but REST requests must still be sent over HTTP/1.1
func TestTransport_RESTOverHTTP1(t *testing.T) {
	var proto int32
	srv, transport := newTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.StoreInt32(&proto, int32(r.ProtoMajor))
		if r.URL.Path != "/api/v9/users/@me" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","username":"test"}`))
	}))

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
	if p := atomic.LoadInt32(&proto); p != 1 {
		t.Fatalf("expected the request over HTTP/1.1, got HTTP/%d", p)
	}
}

func TestTransport_WebsocketUpgradeOverHTTP1(t *testing.T) {
	srv, transport := newTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	client := &http.Client{Transport: transport}

	// make sure an idle connection to the host is pooled before dialing
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.ProtoMajor != 1 {
		t.Fatalf("expected HTTP/1.1, got %s", resp.Proto)
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
	dropped    int64 // bytes dropped while blackholed, first for 64-bit alignment
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
			atomic.AddInt64(&rc.dropped, int64(n))
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

// A connection that silently stops delivering data only holds up the request sent over it: other requests use
// other connections, and the dead connection is closed once ResponseHeaderTimeout aborts its request.
func TestTransport_DeadConnectionOnlyFailsItsRequest(t *testing.T) {
	srv, transport := newTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	transport.ResponseHeaderTimeout = 2 * time.Second
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
		if resp.ProtoMajor != 1 {
			t.Errorf("expected HTTP/1.1, got %s", resp.Proto)
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

	// the next request reuses the idle, now dead, connection
	inflight := make(chan error, 1)
	go func() { inflight <- get(context.Background()) }()
	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadInt64(&dead.dropped) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the request was not sent over the dead connection")
		}
		time.Sleep(time.Millisecond)
	}

	// while it hangs, other requests are sent over a new connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := get(ctx); err != nil {
		t.Fatalf("request next to the dead connection failed: %s", err)
	}
	if n := len(relay.connections()); n != 2 {
		t.Fatalf("expected a second connection, got %d connections", n)
	}

	select {
	case err := <-inflight:
		if err == nil || !strings.Contains(err.Error(), "timeout awaiting response headers") {
			t.Fatalf("expected the request on the dead connection to time out, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("request on the dead connection was not aborted")
	}

	select {
	case <-dead.clientGone:
	case <-time.After(10 * time.Second):
		t.Fatal("dead connection was not closed")
	}
}

func nextProtos(t *http.Transport) string {
	if t.TLSClientConfig == nil {
		return ""
	}
	return strings.Join(t.TLSClientConfig.NextProtos, ",")
}

func TestTransport_HTTP1Only(t *testing.T) {
	initDefaultTransport(t)
	defaultProtos := nextProtos(http.DefaultTransport.(*http.Transport))

	transport := newTransport(nil)
	if transport.ForceAttemptHTTP2 {
		t.Error("expected HTTP/2 not to be attempted")
	}
	if transport.TLSNextProto == nil || len(transport.TLSNextProto) != 0 {
		t.Errorf("expected a non-nil, empty TLSNextProto to disable HTTP/2, got %v", transport.TLSNextProto)
	}
	if protos := nextProtos(transport); strings.Contains(protos, "h2") {
		t.Errorf("expected h2 not to be offered during the TLS handshake, got %q", protos)
	}
	if protos := nextProtos(http.DefaultTransport.(*http.Transport)); protos != defaultProtos {
		t.Errorf("http.DefaultTransport must not be modified, its protocols changed from %q to %q", defaultProtos, protos)
	}
	if transport.MaxIdleConnsPerHost != defaultMaxIdleConnsPerHost {
		t.Errorf("MaxIdleConnsPerHost: got %d", transport.MaxIdleConnsPerHost)
	}
}
