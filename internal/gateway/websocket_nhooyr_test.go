//go:build !integration
// +build !integration

package gateway

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"
	"nhooyr.io/websocket"
)

// partialFramePeer completes a websocket upgrade by hand and, once told to, sends the header of a 100 byte binary
// frame followed by only 3 payload bytes, then stalls without closing the TCP connection.
type partialFramePeer struct {
	srv         *httptest.Server
	url         string
	sendPartial chan struct{}
	release     chan struct{}
	conns       chan net.Conn
}

func newPartialFramePeer(t *testing.T) *partialFramePeer {
	p := &partialFramePeer{sendPartial: make(chan struct{}), release: make(chan struct{}), conns: make(chan net.Conn, 1)}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		key := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		_, _ = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
			base64.StdEncoding.EncodeToString(key[:]))
		_ = rw.Flush()
		p.conns <- conn
		<-p.sendPartial
		_, _ = conn.Write([]byte{0x82, 100, 1, 2, 3})
		<-p.release
		_ = conn.Close()
	}))
	p.url = "ws" + strings.TrimPrefix(p.srv.URL, "http")
	t.Cleanup(func() {
		close(p.release)
		p.srv.Close()
	})
	return p
}

func openSessionConn(t *testing.T, url string, closeTimeout time.Duration) sessionConn {
	t.Helper()
	conn := newSessionConn(nil, time.Second, closeTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Open(ctx, url, nil); err != nil {
		t.Fatal(err)
	}
	return conn
}

// Review item 1: nhooyr's close handshake discards a partially received frame without a deadline; the wrapper
// bounds it and force-closes the connection.
func TestSessionConnCloseBoundedOnPartialFrame(t *testing.T) {
	peer := newPartialFramePeer(t)
	const closeTimeout = 300 * time.Millisecond
	conn := openSessionConn(t, peer.url, closeTimeout)
	<-peer.conns

	closeDone := make(chan error, 1)
	go func() { closeDone <- conn.CloseWithStatus(4000, "reconnecting") }()
	close(peer.sendPartial)
	select {
	case err := <-closeDone:
		if !errors.Is(err, errCloseTimedOut) {
			t.Fatalf("expected the close to time out and force-close, got %v", err)
		}
	case <-time.After(10 * closeTimeout):
		t.Fatal("CloseWithStatus is not bounded while discarding a partial frame")
	}
	if _, err := conn.Read(context.Background()); err == nil {
		t.Fatal("the connection must be closed after a forced close")
	}
}

// gatedReadConn holds the session's receiver before it enters Read until the close handshake finished, so the
// close handshake wins nhooyr's read lock (a receiver still busy with a dispatch when teardown starts).
type gatedReadConn struct {
	sessionConn
	enteredRead  chan struct{}
	enterOnce    sync.Once
	readGate     chan struct{}
	closeStarted chan struct{}
	closeOnce    sync.Once
}

func newGatedReadConn(conn sessionConn) *gatedReadConn {
	return &gatedReadConn{sessionConn: conn, enteredRead: make(chan struct{}), readGate: make(chan struct{}), closeStarted: make(chan struct{})}
}

func (c *gatedReadConn) Read(ctx context.Context) ([]byte, error) {
	c.enterOnce.Do(func() { close(c.enteredRead) })
	<-c.readGate
	return c.sessionConn.Read(ctx)
}

func (c *gatedReadConn) CloseWithStatus(code int, reason string) error {
	c.closeOnce.Do(func() { close(c.closeStarted) })
	err := c.sessionConn.CloseWithStatus(code, reason)
	select {
	case <-c.readGate:
	default:
		close(c.readGate)
	}
	return err
}

// Review item 1: teardown completes within the close bound although the peer stalls inside a frame.
func TestTeardownBoundedOnStalledPartialFrame(t *testing.T) {
	peer := newPartialFramePeer(t)
	to := fastTimeouts()
	to.closeHandshake = 300 * time.Millisecond
	ss := newScriptShard(t, to, nil, nil)

	conn := newGatedReadConn(openSessionConn(t, peer.url, to.closeHandshake))
	<-peer.conns
	go func() {
		<-conn.closeStarted
		close(peer.sendPartial)
	}()

	s := ss.c.newSession(nil)
	s.conn = conn
	s.alive.receiver.Store(true)
	s.wgRecv.Add(1)
	go s.receive()
	<-conn.enteredRead
	s.fail(failReason{kind: failTransport, err: errors.New("local failure")})

	done := make(chan failReason, 1)
	go func() { done <- ss.c.teardown(s, 4000) }()
	select {
	case <-done:
	case <-time.After(10 * (to.closeHandshake + time.Second)):
		t.Fatal("teardown did not complete while the peer stalled inside a frame")
	}
	if workers := s.aliveWorkers(); workers != "" {
		t.Fatalf("workers still alive after teardown: %s", workers)
	}
}

// Known limitation (review Astra#2, not fixed): when nhooyr's Close wins the read lock it consumes the peer's
// close frame and the code is lost. The next connection reads the fatal code normally and the shard stops.
func TestFatalCloseLostDuringCloseStopsOnNextAttempt(t *testing.T) {
	var accepted atomic.Int32
	var closeStarted chan struct{}
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		ctx := context.Background()
		if accepted.Inc() == 1 {
			// the client's receiver is gated, its HELLO wait times out; answer its close with a fatal code
			mu.Lock()
			started := closeStarted
			mu.Unlock()
			<-started
			_ = ws.Close(websocket.StatusCode(4004), "Authentication failed.")
			return
		}
		_ = ws.Write(ctx, websocket.MessageText, []byte(`{"op":10,"d":{"heartbeat_interval":45000}}`))
		for {
			_, data, err := ws.Read(ctx)
			if err != nil {
				return
			}
			if strings.Contains(string(data), `"op":2`) {
				_ = ws.Close(websocket.StatusCode(4004), "Authentication failed.")
				return
			}
		}
	}))
	defer server.Close()
	url := "ws" + strings.TrimPrefix(server.URL, "http")

	to := fastTimeouts()
	to.hello = 200 * time.Millisecond
	var dials atomic.Int32
	newConn := func() (sessionConn, error) {
		conn := newSessionConn(nil, time.Second, time.Second)
		if dials.Inc() == 1 {
			gated := newGatedReadConn(conn)
			mu.Lock()
			closeStarted = gated.closeStarted
			mu.Unlock()
			return gated, nil
		}
		return conn, nil
	}
	shutdown := make(chan interface{})
	defer close(shutdown)
	c, err := NewEventClient(0, &EvtConfig{
		BotToken: "test-token", Endpoint: url, ShardCount: 1, Logger: newTestLogger(t.Name()),
		EventChan: make(chan *Event, 10), SystemShutdown: shutdown, newConn: newConn, timeouts: to,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Disconnect() }()

	err = c.Connect()
	var closeErr *GatewayCloseError
	if !errors.As(err, &closeErr) || closeErr.Code != 4004 {
		t.Fatalf("expected the next attempt to stop the shard with 4004, got %v", err)
	}
	if n := dials.Load(); n > 2 {
		t.Fatalf("expected at most one extra attempt, got %d dials", n)
	}
}
