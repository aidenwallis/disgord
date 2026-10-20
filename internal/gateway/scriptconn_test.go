//go:build !integration
// +build !integration

package gateway

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"
)

// scriptConn is an in-memory sessionConn whose behaviour is driven by the test, for schedules that a real socket
// can not produce deterministically.
type scriptConn struct {
	id int

	inbox   chan []byte // frames delivered to Read
	readErr chan error  // error returned by the next Read

	// onWrite runs inside WriteJSON (on the session's writer goroutine); a non-nil error fails the write.
	onWrite func(sc *scriptConn, p *clientPacket) error

	// holdErrUntilClose delays returning an injected read error until we sent our close frame, so the reader
	// observes it only after teardown started.
	holdErrUntilClose bool
	// readDelayAfterClose keeps the reader busy for a while after the close (delayed reader).
	readDelayAfterClose time.Duration
	// openBlock, when set, makes Open ignore its ctx and block until closed (legacy proxy dial).
	openBlock chan struct{}
	openCalls atomic.Int32

	mu        sync.Mutex
	written   []*clientPacket
	closeCode int
	closed    chan struct{}
	closeOnce sync.Once
	openErr   error
	opened    atomic.Bool
}

func newScriptConn(id int) *scriptConn {
	return &scriptConn{
		id:      id,
		inbox:   make(chan []byte, 1000),
		readErr: make(chan error, 10),
		closed:  make(chan struct{}),
	}
}

func (sc *scriptConn) Open(ctx context.Context, endpoint string, requestHeader http.Header) error {
	sc.openCalls.Inc()
	if sc.openBlock != nil {
		<-sc.openBlock
	}
	if sc.openErr != nil {
		return sc.openErr
	}
	sc.opened.Store(true)
	return nil
}

func (sc *scriptConn) WriteJSON(v interface{}) error {
	p := v.(*clientPacket)
	select {
	case <-sc.closed:
		return errors.New("write on closed connection")
	default:
	}
	if sc.onWrite != nil {
		if err := sc.onWrite(sc, p); err != nil {
			return err
		}
	}
	sc.mu.Lock()
	sc.written = append(sc.written, p)
	sc.mu.Unlock()
	return nil
}

func (sc *scriptConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case b := <-sc.inbox:
		return b, nil
	case err := <-sc.readErr:
		if sc.holdErrUntilClose {
			<-sc.closed
		}
		return nil, err
	case <-sc.closed:
		if sc.readDelayAfterClose > 0 {
			time.Sleep(sc.readDelayAfterClose)
		}
		sc.mu.Lock()
		code := sc.closeCode
		sc.mu.Unlock()
		// like nhooyr: after we sent a close frame, reads report our own close code
		return nil, &CloseErr{code: code, info: fmt.Sprintf("sent close frame: %d", code)}
	case <-ctx.Done():
		return nil, context.Canceled
	}
}

func (sc *scriptConn) Close() error {
	return sc.CloseWithStatus(1000, "")
}

func (sc *scriptConn) CloseWithStatus(code int, reason string) error {
	sc.closeOnce.Do(func() {
		sc.mu.Lock()
		sc.closeCode = code
		sc.mu.Unlock()
		close(sc.closed)
	})
	return nil
}

func (sc *scriptConn) Disconnected() bool {
	select {
	case <-sc.closed:
		return true
	default:
		return false
	}
}

func (sc *scriptConn) sentCloseCode() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.closeCode
}

func (sc *scriptConn) writes() []*clientPacket {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return append([]*clientPacket(nil), sc.written...)
}

func (sc *scriptConn) countOp(op int) int {
	var n int
	for _, p := range sc.writes() {
		if int(p.Op) == op {
			n++
		}
	}
	return n
}

func (sc *scriptConn) send(v interface{}) {
	b, err := stdjson.Marshal(v)
	if err != nil {
		panic(err)
	}
	sc.inbox <- b
}

func (sc *scriptConn) hello(intervalMs int) {
	sc.send(map[string]interface{}{"op": 10, "d": map[string]interface{}{"heartbeat_interval": intervalMs}})
}

func (sc *scriptConn) ready(sessionID string) {
	sc.send(map[string]interface{}{"op": 0, "t": "READY", "s": 1, "d": map[string]interface{}{
		"session_id": sessionID, "resume_gateway_url": "ws://resume.invalid"}})
}

func (sc *scriptConn) dispatch(seq int, name string, data string) {
	sc.inbox <- []byte(fmt.Sprintf(`{"op":0,"t":%q,"s":%d,"d":%s}`, name, seq, data))
}

func (sc *scriptConn) remoteClose(code int) {
	sc.readErr <- &CloseErr{code: code, info: fmt.Sprintf("received close frame: %d", code)}
}

// scriptDialer hands out a new scriptConn per connection attempt; setup customizes each before use.
type scriptDialer struct {
	mu    sync.Mutex
	conns []*scriptConn
	setup func(sc *scriptConn)
	next  chan *scriptConn
}

func newScriptDialer(setup func(sc *scriptConn)) *scriptDialer {
	return &scriptDialer{setup: setup, next: make(chan *scriptConn, 100)}
}

func (d *scriptDialer) newConn() (sessionConn, error) {
	d.mu.Lock()
	sc := newScriptConn(len(d.conns))
	d.conns = append(d.conns, sc)
	d.mu.Unlock()
	if d.setup != nil {
		d.setup(sc)
	}
	d.next <- sc
	return sc, nil
}

func (d *scriptDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.conns)
}

func (d *scriptDialer) await(t *testing.T) *scriptConn {
	t.Helper()
	select {
	case sc := <-d.next:
		return sc
	case <-time.After(20 * time.Second):
		t.Fatal("no new connection attempt")
		return nil
	}
}

type scriptShard struct {
	c        *EvtClient
	d        *scriptDialer
	events   chan *Event
	shutdown chan interface{}
	log      *testLogger
}

func newScriptShard(t *testing.T, to *timeouts, setup func(sc *scriptConn), queue connectQueue) *scriptShard {
	if to == nil {
		to = fastTimeouts()
	}
	log := newTestLogger(t.Name())
	ss := &scriptShard{
		d:        newScriptDialer(setup),
		events:   make(chan *Event, 1000),
		shutdown: make(chan interface{}),
		log:      log,
	}
	c, err := NewEventClient(0, &EvtConfig{
		BotToken:       "test-token",
		Endpoint:       "ws://script.invalid/?v=10&encoding=json",
		ShardCount:     1,
		Logger:         log,
		EventChan:      ss.events,
		SystemShutdown: ss.shutdown,
		connectQueue:   queue,
		newConn:        ss.d.newConn,
		timeouts:       to,
	})
	if err != nil {
		t.Fatal(err)
	}
	ss.c = c
	t.Cleanup(func() {
		_ = c.Disconnect()
		close(ss.shutdown)
		if t.Failed() {
			t.Logf("log tail:\n%s", log.Tail(60))
		}
	})
	return ss
}

// connectAsync starts Connect and returns its result channel.
func (ss *scriptShard) connectAsync() chan error {
	ch := make(chan error, 1)
	go func() { ch <- ss.c.Connect() }()
	return ch
}

func waitOp(t *testing.T, sc *scriptConn, op int, n int) {
	t.Helper()
	if !waitFor(20*time.Second, func() bool { return sc.countOp(op) >= n }) {
		t.Fatalf("conn %d: expected %d frames with op %d, got %d", sc.id, n, op, sc.countOp(op))
	}
}

func awaitErr(t *testing.T, ch chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("Connect did not return")
		return nil
	}
}
