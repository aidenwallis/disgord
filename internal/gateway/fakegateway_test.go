//go:build !integration
// +build !integration

package gateway

// Fake Discord gateway built on httptest + nhooyr (server side). It drives the real EvtClient over the real
// nhooyr transport (websocket_nhooyr.go). Ported from the reproduction harness used to confirm the original
// "connected but dead" shard bugs.

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"
	"nhooyr.io/websocket"

	"github.com/aidenwallis/disgord/internal/gateway/cmd"
)

// ---------------------------------------------------------------------------
// logger
// ---------------------------------------------------------------------------

type testLogger struct {
	mu    sync.Mutex
	start time.Time
	buf   bytes.Buffer
	echo  bool
	name  string
}

func newTestLogger(name string) *testLogger {
	return &testLogger{start: time.Now(), echo: os.Getenv("GATEWAY_TEST_VERBOSE") != "", name: name}
}

func (l *testLogger) write(level string, v ...interface{}) {
	line := fmt.Sprintf("%9.3fs %-5s %s", time.Since(l.start).Seconds(), level, fmt.Sprintln(v...))
	l.mu.Lock()
	l.buf.WriteString(line)
	l.mu.Unlock()
	if l.echo {
		fmt.Fprint(os.Stderr, "["+l.name+"] "+line)
	}
}
func (l *testLogger) Debug(v ...interface{}) { l.write("DEBUG", v...) }
func (l *testLogger) Info(v ...interface{})  { l.write("INFO", v...) }
func (l *testLogger) Error(v ...interface{}) { l.write("ERROR", v...) }
func (l *testLogger) Server(format string, args ...interface{}) {
	l.write("SRV", fmt.Sprintf(format, args...))
}

func (l *testLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *testLogger) Count(substr string) int {
	return strings.Count(l.String(), substr)
}

func (l *testLogger) Tail(n int) string {
	lines := strings.Split(strings.TrimRight(l.String(), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// ---------------------------------------------------------------------------
// tracking listener (gives us the raw net.Conn so we can kill TCP without a close frame)
// ---------------------------------------------------------------------------

type trackingListener struct {
	net.Listener
	mu      sync.Mutex
	conns   map[string]net.Conn
	readBuf atomic.Int32 // if >0, SetReadBuffer on accepted conns
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if n := l.readBuf.Load(); n > 0 {
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetReadBuffer(int(n))
		}
	}
	l.mu.Lock()
	l.conns[c.RemoteAddr().String()] = c
	l.mu.Unlock()
	return c, nil
}

func (l *trackingListener) get(remote string) net.Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conns[remote]
}

// ---------------------------------------------------------------------------
// fake gateway
// ---------------------------------------------------------------------------

type fakeEvent struct {
	seq   uint32
	frame []byte
}

type fakeSession struct {
	id     string
	shard  [2]uint
	seq    uint32
	valid  bool
	events []fakeEvent // dispatched events, replayed on RESUME
}

type dialRecord struct {
	path  string
	query string
}

type fakeGateway struct {
	t   *testing.T
	log *testLogger
	srv *httptest.Server
	lis *trackingListener
	URL string

	hbIntervalMs atomic.Int64
	helloDelayMs atomic.Int64
	replayGapMs  atomic.Int64 // delay between replayed events on RESUME

	mu          sync.Mutex
	conns       []*fakeConn
	dials       []dialRecord
	sessions    map[string]*fakeSession
	sessCounter int
	nonces      map[string]chan struct{}
	resumeURL   string // resume_gateway_url sent in READY; defaults to URL

	dialAttempts     atomic.Int32
	failDials        atomic.Int32 // respond HTTP 500 to the next N upgrade attempts
	resetDials       atomic.Int32 // accept TCP + read request, then RST (no HTTP response)
	hangDials        atomic.Int32 // read request and never respond (until gateway closes)
	dropAfterWS      atomic.Int32 // complete WS upgrade, then RST before sending Hello
	closeAfterWS     atomic.Int32 // complete WS upgrade, then close frame (code closeAfterWSCode) before Hello
	closeAfterWSCode atomic.Int32
	dropResumePath   atomic.Int32 // like dropAfterWS, but only for dials to /resume
	identifyClose    atomic.Int32 // if >0, answer IDENTIFY with this close code
	noAckNew         atomic.Bool  // new connections never ACK heartbeats
	enforceAuth      bool         // like Discord: non-heartbeat command before identify/resume -> close 4003

	closing chan struct{}
	newConn chan *fakeConn
}

type frameRecord struct {
	at time.Time
	op int
	d  stdjson.RawMessage
}

type fakeConn struct {
	gw  *fakeGateway
	idx int
	ws  *websocket.Conn
	raw net.Conn

	path  string
	query string

	writeMu sync.Mutex
	sess    *fakeSession // guarded by gw.mu
	shardID atomic.Int32

	identifies     atomic.Int32
	resumes        atomic.Int32
	heartbeats     atomic.Int32
	hbAfterReady   atomic.Int32
	commands       atomic.Int32
	cmdBeforeReady atomic.Int32
	authed         atomic.Bool
	noAck          atomic.Bool
	frozen         atomic.Bool
	clientClose    atomic.Int32 // close code sent by the client (0 = none/unknown)

	framesMu sync.Mutex
	frames   []frameRecord

	readyOnce sync.Once
	ready     chan struct{}
	readyAt   time.Time
	closedCh  chan struct{}
	closeOnce sync.Once
	closeErr  error
	openedAt  time.Time
}

func newFakeGateway(t *testing.T, log *testLogger) *fakeGateway {
	gw := &fakeGateway{
		t:        t,
		log:      log,
		sessions: map[string]*fakeSession{},
		nonces:   map[string]chan struct{}{},
		closing:  make(chan struct{}),
		newConn:  make(chan *fakeConn, 1000),
	}
	gw.hbIntervalMs.Store(500)
	gw.closeAfterWSCode.Store(4000)
	gw.srv = httptest.NewUnstartedServer(http.HandlerFunc(gw.handle))
	gw.lis = &trackingListener{Listener: gw.srv.Listener, conns: map[string]net.Conn{}}
	gw.srv.Listener = gw.lis
	gw.srv.Start()
	gw.URL = "ws://" + strings.TrimPrefix(gw.srv.URL, "http://")
	gw.resumeURL = gw.URL
	t.Cleanup(gw.close)
	return gw
}

func (gw *fakeGateway) close() {
	select {
	case <-gw.closing:
		return
	default:
	}
	close(gw.closing)
	gw.mu.Lock()
	conns := append([]*fakeConn(nil), gw.conns...)
	gw.mu.Unlock()
	for _, fc := range conns {
		_ = fc.raw.Close()
	}
	gw.lis.mu.Lock()
	for _, c := range gw.lis.conns {
		_ = c.Close()
	}
	gw.lis.mu.Unlock()
	done := make(chan struct{})
	go func() { gw.srv.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
}

func takeOne(a *atomic.Int32) bool {
	for {
		v := a.Load()
		if v <= 0 {
			return false
		}
		if a.CAS(v, v-1) {
			return true
		}
	}
}

func (gw *fakeGateway) handle(w http.ResponseWriter, r *http.Request) {
	n := gw.dialAttempts.Inc()
	gw.mu.Lock()
	gw.dials = append(gw.dials, dialRecord{path: r.URL.Path, query: r.URL.RawQuery})
	gw.mu.Unlock()
	raw := gw.lis.get(r.RemoteAddr)
	switch {
	case takeOne(&gw.failDials):
		gw.log.Server("dial #%d from %s -> HTTP 500 (injected)", n, r.RemoteAddr)
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	case takeOne(&gw.resetDials):
		gw.log.Server("dial #%d from %s -> TCP RST before HTTP response (injected)", n, r.RemoteAddr)
		if tc, ok := raw.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = raw.Close()
		return
	case takeOne(&gw.hangDials):
		gw.log.Server("dial #%d from %s -> hanging, never responding to upgrade (injected)", n, r.RemoteAddr)
		select {
		case <-gw.closing:
		case <-r.Context().Done():
		}
		_ = raw.Close()
		return
	}

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		gw.log.Server("accept error: %v", err)
		return
	}
	ws.SetReadLimit(64 << 20)

	fc := &fakeConn{
		gw:       gw,
		ws:       ws,
		raw:      raw,
		path:     r.URL.Path,
		query:    r.URL.RawQuery,
		ready:    make(chan struct{}),
		closedCh: make(chan struct{}),
		openedAt: time.Now(),
	}
	fc.shardID.Store(-1)
	fc.noAck.Store(gw.noAckNew.Load())
	gw.mu.Lock()
	fc.idx = len(gw.conns)
	gw.conns = append(gw.conns, fc)
	gw.mu.Unlock()
	gw.log.Server("conn #%d accepted (dial #%d) from %s path=%q", fc.idx, n, r.RemoteAddr, r.URL.Path)
	gw.newConn <- fc

	if takeOne(&gw.dropAfterWS) || (r.URL.Path == "/resume" && takeOne(&gw.dropResumePath)) {
		gw.log.Server("conn #%d -> TCP RST right after WS upgrade, before Hello (injected)", fc.idx)
		fc.abort(true)
		fc.markClosed(fmt.Errorf("injected RST before Hello"))
		return
	}
	if takeOne(&gw.closeAfterWS) {
		code := gw.closeAfterWSCode.Load()
		gw.log.Server("conn #%d -> close frame %d right after WS upgrade, before Hello (injected)", fc.idx, code)
		fc.closeWith(int(code), "injected")
		fc.readLoop() // let nhooyr process the close handshake
		return
	}

	if d := gw.helloDelayMs.Load(); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond) // simulated network RTT before Hello
	}
	_ = fc.send(map[string]interface{}{"op": 10, "d": map[string]interface{}{"heartbeat_interval": gw.hbIntervalMs.Load()}})
	fc.readLoop()
}

type clientFrame struct {
	Op int                `json:"op"`
	D  stdjson.RawMessage `json:"d"`
}

func (fc *fakeConn) markClosed(err error) {
	fc.closeOnce.Do(func() {
		fc.closeErr = err
		if code := websocket.CloseStatus(err); code != -1 {
			fc.clientClose.Store(int32(code))
		}
		close(fc.closedCh)
		fc.gw.log.Server("conn #%d closed: %v", fc.idx, err)
	})
}

func (fc *fakeConn) isClosed() bool {
	select {
	case <-fc.closedCh:
		return true
	default:
		return false
	}
}

func (fc *fakeConn) isReady() bool {
	select {
	case <-fc.ready:
		return true
	default:
		return false
	}
}

func (fc *fakeConn) markReady() {
	fc.readyOnce.Do(func() {
		fc.readyAt = time.Now()
		close(fc.ready)
	})
}

func (fc *fakeConn) record(p clientFrame) {
	fc.framesMu.Lock()
	fc.frames = append(fc.frames, frameRecord{at: time.Now(), op: p.Op, d: p.D})
	fc.framesMu.Unlock()
}

func (fc *fakeConn) frameLog() []frameRecord {
	fc.framesMu.Lock()
	defer fc.framesMu.Unlock()
	return append([]frameRecord(nil), fc.frames...)
}

func (fc *fakeConn) readLoop() {
	gw := fc.gw
	for {
		_, data, err := fc.ws.Read(context.Background())
		if err != nil {
			fc.markClosed(err)
			return
		}
		if fc.frozen.Load() {
			// half-open simulation: stop processing AND stop reading entirely
			gw.log.Server("conn #%d frozen: dropping frame and no longer reading", fc.idx)
			select {
			case <-gw.closing:
			case <-fc.closedCh:
			}
			return
		}
		var p clientFrame
		if err := stdjson.Unmarshal(data, &p); err != nil {
			gw.log.Server("conn #%d bad frame: %v", fc.idx, err)
			continue
		}
		fc.record(p)
		switch p.Op {
		case 1: // heartbeat
			fc.heartbeats.Inc()
			if fc.isReady() {
				fc.hbAfterReady.Inc()
			}
			if !fc.noAck.Load() {
				_ = fc.send(map[string]interface{}{"op": 11})
			}
		case 2: // identify
			fc.identifies.Inc()
			if code := gw.identifyClose.Load(); code > 0 {
				gw.log.Server("conn #%d IDENTIFY -> close %d (injected)", fc.idx, code)
				fc.closeWith(int(code), "injected")
				continue
			}
			var id struct {
				Shard []uint `json:"shard"`
			}
			_ = stdjson.Unmarshal(p.D, &id)
			gw.mu.Lock()
			gw.sessCounter++
			s := &fakeSession{id: fmt.Sprintf("sess-%d", gw.sessCounter), valid: true}
			if len(id.Shard) == 2 {
				s.shard = [2]uint{id.Shard[0], id.Shard[1]}
			}
			gw.sessions[s.id] = s
			fc.sess = s
			s.seq = 1
			resumeURL := gw.resumeURL
			gw.mu.Unlock()
			fc.shardID.Store(int32(s.shard[0]))
			fc.authed.Store(true)
			gw.log.Server("conn #%d IDENTIFY shard=%v -> READY session=%s", fc.idx, id.Shard, s.id)
			fc.markReady()
			_ = fc.send(map[string]interface{}{"op": 0, "t": "READY", "s": 1, "d": map[string]interface{}{
				"v": 10, "session_id": s.id, "resume_gateway_url": resumeURL,
				"user": map[string]interface{}{"id": "1", "username": "bot"}, "guilds": []interface{}{},
				"shard": id.Shard, "application": map[string]interface{}{"id": "1"},
			}})
		case 6: // resume
			fc.resumes.Inc()
			var rs struct {
				SessionID string `json:"session_id"`
				Seq       uint32 `json:"seq"`
			}
			_ = stdjson.Unmarshal(p.D, &rs)
			gw.mu.Lock()
			s, ok := gw.sessions[rs.SessionID]
			valid := ok && s.valid
			var replay []fakeEvent
			if valid {
				fc.sess = s
				for _, e := range s.events {
					if e.seq > rs.Seq {
						replay = append(replay, e)
					}
				}
			}
			gw.mu.Unlock()
			if !valid {
				gw.log.Server("conn #%d RESUME session=%s seq=%d -> invalid session (op 9 d=false)", fc.idx, rs.SessionID, rs.Seq)
				_ = fc.send(map[string]interface{}{"op": 9, "d": false})
				continue
			}
			fc.shardID.Store(int32(s.shard[0]))
			fc.authed.Store(true)
			gw.log.Server("conn #%d RESUME session=%s seq=%d -> replaying %d events", fc.idx, rs.SessionID, rs.Seq, len(replay))
			go fc.replay(s, replay)
		default: // gateway commands (3 presence, 4 voice, 8 request guild members)
			fc.commands.Inc()
			if !fc.isReady() {
				fc.cmdBeforeReady.Inc()
			}
			if gw.enforceAuth && !fc.authed.Load() {
				gw.log.Server("conn #%d op %d before identify/resume -> close 4003", fc.idx, p.Op)
				fc.closeWith(4003, "Not authenticated.")
				continue
			}
			if p.Op == 8 {
				var rgm struct {
					Nonce string `json:"nonce"`
				}
				_ = stdjson.Unmarshal(p.D, &rgm)
				gw.mu.Lock()
				if ch, ok := gw.nonces[rgm.Nonce]; ok {
					delete(gw.nonces, rgm.Nonce)
					close(ch)
				}
				gw.mu.Unlock()
			}
		}
	}
}

// replay sends the missed events and then RESUMED, like Discord does.
func (fc *fakeConn) replay(s *fakeSession, events []fakeEvent) {
	gap := time.Duration(fc.gw.replayGapMs.Load()) * time.Millisecond
	for _, e := range events {
		if gap > 0 {
			time.Sleep(gap)
		}
		if err := fc.sendRaw(e.frame); err != nil {
			return
		}
	}
	fc.gw.mu.Lock()
	s.seq++
	seq := s.seq
	fc.gw.mu.Unlock()
	fc.markReady()
	_ = fc.send(map[string]interface{}{"op": 0, "t": "RESUMED", "s": seq, "d": map[string]interface{}{}})
}

func (fc *fakeConn) send(v interface{}) error {
	b, err := stdjson.Marshal(v)
	if err != nil {
		return err
	}
	return fc.sendRaw(b)
}

func (fc *fakeConn) sendRaw(b []byte) error {
	fc.writeMu.Lock()
	defer fc.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return fc.ws.Write(ctx, websocket.MessageText, b)
}

// dispatch sends an op 0 event with the session's next sequence number; it is remembered for RESUME replays.
func (fc *fakeConn) dispatch(name string, data []byte) error {
	gw := fc.gw
	gw.mu.Lock()
	s := fc.sess
	if s == nil {
		gw.mu.Unlock()
		return fmt.Errorf("conn #%d has no session", fc.idx)
	}
	s.seq++
	seq := s.seq
	var b bytes.Buffer
	fmt.Fprintf(&b, `{"op":0,"t":%q,"s":%d,"d":`, name, seq)
	b.Write(data)
	b.WriteString("}")
	s.events = append(s.events, fakeEvent{seq: seq, frame: b.Bytes()})
	gw.mu.Unlock()
	return fc.sendRaw(b.Bytes())
}

func (fc *fakeConn) invalidateSession() {
	fc.gw.mu.Lock()
	if fc.sess != nil {
		fc.sess.valid = false
	}
	fc.gw.mu.Unlock()
}

// abort kills the TCP connection without a websocket close frame.
func (fc *fakeConn) abort(rst bool) {
	if rst {
		if tc, ok := fc.raw.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
	}
	_ = fc.raw.Close()
}

func (fc *fakeConn) closeWith(code int, reason string) {
	go func() { _ = fc.ws.Close(websocket.StatusCode(code), reason) }()
}

func (gw *fakeGateway) connCount() int {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	return len(gw.conns)
}

func (gw *fakeGateway) conn(i int) *fakeConn {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	if i < 0 || i >= len(gw.conns) {
		return nil
	}
	return gw.conns[i]
}

func (gw *fakeGateway) allConns() []*fakeConn {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	return append([]*fakeConn(nil), gw.conns...)
}

func (gw *fakeGateway) dialLog() []dialRecord {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	return append([]dialRecord(nil), gw.dials...)
}

func (gw *fakeGateway) totalIdentifies() int {
	var n int
	for _, fc := range gw.allConns() {
		n += int(fc.identifies.Load())
	}
	return n
}

// waitHealthy waits for a connection with idx >= minIdx (and matching shard if shard >= 0) that
// completed identify/resume, has seen >= minHB heartbeats since, and is still open.
func (gw *fakeGateway) waitHealthy(minIdx, shard, minHB int, timeout time.Duration) (*fakeConn, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, fc := range gw.allConns() {
			if fc.idx < minIdx || fc.isClosed() || !fc.isReady() {
				continue
			}
			if shard >= 0 && int(fc.shardID.Load()) != shard {
				continue
			}
			if int(fc.hbAfterReady.Load()) >= minHB {
				return fc, true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil, false
}

func (gw *fakeGateway) summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "dial attempts=%d, ws conns=%d\n", gw.dialAttempts.Load(), gw.connCount())
	for _, fc := range gw.allConns() {
		state := "open"
		if fc.isClosed() {
			state = "closed"
		}
		fmt.Fprintf(&b, "  conn #%d shard=%d path=%q %s ready=%v identifies=%d resumes=%d heartbeats=%d (after ready %d) commands=%d clientClose=%d\n",
			fc.idx, fc.shardID.Load(), fc.path, state, fc.isReady(), fc.identifies.Load(), fc.resumes.Load(), fc.heartbeats.Load(),
			fc.hbAfterReady.Load(), fc.commands.Load(), fc.clientClose.Load())
	}
	return b.String()
}

func (gw *fakeGateway) expectNonce(nonce string) chan struct{} {
	ch := make(chan struct{})
	gw.mu.Lock()
	gw.nonces[nonce] = ch
	gw.mu.Unlock()
	return ch
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// client harness (real EvtClient, real nhooyr transport)
// ---------------------------------------------------------------------------

type testConsumer struct {
	eChan    chan *Event
	shutdown chan interface{}
	gate     sync.Mutex
	paused   atomic.Bool
	probes   sync.Map
	events   atomic.Int64

	mu  sync.Mutex
	ids []string // "id" field of every MESSAGE_CREATE, in delivery order
}

var probeRe = regexp.MustCompile(`"probe":"([^"]+)"`)
var idRe = regexp.MustCompile(`"id":"([^"]+)"`)

func newTestConsumer(buf int, shutdown chan interface{}) *testConsumer {
	rc := &testConsumer{eChan: make(chan *Event, buf), shutdown: shutdown}
	go func() {
		for {
			select {
			case evt := <-rc.eChan:
				rc.gate.Lock()
				rc.gate.Unlock() //nolint:staticcheck
				rc.events.Inc()
				if evt.Name == "MESSAGE_CREATE" {
					if m := idRe.FindSubmatch(evt.Data); m != nil {
						rc.mu.Lock()
						rc.ids = append(rc.ids, string(m[1]))
						rc.mu.Unlock()
					}
				}
				if m := probeRe.FindSubmatch(evt.Data); m != nil {
					if ch, ok := rc.probes.Load(string(m[1])); ok {
						rc.probes.Delete(string(m[1]))
						close(ch.(chan struct{}))
					}
				}
			case <-rc.shutdown:
				return
			}
		}
	}()
	return rc
}

func (rc *testConsumer) pause() {
	if rc.paused.CAS(false, true) {
		rc.gate.Lock()
	}
}
func (rc *testConsumer) resume() {
	if rc.paused.CAS(true, false) {
		rc.gate.Unlock()
	}
}

func (rc *testConsumer) deliveredIDs() []string {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]string(nil), rc.ids...)
}

var probeCounter atomic.Int64

// probeEvent dispatches an event on fc and waits until the consumer receives it.
func (rc *testConsumer) probeEvent(fc *fakeConn, timeout time.Duration) error {
	id := fmt.Sprintf("p%d", probeCounter.Inc())
	ch := make(chan struct{})
	rc.probes.Store(id, ch)
	if err := fc.dispatch("MESSAGE_CREATE", []byte(fmt.Sprintf(`{"id":"probe-%s","probe":%q}`, id, id))); err != nil {
		return fmt.Errorf("server dispatch failed: %w", err)
	}
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("probe event %s not delivered to consumer within %v", id, timeout)
	}
}

type emitter interface {
	Emit(command string, data CmdPayload) error
}

// probeCommand calls Emit(REQUEST_GUILD_MEMBERS) and waits for the server to receive it.
func probeCommand(gw *fakeGateway, e emitter, timeout time.Duration) (time.Duration, error) {
	nonce := fmt.Sprintf("n%d", probeCounter.Inc())
	got := gw.expectNonce(nonce)
	start := time.Now()
	errCh := make(chan error, 1)
	go func() {
		errCh <- e.Emit(cmd.RequestGuildMembers, &RequestGuildMembersPayload{GuildIDs: []Snowflake{1 << 22}, Nonce: nonce})
	}()
	select {
	case err := <-errCh:
		if err != nil {
			return time.Since(start), fmt.Errorf("Emit returned error: %w", err)
		}
	case <-time.After(timeout):
		return time.Since(start), fmt.Errorf("Emit() blocked for > %v", timeout)
	}
	emitDur := time.Since(start)
	select {
	case <-got:
		return emitDur, nil
	case <-time.After(timeout):
		return emitDur, fmt.Errorf("command (nonce %s) was accepted by Emit but never reached the server within %v", nonce, timeout)
	}
}

// fastTimeouts keep scenario tests quick; every assertion stays >= 10x looser than these.
func fastTimeouts() *timeouts {
	return &timeouts{
		dial:              2 * time.Second,
		hello:             2 * time.Second,
		write:             2 * time.Second,
		readyIdle:         2 * time.Second,
		teardownJoinWarn:  time.Second,
		backoffMin:        20 * time.Millisecond,
		backoffMax:        200 * time.Millisecond,
		healthyAfter:      time.Second,
		disconnect:        15 * time.Second,
		invalidSessionMin: 10 * time.Millisecond,
		invalidSessionMax: 50 * time.Millisecond,
		// the fake gateway heartbeats every few hundred ms; a 60s window would leave no budget for commands
		outboundWindow: 5 * time.Second,
	}
}

type shardOpts struct {
	eventBuf     int
	httpClient   *http.Client
	timeouts     *timeouts
	endpoint     string
	connectQueue connectQueue
	identifySync *shardSync
	newConn      func() (sessionConn, error)
	shardID      uint
}

type testShard struct {
	*testConsumer
	t        *testing.T
	c        *EvtClient
	log      *testLogger
	gw       *fakeGateway
	shutdown chan interface{}
}

func newTestShard(t *testing.T, gw *fakeGateway, opts shardOpts) *testShard {
	if opts.eventBuf == 0 {
		opts.eventBuf = 2
	}
	if opts.timeouts == nil {
		opts.timeouts = fastTimeouts()
	}
	if opts.endpoint == "" {
		opts.endpoint = gw.URL
	}
	shutdown := make(chan interface{})
	rc := &testShard{t: t, gw: gw, log: gw.log, shutdown: shutdown}
	rc.testConsumer = newTestConsumer(opts.eventBuf, shutdown)
	c, err := NewEventClient(opts.shardID, &EvtConfig{
		BotToken:       "test-token",
		HTTPClient:     opts.httpClient,
		Endpoint:       opts.endpoint,
		Browser:        "disgord-test",
		Device:         "disgord-test",
		ShardCount:     1,
		Logger:         gw.log,
		EventChan:      rc.eChan,
		SystemShutdown: shutdown,
		DiscordPktPool: &sync.Pool{New: func() interface{} { return &DiscordPacket{} }},
		connectQueue:   opts.connectQueue,
		identifySync:   opts.identifySync,
		newConn:        opts.newConn,
		timeouts:       opts.timeouts,
	})
	if err != nil {
		t.Fatal(err)
	}
	rc.c = c
	t.Cleanup(func() {
		rc.resume()
		_ = c.Disconnect()
		close(shutdown)
		if t.Failed() {
			t.Logf("server:\n%s\nlog tail:\n%s", gw.summary(), gw.log.Tail(60))
		}
	})
	return rc
}

func (rc *testShard) connect(t *testing.T) *fakeConn {
	t.Helper()
	if err := rc.c.Connect(); err != nil {
		t.Fatalf("initial connect: %v", err)
	}
	fc, ok := rc.gw.waitHealthy(0, -1, 2, 20*time.Second)
	if !ok {
		t.Fatalf("initial connection never became healthy")
	}
	return fc
}

// expectRecovery asserts that, after a fault, a new connection (idx >= minIdx) completes identify/resume,
// keeps heartbeating, delivers events to the consumer, and accepts outgoing commands.
func (rc *testShard) expectRecovery(t *testing.T, minIdx int, deadline time.Duration) *fakeConn {
	t.Helper()
	fc, ok := rc.gw.waitHealthy(minIdx, -1, 3, deadline)
	if !ok {
		t.Fatalf("no healthy connection (idx>=%d, ready + 3 heartbeats) within %v; state=%s", minIdx, deadline, rc.c.stateSnapshot())
	}
	if err := rc.probeEvent(fc, 10*time.Second); err != nil {
		t.Fatalf("reconnected on conn #%d but event delivery broken: %v", fc.idx, err)
	}
	if _, err := probeCommand(rc.gw, rc.c, 10*time.Second); err != nil {
		t.Fatalf("reconnected on conn #%d but outgoing command broken: %v", fc.idx, err)
	}
	return fc
}

// ---------------------------------------------------------------------------
// goroutine inspection
// ---------------------------------------------------------------------------

func allStacks() string {
	buf := make([]byte, 64<<20)
	n := runtime.Stack(buf, true)
	return string(buf[:n])
}

// libraryGoroutines returns goroutines running event client code (not the harness).
func libraryGoroutines() []string {
	var out []string
	for _, g := range strings.Split(allStacks(), "\n\n") {
		if strings.Contains(g, "fakeGateway") || strings.Contains(g, "fakeConn") || strings.Contains(g, "testConsumer") ||
			strings.Contains(g, "testing.tRunner") || strings.Contains(g, "probeCommand") || strings.Contains(g, "scriptConn") {
			continue
		}
		if strings.Contains(g, "disgord/internal/gateway.(*session)") || strings.Contains(g, "disgord/internal/gateway.(*EvtClient)") {
			out = append(out, g)
		}
	}
	return out
}

func summarizeGoroutines() string {
	counts := map[string]int{}
	re := regexp.MustCompile(`disgord/internal/gateway\.\(\*(session|EvtClient)\)\.([A-Za-z0-9_.]+)`)
	for _, g := range libraryGoroutines() {
		key := "?"
		if m := re.FindStringSubmatch(g); m != nil {
			key = m[1] + "." + m[2]
		}
		counts[key]++
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "  %3d x %s\n", counts[k], k)
	}
	return b.String()
}

// waitNoLibraryGoroutines waits until no event client goroutine is left (supervisor, workers, identify helpers).
func waitNoLibraryGoroutines(t *testing.T, timeout time.Duration) {
	t.Helper()
	if !waitFor(timeout, func() bool { return len(libraryGoroutines()) == 0 }) {
		t.Fatalf("event client goroutines still running:\n%s", summarizeGoroutines())
	}
}

// smallBufferHTTPClient returns an http.Client whose TCP connections have a tiny kernel send buffer.
func smallBufferHTTPClient(size int) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetWriteBuffer(size)
			}
			return c, nil
		},
	}}
}
