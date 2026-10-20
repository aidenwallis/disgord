package gateway

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/aidenwallis/disgord/json"

	"go.uber.org/atomic"

	"nhooyr.io/websocket"
)

var errConnNotOpen = errors.New("websocket connection is not open")

// newConn creates the long-lived wrapper used by the (legacy) voice client.
func newConn(httpClient *http.Client) (Conn, error) {
	return &nhooyr{
		httpClient: httpClient,
	}, nil
}

// newSessionConn creates the wrapper for one event client session. Every write is bounded by writeTimeout,
// which also works around writes that can otherwise hang forever in nhooyr v1.8.7 (#405).
func newSessionConn(httpClient *http.Client, writeTimeout time.Duration) sessionConn {
	return &nhooyr{
		httpClient:   httpClient,
		writeTimeout: writeTimeout,
	}
}

type nhooyr struct {
	mu           sync.RWMutex
	c            *websocket.Conn
	httpClient   *http.Client
	writeTimeout time.Duration
	isConnected  atomic.Bool
}

func (g *nhooyr) conn() *websocket.Conn {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.c
}

func (g *nhooyr) Open(ctx context.Context, endpoint string, requestHeader http.Header) (err error) {
	// establish ws connection
	c, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: g.httpClient,
		HTTPHeader: requestHeader,
	})
	if err != nil {
		if c != nil {
			_ = c.Close(websocket.StatusNormalClosure, "")
		}
		return err
	}

	c.SetReadLimit(32768 * 10000) // discord.. Can we add stream support?
	g.mu.Lock()
	g.c = c
	g.mu.Unlock()
	g.isConnected.Store(true)
	return nil
}

func (g *nhooyr) WriteJSON(v interface{}) (err error) {
	c := g.conn()
	if c == nil {
		return errConnNotOpen
	}

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if g.writeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.writeTimeout)
		defer cancel()
	}
	return c.Write(ctx, websocket.MessageText, data)
}

func (g *nhooyr) Close() (err error) {
	return g.closeWith(websocket.StatusNormalClosure, "Bot is shutting down")
}

func (g *nhooyr) CloseWithStatus(code int, reason string) error {
	return g.closeWith(websocket.StatusCode(code), reason)
}

func (g *nhooyr) closeWith(code websocket.StatusCode, reason string) (err error) {
	c := g.conn()
	if c == nil {
		g.isConnected.Store(false)
		return nil
	}

	err = c.Close(code, reason)
	if !g.isConnected.Load() {
		err = nil // discard error if we're already closed, should be a noop anyways
	}
	g.isConnected.Store(false)
	return err
}

func (g *nhooyr) Read(ctx context.Context) (packet []byte, err error) {
	c := g.conn()
	if c == nil {
		return nil, errConnNotOpen
	}

	var messageType websocket.MessageType
	messageType, packet, err = c.Read(ctx)
	if err != nil {
		// Cancelling Read by ctx results in closed WS, see issue
		// https://github.com/nhooyr/websocket/issues/242
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			g.isConnected.Store(false)
			return nil, context.Canceled
		}
		var closeErr websocket.CloseError
		if errors.As(err, &closeErr) {
			g.isConnected.Store(false)
			err = &CloseErr{
				code: int(closeErr.Code),
				info: closeErr.Error(),
			}
		}
		return nil, err
	}

	if messageType == websocket.MessageBinary {
		packet, err = decompressBytes(packet)
	}
	return packet, nil
}

func (g *nhooyr) Disconnected() bool {
	return !g.isConnected.Load()
}

var _ Conn = (*nhooyr)(nil)
var _ sessionConn = (*nhooyr)(nil)
