package disgord

import (
	"context"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/proxy"
)

const (
	defaultResponseHeaderTimeout = 60 * time.Second
	defaultH2ReadIdleTimeout     = 30 * time.Second
	defaultH2PingTimeout         = 15 * time.Second
)

// newTransport returns a clone of http.DefaultTransport that also limits the wait for response headers and
// health checks HTTP/2 connections. When dialer is not nil, every connection is made through it (and environment
// proxies are ignored).
func newTransport(dialer proxy.Dialer) *http.Transport {
	return newTransportWithHealthCheck(dialer, defaultH2ReadIdleTimeout, defaultH2PingTimeout)
}

// newTransportWithHealthCheck is newTransport with custom HTTP/2 health check timeouts: when nothing was read from
// a connection for readIdle, a ping is sent and the connection is closed unless the ping is answered within ping.
func newTransportWithHealthCheck(dialer proxy.Dialer, readIdle, ping time.Duration) *http.Transport {
	var t *http.Transport
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		t = dt.Clone()
	} else {
		t = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	}
	t.ResponseHeaderTimeout = defaultResponseHeaderTimeout

	if dialer != nil {
		t.Proxy = nil
		t.DialContext = proxyDialContext(dialer)
	}

	// REST requests to Discord are multiplexed over a single HTTP/2 connection. Without health checks a silently
	// dead connection hangs every request until the OS gives up on it, which can take many minutes.
	if h2, err := http2.ConfigureTransports(t); err == nil {
		h2.ReadIdleTimeout = readIdle
		h2.PingTimeout = ping
	}
	return t
}

func proxyDialContext(dialer proxy.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	if d, ok := dialer.(proxy.ContextDialer); ok {
		return d.DialContext
	}

	// a plain proxy.Dialer can not be cancelled, the dial lasts as long as the dialer allows
	return func(_ context.Context, network, addr string) (net.Conn, error) {
		return dialer.Dial(network, addr)
	}
}
