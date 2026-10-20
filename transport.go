package disgord

import (
	"context"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/proxy"
)

const defaultResponseHeaderTimeout = 60 * time.Second

// newTransport returns a clone of http.DefaultTransport that also limits the wait for response headers.
// When dialer is not nil, every connection is made through it (and environment proxies are ignored).
func newTransport(dialer proxy.Dialer) *http.Transport {
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
