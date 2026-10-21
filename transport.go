package disgord

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/proxy"
)

const (
	defaultResponseHeaderTimeout = 60 * time.Second
	defaultMaxIdleConnsPerHost   = 100
)

// newTransport returns a clone of http.DefaultTransport that only uses HTTP/1.1, keeps more idle connections per
// host and limits the wait for response headers. When dialer is not nil, every connection is made through it (and
// environment proxies are ignored).
//
// HTTP/2 is disabled on purpose: it multiplexes every REST request over a single connection, so one silently dead
// connection stalls all of them until it is detected. With HTTP/1.1 a dead connection only holds up the request
// using it, until ResponseHeaderTimeout aborts it, while other requests use other connections.
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
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	}
	t.ResponseHeaderTimeout = defaultResponseHeaderTimeout

	// nearly every request goes to discord.com; with the default of 2 idle connections per host, most connections
	// opened during a burst would be closed again and re-dialed for the next one
	t.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost

	// a non-nil, empty TLSNextProto disables HTTP/2
	t.ForceAttemptHTTP2 = false
	t.TLSNextProto = map[string]func(authority string, c *tls.Conn) http.RoundTripper{}

	// once http.DefaultTransport was used, its TLS config offers h2 and Clone copies it. Discord would accept h2
	// and the connection would be unusable, as nothing handles it anymore.
	if t.TLSClientConfig != nil {
		var protos []string
		for _, p := range t.TLSClientConfig.NextProtos {
			if p != "h2" {
				protos = append(protos, p)
			}
		}
		t.TLSClientConfig.NextProtos = protos
	}

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
