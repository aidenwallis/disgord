//go:build !integration
// +build !integration

package httd

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestMajorParam(t *testing.T) {
	table := map[string]string{
		"POST:/channels/1/messages":                           "channels/1",
		"GET:/channels/1":                                     "channels/1",
		"PUT:/channels/1/messages/{id}/reactions/{emoji}/@me": "channels/1",
		"GET:/guilds/2/members/{id}":                          "guilds/2",
		"GET:/guilds/2":                                       "guilds/2",
		"GET:/webhooks/3":                                     "webhooks/3",
		"POST:/webhooks/3/token":                              "webhooks/3/token",
		"PATCH:/webhooks/3/token/messages/{id}":               "webhooks/3/token",
		"/channels/1/messages":                                "channels/1",
		"GET:/channels":                                       "",
		"GET:/channels/":                                      "",
		"GET:/users/@me":                                      "",
		"GET:/gateway/bot":                                    "",
		"POST:/interactions/4/token/callback":                 "",
		"GET:/test/{id}":                                      "",
		"no-path":                                             "",
	}

	for localKey, wants := range table {
		if got := majorParam(localKey); got != wants {
			t.Errorf("majorParam(%q) = %q, wants %q", localKey, got, wants)
		}
	}
}

func rateLimitHeader(hash string, remaining int) http.Header {
	h := make(http.Header)
	h.Set(XRateLimitBucket, hash)
	h.Set(XRateLimitRemaining, strconv.Itoa(remaining))
	h.Set(XRateLimitReset, strconv.FormatInt(time.Now().Add(time.Hour).UnixNano()/int64(time.Millisecond), 10))
	return h
}

// send runs a request through the manager. The response carries the given rate limit header.
func send(t *testing.T, m *Manager, localKey string, header http.Header) (bucket RESTBucket) {
	t.Helper()
	m.Bucket(localKey, func(b RESTBucket) {
		bucket = b
		_, _, err := b.Transaction(context.Background(), func() (*http.Response, []byte, error) {
			h := make(http.Header, len(header))
			for k, v := range header {
				h[k] = append([]string(nil), v...)
			}
			return normalizedResponse(http.StatusOK, h), nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	return bucket
}

func bucketOf(m *Manager, localKey string) (bucket RESTBucket) {
	m.Bucket(localKey, func(b RESTBucket) {
		bucket = b
	})
	return bucket
}

func TestManager_ConsolidatesByHashAndMajorParam(t *testing.T) {
	const hash = "shared-hash"
	type route struct {
		localKey string
		hash     string
	}
	table := []struct {
		name string
		a, b route
		same bool
	}{
		{"different channels", route{"POST:/channels/1/messages", hash}, route{"POST:/channels/2/messages", hash}, false},
		{"different guilds", route{"GET:/guilds/1/members", hash}, route{"GET:/guilds/2/members", hash}, false},
		{"same channel", route{"POST:/channels/1/messages", hash}, route{"PATCH:/channels/1/messages/{id}", hash}, true},
		{"same guild", route{"GET:/guilds/1/members", hash}, route{"GET:/guilds/1/roles", hash}, true},
		{"same path same hash", route{"GET:/channels/1/messages", hash}, route{"POST:/channels/1/messages", hash}, true},
		{"same path different hash", route{"GET:/channels/1/messages", hash}, route{"POST:/channels/1/messages", "other"}, false},
		{"different webhook token", route{"POST:/webhooks/1/tokenA", hash}, route{"POST:/webhooks/1/tokenB", hash}, false},
		{"same webhook token", route{"POST:/webhooks/1/tokenA", hash}, route{"PATCH:/webhooks/1/tokenA/messages/{id}", hash}, true},
		{"webhook with and without token", route{"GET:/webhooks/1", hash}, route{"GET:/webhooks/1/tokenA", hash}, false},
		{"no major param", route{"GET:/users/@me", hash}, route{"GET:/users/@me/guilds", hash}, true},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			m := NewManager(nil)
			send(t, m, tt.a.localKey, rateLimitHeader(tt.a.hash, 5))
			send(t, m, tt.b.localKey, rateLimitHeader(tt.b.hash, 5))

			a, b := bucketOf(m, tt.a.localKey), bucketOf(m, tt.b.localKey)
			if same := a == b; same != tt.same {
				t.Fatalf("same bucket: got %t, wants %t. Grouping: %+v", same, tt.same, m.BucketGrouping())
			}
		})
	}
}

func TestManager_ExhaustedChannelDoesNotBlockOtherChannels(t *testing.T) {
	m := NewManager(nil)
	const hash = "create-message"
	send(t, m, "POST:/channels/1/messages", rateLimitHeader(hash, 0))
	send(t, m, "POST:/channels/2/messages", rateLimitHeader(hash, 5))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var err1, err2 error
	m.Bucket("POST:/channels/1/messages", func(b RESTBucket) {
		_, _, err1 = b.Transaction(ctx, func() (*http.Response, []byte, error) {
			return normalizedResponse(http.StatusOK, nil), nil, nil
		})
	})
	m.Bucket("POST:/channels/2/messages", func(b RESTBucket) {
		_, _, err2 = b.Transaction(ctx, func() (*http.Response, []byte, error) {
			return normalizedResponse(http.StatusOK, nil), nil, nil
		})
	})
	if err1 == nil {
		t.Error("channel 1 should be rate limited")
	}
	if err2 != nil {
		t.Errorf("channel 2 should not be rate limited by channel 1: %s", err2)
	}
}

func TestManager_UnknownHashKeepsLocalBucket(t *testing.T) {
	m := NewManager(nil)
	send(t, m, "POST:/channels/1/messages", nil)
	send(t, m, "GET:/channels/1/messages", nil)

	if bucketOf(m, "POST:/channels/1/messages") == bucketOf(m, "GET:/channels/1/messages") {
		t.Fatal("routes without a discord bucket hash should keep their own bucket")
	}
}

func TestManager_GlobalHashIsNotSplitByMajorParam(t *testing.T) {
	table := map[string]http.Header{
		// the presence check in updateAfterRequest looks up the literal (non-canonical) header key
		"empty bucket header": {XRateLimitBucket: []string{""}},
		"global header":       {http.CanonicalHeaderKey(XRateLimitGlobal): []string{"true"}},
	}

	for name, header := range table {
		t.Run(name, func(t *testing.T) {
			m := NewManager(nil)
			a := send(t, m, "POST:/channels/1/messages", header)
			send(t, m, "POST:/channels/2/messages", header)

			if hash := a.(*ltBucket).getHash(); hash != GlobalHash {
				t.Fatalf("got hash %q, wants %q", hash, GlobalHash)
			}
			if bucketOf(m, "POST:/channels/1/messages") != bucketOf(m, "POST:/channels/2/messages") {
				t.Fatal("routes with the global hash should share a bucket")
			}
			if ids := m.BucketGrouping()[GlobalHash]; len(ids) != 2 {
				t.Fatalf("global group should hold both routes, got %v", ids)
			}
		})
	}
}

func TestManager_ConcurrentBucketHash(t *testing.T) {
	m := NewManager(nil)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				m.Bucket("POST:/channels/123/messages", func(b RESTBucket) {
					_, _, err := b.Transaction(context.Background(), func() (*http.Response, []byte, error) {
						h := make(http.Header)
						h.Set(XRateLimitBucket, "shared-hash")
						return normalizedResponse(http.StatusOK, h), nil, nil
					})
					if err != nil {
						t.Error(err)
					}
				})
			}
		}()
	}
	wg.Wait()
}
