package httd

import (
	"strings"
	"sync"
)

const GlobalHash = "global"

func relationsByBucketID(relations map[string]string) map[string][]string {
	byHash := make(map[string][]string)
	for id, hash := range relations {
		if _, ok := byHash[hash]; !ok {
			byHash[hash] = []string{id}
		} else {
			byHash[hash] = append(byHash[hash], id)
		}
	}

	return byHash
}

func NewManager(defaultRelations map[string]string) *Manager {
	global := newLeakyBucket(nil)
	global.hash = GlobalHash

	m := &Manager{
		proxy:   make(map[string]string),
		buckets: make(map[string]*ltBucket),
		global:  global,
	}

	hashRelations := relationsByBucketID(defaultRelations)
	for hash, ids := range hashRelations {
		var bucket *ltBucket
		if hash == GlobalHash {
			bucket = m.global
		} else {
			bucket = newLeakyBucket(m.global)
		}

		for i := range ids {
			m.buckets[ids[i]] = bucket
		}
	}

	return m
}

type Manager struct {
	mu sync.RWMutex

	// proxy links a local endpoint hash to a bucket key derived from the discord defined rate limit ltBucket hash
	// (see consolidatedBucketKey). When no discord ltBucket hash is known, the value and the key are the same
	proxy   map[string]string
	buckets map[string]*ltBucket

	global *ltBucket
}

var _ RESTBucketManager = (*Manager)(nil)

func (r *Manager) BucketGrouping() (group map[string][]string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	group = make(map[string][]string)
	for k, v := range r.proxy {
		group[v] = append(group[v], k)
	}
	return group
}

func (r *Manager) ProxyID(id string) (pID string) {
	// only do a write lock if we need to create a new proxy
	r.mu.RLock()
	pID, ok := r.proxy[id]
	r.mu.RUnlock()
	if !ok {
		r.mu.Lock()
		if _, ok = r.proxy[id]; !ok {
			r.proxy[id] = id
		}
		pID = r.proxy[id]
		r.mu.Unlock()
	}

	return pID
}

func (r *Manager) UpdateProxyID(id, pID, bucketKey string) {
	if bucketKey == "" || bucketKey == pID {
		return
	}

	r.mu.Lock()
	if _, exists := r.buckets[bucketKey]; !exists {
		r.buckets[bucketKey] = r.buckets[r.proxy[id]]
	}
	r.proxy[id] = bucketKey
	r.mu.Unlock()
}

func (r *Manager) Bucket(id string, cb func(bucket RESTBucket)) {
	pID := r.ProxyID(id)

	// only do a write lock if we need to create a new ltBucket
	r.mu.RLock()
	bucket, ok := r.buckets[pID]
	r.mu.RUnlock()
	if !ok {
		r.mu.Lock()
		if _, ok = r.buckets[pID]; !ok {
			r.buckets[pID] = newLeakyBucket(r.global)
		}
		bucket = r.buckets[pID]
		r.mu.Unlock()
	}

	cb(bucket)
	r.UpdateProxyID(id, pID, consolidatedBucketKey(bucket.getHash(), id))
}

// consolidatedBucketKey returns the key of the bucket shared by every endpoint that Discord reports with the given
// bucket hash. Discord's hash does not include the major parameter, which is rate limited separately.
func consolidatedBucketKey(hash, localKey string) string {
	if hash == "" || hash == GlobalHash {
		return hash
	}
	return hash + "|" + majorParam(localKey)
}

// majorParam extracts the major parameter (channel, guild or webhook id + token) from a local key: METHOD:/path.
func majorParam(localKey string) string {
	i := strings.IndexByte(localKey, '/')
	if i < 0 {
		return ""
	}
	segments := strings.Split(localKey[i+1:], "/")
	if len(segments) < 2 || segments[1] == "" {
		return ""
	}

	switch segments[0] {
	case "channels", "guilds":
		return segments[0] + "/" + segments[1]
	case "webhooks":
		if len(segments) > 2 && segments[2] != "" {
			return segments[0] + "/" + segments[1] + "/" + segments[2]
		}
		return segments[0] + "/" + segments[1]
	default:
		return ""
	}
}

func (r *Manager) Consolidate() {

}
