// Package answercache caches /answer responses + collapses concurrent
// identical requests to a single LLM call.
//
// Two pieces in one type: a TTL'd byte-blob cache (the serialized JSON
// response from the previous identical request) and a singleflight
// group keyed on the same key. When N clients send the same /answer
// query in the same TTL window, the first computes, the rest hit the
// cache; when N arrive simultaneously before the first completes, the
// rest block on the same in-flight call and all get the same answer.
//
// Use case: serving a popular query under a 100 RPS spike — without
// this layer every request would do hybrid retrieval + rerank + LLM
// synthesis, ~3-8s each. With this layer the first one pays; the rest
// hit either the in-flight join (sub-second) or the TTL cache (µs).
package answercache

import (
	"sync"
	"sync/atomic"
	"time"
)

// Cache is a TTL'd byte-blob cache fused with singleflight. Safe for
// concurrent callers.
type Cache struct {
	ttl time.Duration
	cap int

	mu    sync.RWMutex
	store map[string]entry
	calls map[string]*call

	hits   atomic.Int64
	misses atomic.Int64
	shared atomic.Int64 // singleflight joins
}

type entry struct {
	val []byte
	exp time.Time
}

type call struct {
	done chan struct{}
	val  []byte
	err  error
}

// New builds a cache with the given TTL and entry cap. ttl≤0 disables
// caching (singleflight still works); cap≤0 leaves the cache unbounded
// (don't do this in production).
func New(ttl time.Duration, cap int) *Cache {
	return &Cache{
		ttl:   ttl,
		cap:   cap,
		store: make(map[string]entry, max(16, cap)),
		calls: make(map[string]*call),
	}
}

// Get returns the cached value when fresh. nil + false on miss or expiry.
func (c *Cache) Get(key string) ([]byte, bool) {
	if c == nil || c.ttl <= 0 {
		return nil, false
	}
	c.mu.RLock()
	e, ok := c.store[key]
	c.mu.RUnlock()
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	if time.Now().After(e.exp) {
		c.misses.Add(1)
		return nil, false
	}
	c.hits.Add(1)
	return e.val, true
}

// Set stores val under key with the configured TTL. Random eviction
// when over cap — LRU is overkill at this scale and would need a
// per-entry mutex traversal.
func (c *Cache) Set(key string, val []byte) {
	if c == nil || c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	if c.cap > 0 && len(c.store) >= c.cap {
		for k := range c.store {
			delete(c.store, k)
			break
		}
	}
	c.store[key] = entry{val: val, exp: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}

// Do is the singleflight entry point. fn runs at most once per key
// concurrently; concurrent calls with the same key block on the first
// one and get its return values. The TTL cache is checked before fn
// is invoked and the result is stored on success. Returns (val, err,
// shared) where shared=true means this call joined an in-flight one.
func (c *Cache) Do(key string, fn func() ([]byte, error)) ([]byte, error, bool) {
	if v, ok := c.Get(key); ok {
		return v, nil, true
	}
	c.mu.Lock()
	if existing, ok := c.calls[key]; ok {
		c.mu.Unlock()
		<-existing.done
		c.shared.Add(1)
		return existing.val, existing.err, true
	}
	ca := &call{done: make(chan struct{})}
	c.calls[key] = ca
	c.mu.Unlock()

	val, err := fn()
	ca.val = val
	ca.err = err
	close(ca.done)

	c.mu.Lock()
	delete(c.calls, key)
	c.mu.Unlock()

	if err == nil && len(val) > 0 {
		c.Set(key, val)
	}
	return val, err, false
}

// Stats is a snapshot for /metrics + /stats publishing.
type Stats struct {
	Entries int   `json:"entries"`
	Cap     int   `json:"cap"`
	TTLSec  int64 `json:"ttl_sec"`
	Hits    int64 `json:"hits"`
	Misses  int64 `json:"misses"`
	Shared  int64 `json:"shared"`
}

func (c *Cache) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	c.mu.RLock()
	n := len(c.store)
	c.mu.RUnlock()
	return Stats{
		Entries: n,
		Cap:     c.cap,
		TTLSec:  int64(c.ttl / time.Second),
		Hits:    c.hits.Load(),
		Misses:  c.misses.Load(),
		Shared:  c.shared.Load(),
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
