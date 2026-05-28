package server

import (
	"container/list"
	"sync"
	"time"
)

// lruEntry is one slot in the LRU eviction list.
type lruEntry[V any] struct {
	key     string
	value   V
	expires time.Time // zero = no TTL
}

// lruCache is a thread-safe, bounded LRU cache with optional per-entry TTL.
// When the cache exceeds maxSize, the least-recently-used entry is evicted.
// A zero TTL disables time-based expiry.
type lruCache[V any] struct {
	mu        sync.Mutex
	maxSize   int
	ttl       time.Duration
	items     map[string]*list.Element
	evictList *list.List
}

func newLRU[V any](maxSize int, ttl time.Duration) *lruCache[V] {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &lruCache[V]{
		maxSize:   maxSize,
		ttl:       ttl,
		items:     make(map[string]*list.Element, maxSize),
		evictList: list.New(),
	}
}

// get returns the value for key and true on hit. On miss or expired entry,
// returns the zero value and false. A hit promotes the entry to MRU.
func (c *lruCache[V]) get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	entry := elem.Value.(*lruEntry[V])
	if c.ttl > 0 && !entry.expires.IsZero() && time.Now().After(entry.expires) {
		c.evictList.Remove(elem)
		delete(c.items, key)
		var zero V
		return zero, false
	}
	c.evictList.MoveToFront(elem)
	return entry.value, true
}

// set inserts or updates key→value and promotes the entry to MRU. If the
// cache is at capacity, the LRU entry is evicted.
func (c *lruCache[V]) set(key string, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.evictList.MoveToFront(elem)
		entry := elem.Value.(*lruEntry[V])
		entry.value = value
		if c.ttl > 0 {
			entry.expires = time.Now().Add(c.ttl)
		}
		return
	}
	entry := &lruEntry[V]{key: key, value: value}
	if c.ttl > 0 {
		entry.expires = time.Now().Add(c.ttl)
	}
	elem := c.evictList.PushFront(entry)
	c.items[key] = elem
	for c.evictList.Len() > c.maxSize {
		oldest := c.evictList.Back()
		if oldest == nil {
			break
		}
		c.evictList.Remove(oldest)
		delete(c.items, oldest.Value.(*lruEntry[V]).key)
	}
}

// len returns the current number of entries.
func (c *lruCache[V]) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evictList.Len()
}
