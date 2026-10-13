package community

import (
	"container/list"
	"net/http"
	"sync"
	"time"
)

const limiterKeys = 10000

type limitEntry struct {
	key   string
	count int
	until time.Time
}

// limiter is a fixed-window counter over at most max keys; a full store evicts its oldest key, never refuses a new one.
type limiter struct {
	mu    sync.Mutex
	max   int
	keys  map[string]*list.Element
	order *list.List
}

func newLimiter(max int) *limiter {
	return &limiter{max: max, keys: map[string]*list.Element{}, order: list.New()}
}

func (l *limiter) drop(e *list.Element) {
	delete(l.keys, e.Value.(*limitEntry).key)
	l.order.Remove(e)
}

func (l *limiter) allow(key string, n int, window time.Duration, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for e := l.order.Front(); e != nil && !e.Value.(*limitEntry).until.After(now); e = l.order.Front() {
		l.drop(e)
	}
	if n < 1 {
		return false
	}
	if e, ok := l.keys[key]; ok {
		b := e.Value.(*limitEntry)
		if !b.until.After(now) {
			l.drop(e)
		} else {
			if b.count >= n {
				return false
			}
			b.count++
			return true
		}
	}
	for len(l.keys) >= l.max {
		l.drop(l.order.Front())
	}
	l.keys[key] = l.order.PushBack(&limitEntry{key: key, count: 1, until: now.Add(window)})
	return true
}

// allow limits unauthenticated keys (client network, attacker-chosen email); allowUser limits keys that need a session.
func (s *Server) allow(key string, n int, window time.Duration) bool {
	return s.anonLimits.allow(key, n, window, time.Now())
}

func (s *Server) allowUser(key string, n int, window time.Duration) bool {
	return s.userLimits.allow(key, n, window, time.Now())
}

func (s *Server) clientNet(r *http.Request) string { return netKey(s.clientIP(r)) }
