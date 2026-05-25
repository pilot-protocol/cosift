package server

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

// ipLimiter is a small per-IP sliding-window limiter for public mutate endpoints
// (currently just /feedback). Not a leaky bucket because tracking the actual
// timestamps lets us answer "how many in the last minute" exactly, with no
// fractional-token bookkeeping.
//
// Why not pull in `golang.org/x/time/rate`: that's per-key rate limiting too,
// but adds a dep and requires manual eviction. Our 60-LOC version is enough
// for the throttle we actually need.
//
// Memory: O(unique_IPs * limit). At limit=60 and 1k IPs that's 60k time.Time =
// ~1.5 MB. Linear in unique callers. The sweep below drops idle entries.
type ipLimiter struct {
	mu     sync.Mutex
	hits   map[string][]time.Time
	limit  int
	window time.Duration
}

func newIPLimiter(limit int, window time.Duration) *ipLimiter {
	if limit <= 0 {
		limit = 60
	}
	if window <= 0 {
		window = time.Minute
	}
	l := &ipLimiter{
		hits:   make(map[string][]time.Time),
		limit:  limit,
		window: window,
	}
	go l.sweep()
	return l
}

// Allow returns true if this IP has fewer than `limit` requests in the last
// `window`. Records the new request when allowed.
func (l *ipLimiter) Allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-l.window)

	h := l.hits[ip]
	// Trim stale entries in place.
	n := 0
	for _, t := range h {
		if t.After(cutoff) {
			h[n] = t
			n++
		}
	}
	h = h[:n]

	if n >= l.limit {
		l.hits[ip] = h
		return false
	}
	l.hits[ip] = append(h, now)
	return true
}

// sweep periodically drops IPs whose timestamps have all aged out. Runs once
// per window — cheap enough that we don't need to track an explicit "last
// access" time per IP.
func (l *ipLimiter) sweep() {
	t := time.NewTicker(l.window)
	defer t.Stop()
	for range t.C {
		l.mu.Lock()
		cutoff := time.Now().Add(-l.window)
		for ip, h := range l.hits {
			alive := false
			for _, ts := range h {
				if ts.After(cutoff) {
					alive = true
					break
				}
			}
			if !alive {
				delete(l.hits, ip)
			}
		}
		l.mu.Unlock()
	}
}

// clientIP is the package-level fallback for callers without a Server context.
// Equivalent to a resolver with no trusted proxies — returns the direct peer.
// Server methods should call s.resolveClientIP(r) instead.
func clientIP(r *http.Request) string {
	ip := directPeerIP(r.RemoteAddr)
	ip = strings.TrimPrefix(ip, "[")
	ip = strings.TrimSuffix(ip, "]")
	return ip
}
