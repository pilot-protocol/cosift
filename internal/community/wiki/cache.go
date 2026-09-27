package wiki

import (
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	articleBound  = 300 * time.Second
	notFoundBound = 60 * time.Second
	hubBound      = 300 * time.Second
	sitemapBound  = 3600 * time.Second

	pageCap     = 20000
	negativeCap = 120

	unknownRate  = 2
	unknownBurst = 2

	ipLimit  = 120
	ipWindow = 60 * time.Second
	ipCap    = 10000
)

// entry is a rendered by-slug outcome, served until fetched+bound.
type entry struct {
	kind     outcome
	status   int
	body     []byte
	location string
	fetched  time.Time
	bound    time.Duration
}

func (e *entry) remaining(now time.Time) (time.Duration, bool) {
	left := e.bound - now.Sub(e.fetched)
	return left, left > 0
}

// maxAge is R = max(0, bound − age) in whole seconds.
func maxAge(bound, age time.Duration) int {
	return max(0, int((bound-age)/time.Second))
}

type listing struct {
	ID, Slug, Title, Vertical string
	Updated                   time.Time
	UpdatedRaw                string
	Promoted                  bool
}

// snapshot is one complete walk of the published list, taken at at.
type snapshot struct {
	at       time.Time
	known    map[string]bool
	promoted []listing
	sitemap  []listing
}

func buildSnapshot(at time.Time, items []listing) *snapshot {
	s := &snapshot{at: at, known: make(map[string]bool, len(items))}
	for _, it := range items {
		s.known[it.Slug] = true
		if it.Promoted {
			s.promoted = append(s.promoted, it)
		}
	}
	slices.SortFunc(s.promoted, func(a, b listing) int { return strings.Compare(a.Slug, b.Slug) })
	s.sitemap = slices.Clone(s.promoted)
	slices.SortFunc(s.sitemap, func(a, b listing) int {
		if c := b.Updated.Compare(a.Updated); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	return s
}

func (s *snapshot) without(slug string) *snapshot {
	out := &snapshot{at: s.at, known: make(map[string]bool, len(s.known))}
	for k := range s.known {
		if k != slug {
			out.known[k] = true
		}
	}
	drop := func(l listing) bool { return l.Slug == slug }
	out.promoted = slices.DeleteFunc(slices.Clone(s.promoted), drop)
	out.sitemap = slices.DeleteFunc(slices.Clone(s.sitemap), drop)
	return out
}

func (s *snapshot) vertical(v string) []listing {
	var out []listing
	for _, l := range s.promoted {
		if l.Vertical == v {
			out = append(out, l)
		}
	}
	return out
}

// tokens is the unknown-slug engine budget: a token bucket.
type tokens struct {
	have float64
	last time.Time
}

func (t *tokens) take(now time.Time) bool {
	switch {
	case t.last.IsZero():
		t.have = unknownBurst
	case now.After(t.last):
		t.have = min(unknownBurst, t.have+now.Sub(t.last).Seconds()*unknownRate)
	}
	if now.After(t.last) {
		t.last = now
	}
	if t.have < 1 {
		return false
	}
	t.have--
	return true
}

// ipLimiter is the wiki's own per-client limit on cache misses: ipLimit
// requests per ipWindow per client. At ipCap clients the oldest is evicted.
type ipLimiter struct {
	mu    sync.Mutex
	m     map[string]ipWindowState
	order []ipSlot
}

type ipSlot struct {
	key   string
	until time.Time
}

// clientKey is the limiter key: an IPv4 address, or an IPv6 address's /64.
func clientKey(ip string) string {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}
	if a = a.Unmap(); a.Is4() {
		return a.String()
	}
	return netip.PrefixFrom(a.WithZone(""), 64).Masked().String()
}

type ipWindowState struct {
	n     int
	until time.Time
}

func (l *ipLimiter) allow(key string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m == nil {
		l.m = map[string]ipWindowState{}
	}
	for len(l.order) > 0 && !now.Before(l.order[0].until) {
		l.pop()
	}
	w, ok := l.m[key]
	if !ok || !now.Before(w.until) {
		for !ok && len(l.order) > 0 && len(l.m) >= ipCap {
			l.pop()
		}
		w = ipWindowState{until: now.Add(ipWindow)}
		l.order = append(l.order, ipSlot{key, w.until})
	}
	if w.n >= ipLimit {
		return false, w.until.Sub(now)
	}
	w.n++
	l.m[key] = w
	return true, 0
}

// pop drops the oldest slot, and its client when that slot is still current.
func (l *ipLimiter) pop() {
	s := l.order[0]
	l.order = l.order[1:]
	if w, ok := l.m[s.key]; ok && w.until.Equal(s.until) {
		delete(l.m, s.key)
	}
}

func (l *ipLimiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.m)
}

// flight is one in-progress engine fetch that concurrent misses share.
type flight struct {
	done chan struct{}
	e    *entry
	code string
}
