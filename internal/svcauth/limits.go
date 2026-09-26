package svcauth

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	throttleSummaryEvery = time.Minute
	failSweepEvery       = 10 * time.Second
)

// failedAuthLimiter is the per-client bucket charged on every 401. It only
// ever turns a failure into a 429; it never sees a request that authenticates.
type failedAuthLimiter struct {
	now  func() time.Time
	logf func(string, ...any)

	mu        sync.Mutex
	clients   map[string]*failEntry
	lastSweep time.Time
}

type failEntry struct {
	b          bucket
	suppressed int
	summaryAt  time.Time
}

func newFailedAuthLimiter(now func() time.Time, logf func(string, ...any)) *failedAuthLimiter {
	return &failedAuthLimiter{now: now, logf: logf, clients: map[string]*failEntry{}}
}

// fail charges one failure. A refused charge is counted towards the client's
// summary line, written at most once per minute.
func (l *failedAuthLimiter) fail(client string, fa FailedAuth) (time.Duration, bool) {
	now := l.now()
	rate := float64(fa.PerMinute) / 60
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sweep(now, fa)
	e := l.clients[client]
	if e == nil {
		e = &failEntry{}
		l.clients[client] = e
	}
	e.b.refill(now, rate, float64(fa.Burst))
	if e.b.tokens >= 1 {
		e.b.tokens--
		return 0, true
	}
	e.suppressed++
	if e.summaryAt.IsZero() || now.Sub(e.summaryAt) >= throttleSummaryEvery {
		l.summary(client, e)
		e.summaryAt = now
	}
	return e.b.wait(rate), false
}

func (l *failedAuthLimiter) summary(client string, e *failEntry) {
	if l.logf != nil {
		l.logf("v1 auth_throttled client=%s count=%d", client, e.suppressed)
	}
	e.suppressed = 0
}

// sweep drops clients whose bucket has refilled, flushing any pending count.
func (l *failedAuthLimiter) sweep(now time.Time, fa FailedAuth) {
	if now.Sub(l.lastSweep) < failSweepEvery {
		return
	}
	l.lastSweep = now
	rate, capacity := float64(fa.PerMinute)/60, float64(fa.Burst)
	for k, e := range l.clients {
		e.b.refill(now, rate, capacity)
		if e.b.tokens < capacity {
			continue
		}
		if e.suppressed > 0 {
			l.summary(k, e)
		}
		delete(l.clients, k)
	}
}

func peerAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return host
}

// clientKey is the failed-auth limiter key: the single X-Forwarded-For value a
// loopback proxy wrote, loopback-direct for an on-box caller, else the peer.
func clientKey(r *http.Request, clientIPHeader string) string {
	peer := peerAddr(r)
	if ip := net.ParseIP(peer); ip == nil || !ip.IsLoopback() {
		return peer
	}
	if xff := r.Header.Values("X-Forwarded-For"); len(xff) == 1 && !strings.Contains(xff[0], ",") {
		if ip := net.ParseIP(strings.TrimSpace(xff[0])); ip != nil {
			return ip.String()
		}
	}
	if !forwarded(r, clientIPHeader) {
		return "loopback-direct"
	}
	return peer
}
