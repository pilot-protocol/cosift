package community

import (
	"math"
	"net/http"
	"strconv"
	"time"
)

const (
	routeLimiterKeys = 50000
	meterPerMinute   = 60

	contributionsPerMinute = 30
)

// allowRoute charges a per-user, per-route window; the caller must have authenticated, so anonymous traffic never adds a key.
func (s *Server) allowRoute(w http.ResponseWriter, route, userID string, n int) bool {
	now := time.Now()
	ok, until := s.routeLimits.take(route+":"+userID, n, time.Minute, now)
	if ok {
		return true
	}
	seconds := max(1, int(math.Ceil(until.Sub(now).Seconds())))
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	respond(w, http.StatusTooManyRequests, map[string]any{"error": "rate limit reached", "code": "rate_limited", "retry_after_seconds": seconds})
	return false
}
