package community

import (
	"errors"
	"log"
	"math"
	"net/http"
	"net/netip"
	"strconv"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const bearerFailuresPerMinute = 30

// failureNet is the network a failed bearer is counted against: the /24 for IPv4, the /64 for IPv6.
func failureNet(ip string) string {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}
	if a = a.Unmap(); a.Is4() {
		return netip.PrefixFrom(a, 24).Masked().String()
	}
	return netip.PrefixFrom(a.WithZone(""), 64).Masked().String()
}

// authFailed answers a bearer that did not authenticate, with 429 beyond the network's budget; it runs after verification, so a valid token is never throttled.
func (s *Server) authFailed(w http.ResponseWriter, r *http.Request, err error) {
	if !errors.Is(err, sharedaccount.ErrUnauthorized) {
		sharedProblem(w, err)
		return
	}
	net := failureNet(s.clientIP(r))
	now := time.Now()
	ok, until := s.failLimits.take("bearer-failure:"+net, bearerFailuresPerMinute, time.Minute, now)
	if ok {
		sharedProblem(w, err)
		return
	}
	if s.failLogs.allow("bearer-failure-log:"+net, 1, time.Minute, now) {
		log.Printf("community: more than %d failed bearers a minute from %s", bearerFailuresPerMinute, net)
	}
	seconds := max(1, int(math.Ceil(until.Sub(now).Seconds())))
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	problemCode(w, http.StatusTooManyRequests, "too many failed attempts; try again later", "rate_limited", map[string]any{"retry_after_seconds": seconds})
}
