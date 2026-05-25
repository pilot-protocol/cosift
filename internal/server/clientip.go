package server

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

// clientIPResolver decides what counts as the "client IP" for rate limiting.
// Default behavior: return the direct TCP peer's IP (RemoteAddr without port).
// When configured with trustedProxies, walks X-Forwarded-For for requests
// that came in through a trusted reverse proxy.
//
// Why not just always trust XFF: any caller can set XFF on a direct request.
// Trusting it unconditionally lets attackers spoof their IP and bypass the
// limiter. Only trust the header when we know who terminated the TCP
// connection — i.e., the direct peer is a known proxy.
type clientIPResolver struct {
	trustedProxies []*net.IPNet
}

// newClientIPResolver parses the CIDR list. Returns nil resolver + error on
// malformed input — caller decides whether to fail-fast or fall back to
// direct-only.
func newClientIPResolver(cidrs []string) (*clientIPResolver, error) {
	r := &clientIPResolver{trustedProxies: make([]*net.IPNet, 0, len(cidrs))}
	for _, c := range cidrs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q: %w", c, err)
		}
		r.trustedProxies = append(r.trustedProxies, n)
	}
	return r, nil
}

// Resolve returns the client IP for the given request. Falls back to the
// direct peer when there's no XFF, when no proxies are trusted, or when
// the immediate peer isn't on the trust list.
func (r *clientIPResolver) Resolve(req *http.Request) string {
	direct := directPeerIP(req.RemoteAddr)
	if r == nil || len(r.trustedProxies) == 0 || direct == "" {
		return direct
	}
	if !r.isTrusted(direct) {
		return direct
	}
	xff := req.Header.Get("X-Forwarded-For")
	if xff == "" {
		return direct
	}
	// Walk RIGHT-TO-LEFT (rightmost is the proxy that called us). The first
	// IP we see that ISN'T itself a trusted proxy is the real client.
	parts := strings.Split(xff, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		p := strings.TrimSpace(parts[i])
		if p == "" {
			continue
		}
		// Strip any zone suffix or brackets from IPv6 entries.
		p = strings.TrimPrefix(p, "[")
		p = strings.TrimSuffix(p, "]")
		if r.isTrusted(p) {
			continue
		}
		// Validate it parses as an IP — defends against header garbage.
		if net.ParseIP(p) == nil {
			continue
		}
		return p
	}
	return direct
}

func (r *clientIPResolver) isTrusted(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	for _, n := range r.trustedProxies {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// directPeerIP extracts the host portion of r.RemoteAddr (strips :port).
// Renamed from clientIP() to make the distinction clear: this is the TCP
// peer, NOT necessarily the originating client.
func directPeerIP(remoteAddr string) string {
	if h, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return h
	}
	return remoteAddr
}
