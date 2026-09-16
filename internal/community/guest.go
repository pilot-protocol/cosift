package community

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

const guestCooldown = 30 * time.Minute

// clientIP trusts forwarded addresses only when the direct peer is a configured
// proxy. Walk from right to left so client-supplied XFF cannot bypass quotas.
func (s *Server) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return "unknown"
	}
	peer = peer.Unmap()
	trusted := func(ip netip.Addr) bool {
		for _, prefix := range s.trustedProxies {
			if prefix.Contains(ip) {
				return true
			}
		}
		return false
	}
	if !trusted(peer) {
		return peer.String()
	}
	chain := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	if len(chain) > 32 {
		return peer.String()
	}
	current := peer
	for i := len(chain) - 1; i >= 0; i-- {
		if !trusted(current) {
			return current.String()
		}
		ip, err := netip.ParseAddr(strings.TrimSpace(chain[i]))
		if err != nil {
			return peer.String()
		}
		current = ip.Unmap()
	}
	return current.String()
}

func (s *Server) guestKey(r *http.Request) string {
	return tokenHash(s.guestSalt + ":" + s.clientIP(r))
}

func (s *Server) guestStatus(w http.ResponseWriter, r *http.Request) {
	var until int64
	err := s.db.QueryRowContext(r.Context(), `SELECT expires_at FROM guest_usage WHERE ip_hash=?`, s.guestKey(r)).Scan(&until)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		problem(w, 500, "could not check guest allowance")
		return
	}
	respond(w, 200, map[string]any{"available": until <= time.Now().Unix(), "retry_at": until, "interval_seconds": int(guestCooldown.Seconds())})
}

// reserveGuest is atomic across concurrent requests and survives restarts.
// Failure paths release this exact reservation; success commits the cooldown.
func (s *Server) reserveGuest(w http.ResponseWriter, r *http.Request) (finish func(bool), ok bool) {
	key, token := s.guestKey(r), randomID()
	now := time.Now().Unix()
	until := now + int64(guestCooldown.Seconds())
	_, err := s.db.ExecContext(r.Context(), `DELETE FROM guest_usage WHERE expires_at<=?`, now)
	if err != nil {
		problem(w, 500, "guest allowance unavailable")
		return nil, false
	}
	res, err := s.db.ExecContext(r.Context(), `INSERT INTO guest_usage(ip_hash,expires_at,reservation) VALUES(?,?,?) ON CONFLICT(ip_hash) DO UPDATE SET expires_at=excluded.expires_at,reservation=excluded.reservation WHERE guest_usage.expires_at<=?`, key, until, token, now)
	if err != nil {
		problem(w, 500, "guest allowance unavailable")
		return nil, false
	}
	n, err := res.RowsAffected()
	if err != nil {
		problem(w, 500, "guest allowance unavailable")
		return nil, false
	}
	if n == 0 {
		if err := s.db.QueryRowContext(r.Context(), `SELECT expires_at FROM guest_usage WHERE ip_hash=?`, key).Scan(&until); err != nil {
			problem(w, 500, "guest allowance unavailable")
			return nil, false
		}
		seconds := max(int64(1), until-now)
		w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
		respond(w, 429, map[string]any{"error": fmt.Sprintf("Guest access allows one Search, Research, Answer, or submission every 30 minutes. Try again in %d minutes, or sign in.", (seconds+59)/60), "retry_at": until, "retry_after_seconds": seconds})
		return nil, false
	}
	return func(success bool) {
		if success {
			return
		}
		// The request may have been cancelled. Still release failed work.
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = s.db.ExecContext(ctx, `DELETE FROM guest_usage WHERE ip_hash=? AND reservation=?`, key, token)
	}, true
}

func (s *Server) optionalAuth(next userHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(cookieName)
		if err != nil {
			next(w, r, User{})
			return
		}
		u, err := scanUser(s.db.QueryRowContext(r.Context(), `SELECT u.id,u.email,u.name,u.interests,u.onboarded FROM users u JOIN sessions s ON u.id=s.user_id WHERE s.hash=? AND s.expires_at>?`, tokenHash(cookie.Value), time.Now().Unix()))
		if errors.Is(err, sql.ErrNoRows) {
			next(w, r, User{})
			return
		}
		if err != nil {
			problem(w, 500, "account unavailable")
			return
		}
		next(w, r, u)
	}
}
