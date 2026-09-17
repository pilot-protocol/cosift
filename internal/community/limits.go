package community

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

func (c *Config) defaultLimits() error {
	if c.GuestInterval == 0 {
		c.GuestInterval = 30 * time.Minute
	}
	if c.SearchRPM == 0 {
		c.SearchRPM = 120
	}
	if c.AnswerRPM == 0 {
		c.AnswerRPM = 20
	}
	if c.ResearchPer10Min == 0 {
		c.ResearchPer10Min = 3
	}
	if c.GuestInterval < time.Second || c.GuestInterval > 24*time.Hour || c.GuestInterval%time.Second != 0 {
		return fmt.Errorf("guest interval must be whole seconds between 1s and 24h")
	}
	for _, n := range []int{c.SearchRPM, c.AnswerRPM, c.ResearchPer10Min} {
		if n < 1 || n > 10000 {
			return fmt.Errorf("request limits must be between 1 and 10000")
		}
	}
	return nil
}

func (s *Server) limitPolicy() map[string]any {
	guest := s.guestIntervals()
	return map[string]any{
		"guest_interval_seconds":             int(s.cfg.GuestInterval.Seconds()),
		"guest":                              map[string]any{"search": modeLimit{1, guest["search"]}, "answer": modeLimit{1, guest["answer"]}, "research": modeLimit{1, guest["research"]}},
		"member":                             map[string]any{"search": modeLimit{s.cfg.SearchRPM, 60}, "answer": modeLimit{s.cfg.AnswerRPM, 60}, "research": modeLimit{s.cfg.ResearchPer10Min, 600}},
		"member_free_requests_per_minute":    0,
		"all_authenticated_requests_metered": true,
		"request_credit_costs":               requestCreditCosts(),
		"credits_bypass_caps":                false,
	}
}

func (s *Server) guestIntervals() map[string]int {
	base := int(s.cfg.GuestInterval.Seconds())
	intervals := make(map[string]int, 3)
	for mode, cost := range requestCreditCosts() {
		intervals[mode] = base * cost
	}
	return intervals
}

type modeLimit struct {
	Requests      int `json:"requests"`
	WindowSeconds int `json:"window_seconds"`
}

// Persist mode caps so a process restart cannot restore expensive work.
// All aliases and sessions for an account share a bucket. Guest identities are
// salted hashes; forwarded IPs are accepted only from configured proxies.
func (s *Server) allowRetrieval(w http.ResponseWriter, r *http.Request, u User, mode string) (func(bool), bool) {
	identity := "member:" + u.ID
	limit, window := s.cfg.SearchRPM, int64(60)
	switch mode {
	case "answer":
		limit = s.cfg.AnswerRPM
	case "research":
		limit, window = s.cfg.ResearchPer10Min, 600
	}
	if u.ID == "" {
		identity = "guest:" + s.guestKey(r)
		limit, window = 1, int64(s.guestIntervals()[mode])
	}
	now := time.Now().Unix()
	var until int64
	// Cleanup only expired entries; this never resets a live allowance.
	if _, err := s.db.ExecContext(r.Context(), `DELETE FROM retrieval_usage WHERE expires_at<=?`, now); err != nil {
		problem(w, 503, "request allowance unavailable")
		return nil, false
	}
	err := s.db.QueryRowContext(r.Context(), `INSERT INTO retrieval_usage(identity,mode,count,expires_at) VALUES(?,?,1,?) ON CONFLICT(identity,mode) DO UPDATE SET count=retrieval_usage.count+1 WHERE retrieval_usage.count<? RETURNING expires_at`, identity, mode, now+window, limit).Scan(&until)
	if err == nil {
		return func(success bool) {
			if success {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, _ = s.db.ExecContext(ctx, `UPDATE retrieval_usage SET count=count-1 WHERE identity=? AND mode=? AND expires_at=? AND count>0`, identity, mode, until)
		}, true
	}
	if !errors.Is(err, sql.ErrNoRows) {
		problem(w, 503, "request allowance unavailable")
		return nil, false
	}
	if err = s.db.QueryRowContext(r.Context(), `SELECT expires_at FROM retrieval_usage WHERE identity=? AND mode=?`, identity, mode).Scan(&until); err != nil {
		problem(w, 503, "request allowance unavailable")
		return nil, false
	}
	seconds := max(int64(1), until-now)
	w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	respond(w, 429, map[string]any{"error": fmt.Sprintf("%s limit reached (%d per %d seconds). Credits cannot exceed this limit.", mode, limit, window), "mode": mode, "retry_at": until, "retry_after_seconds": seconds})
	return nil, false
}

// Preserve the time of the previous successful guest request when changing
// policy, including upgrading databases created with the old 30-minute limit.
func (s *Server) migrateGuestInterval() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	old := int64(1800)
	err = tx.QueryRow(`SELECT value FROM settings WHERE key='guest_interval_seconds'`).Scan(&old)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	next := int64(s.cfg.GuestInterval.Seconds())
	if old != next {
		// Legacy reservations had no mode prefix and used one uniform interval.
		// New reservations retain their weight when an operator changes the base.
		if _, err = tx.Exec(`UPDATE guest_usage SET expires_at=expires_at+(?-?)*CASE WHEN reservation LIKE 'research:%' THEN 3 WHEN reservation LIKE 'answer:%' THEN 2 ELSE 1 END`, next, old); err != nil {
			return err
		}
		// The shared reservation is authoritative across guest modes. Align mode
		// caps with that expiry, including legacy uniform reservations. Orphaned
		// legacy mode rows expire instead of contradicting /api/guest status.
		if _, err = tx.Exec(`UPDATE retrieval_usage SET expires_at=COALESCE((SELECT expires_at FROM guest_usage WHERE ip_hash=substr(retrieval_usage.identity,7)),0) WHERE identity LIKE 'guest:%' AND mode IN ('search','answer','research')`); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`INSERT INTO settings(key,value) VALUES('guest_interval_seconds',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, next); err != nil {
		return err
	}
	return tx.Commit()
}
