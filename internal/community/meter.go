package community

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const articleReadReason = "article_read"

var (
	articleRefPattern = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	requestIDPattern  = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)
)

func problemCode(w http.ResponseWriter, status int, msg, code string, extra map[string]any) {
	body := map[string]any{"error": msg, "code": code}
	for k, v := range extra {
		body[k] = v
	}
	respond(w, status, body)
}

// meter charges one covered article read. It has no refund path: a charge, once written, stays.
func (s *Server) meter(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Shared == nil {
		problemCode(w, http.StatusNotFound, "metering is not enabled", "not_found", nil)
		return
	}
	if strings.TrimSpace(r.Header.Get("Authorization")) == "" {
		problemCode(w, http.StatusUnauthorized, "a bearer token is required", "bearer_required", nil)
		return
	}
	s.sharedAuth(w, r, s.chargeArticleRead, false)
}

func (s *Server) chargeArticleRead(w http.ResponseWriter, r *http.Request, u User) {
	if !s.allowRoute(w, "meter", u.ID, meterPerMinute) {
		return
	}
	var in struct {
		Kind      string `json:"kind"`
		Ref       string `json:"ref"`
		RequestID string `json:"request_id"`
	}
	if decode(r, &in) != nil {
		problemCode(w, http.StatusBadRequest, "invalid request body", "invalid_body", nil)
		return
	}
	cost, ok := meterCreditCosts()[in.Kind]
	switch {
	case !ok:
		problemCode(w, http.StatusUnprocessableEntity, "unsupported kind", "unsupported_kind", nil)
		return
	case !articleRefPattern.MatchString(in.Ref):
		problemCode(w, http.StatusBadRequest, "invalid ref", "invalid_ref", nil)
		return
	case !requestIDPattern.MatchString(in.RequestID):
		problemCode(w, http.StatusBadRequest, "invalid request_id", "invalid_request_id", nil)
		return
	}
	ctx := r.Context()
	charged, replayed, balance, err := s.debitMeter(ctx, u.ID, in.RequestID, cost)
	if err != nil {
		log.Printf("community: credits_unavailable")
		problemCode(w, http.StatusInternalServerError, "credits unavailable", "credits_unavailable", nil)
		return
	}
	if !charged && !replayed {
		problemCode(w, http.StatusPaymentRequired, "insufficient credits", "insufficient_credits", map[string]any{"credits_required": cost, "balance": balance, "topup_url": s.cfg.PublicURL + "/"})
		return
	}
	respond(w, http.StatusOK, map[string]any{"charged": cost, "balance": balance, "request_id": in.RequestID, "replayed": replayed})
}

// debitMeter is one conditional statement keyed by the caller's request id; zero rows is a replay or too few credits.
func (s *Server) debitMeter(ctx context.Context, userID, requestID string, cost int) (charged, replayed bool, balance int, err error) {
	if err = s.grantMonthlyCredits(ctx, userID, time.Now()); err != nil {
		return
	}
	id := "meter:" + userID + ":" + requestID
	res, err := s.db.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at)
SELECT ?,?,?,?,? WHERE (SELECT COALESCE(sum(delta),0) FROM credit_ledger WHERE user_id=?)>=? ON CONFLICT(id) DO NOTHING`,
		id, userID, -cost, articleReadReason, time.Now().Unix(), userID, cost)
	if err != nil {
		return
	}
	n, _ := res.RowsAffected()
	charged = n == 1
	if !charged {
		var found string
		switch e := s.db.QueryRowContext(ctx, `SELECT id FROM credit_ledger WHERE id=? AND user_id=?`, id, userID).Scan(&found); {
		case e == nil:
			replayed = true
		case !errors.Is(e, sql.ErrNoRows):
			err = e
			return
		}
	}
	err = s.db.QueryRowContext(ctx, `SELECT COALESCE(sum(delta),0) FROM credit_ledger WHERE user_id=?`, userID).Scan(&balance)
	return
}
