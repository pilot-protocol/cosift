package community

import (
	"context"
	"net/http"
	"time"
)

const contributionReward = 10

func (s *Server) credits(w http.ResponseWriter, r *http.Request, u User) {
	var balance int
	if err := s.db.QueryRowContext(r.Context(), `SELECT COALESCE(sum(delta),0) FROM credit_ledger WHERE user_id=?`, u.ID).Scan(&balance); err != nil {
		problem(w, 500, "credits unavailable")
		return
	}
	respond(w, 200, map[string]any{"balance": balance, "free_requests_per_minute": s.cfg.MemberFreeRPM, "limits": s.limitPolicy(), "extra_request_cost": 1, "verified_contribution_reward": contributionReward, "payments_enabled": s.paymentsEnabled(), "credit_pack": creditPack()})
}

// reserveCredit performs a conditional debit atomically. Refunds have an
// idempotency key derived from the debit, so a retry cannot mint credits.
func (s *Server) reserveCredit(w http.ResponseWriter, r *http.Request, u User) (func(bool), bool) {
	id := "request:" + randomID()
	res, err := s.db.ExecContext(r.Context(), `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at)
SELECT ?,?,-1,'extra_request',? WHERE (SELECT COALESCE(sum(delta),0) FROM credit_ledger WHERE user_id=?)>=1`, id, u.ID, time.Now().Unix(), u.ID)
	if err != nil {
		problem(w, 500, "credits unavailable")
		return nil, false
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		w.Header().Set("Retry-After", "60")
		message := "free request limit reached; contribute verified new webpages to earn credits, or try again in a minute"
		if s.paymentsEnabled() {
			message = "free request limit reached; buy credits in the web app, contribute verified webpages, or try again in a minute"
		}
		problem(w, 429, message)
		return nil, false
	}
	return func(success bool) {
		if success {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		// Removing the reservation restores the balance even if writing a new
		// refund record would be interrupted; repeated calls are harmless.
		_, _ = s.db.ExecContext(ctx, `DELETE FROM credit_ledger WHERE id=? AND delta=-1`, id)
	}, true
}

func (s *Server) rewardContribution(ctx context.Context, submissionID, contentHash string) error {
	// Global content uniqueness prevents the same page, mirrored URLs, retries,
	// and separate accounts from receiving the same reward twice.
	_, err := s.db.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at)
SELECT ?,user_id,?,'verified_contribution',? FROM submissions WHERE id=? AND user_id IS NOT NULL
ON CONFLICT(id) DO NOTHING`, "contribution:"+contentHash, contributionReward, time.Now().Unix(), submissionID)
	return err
}
