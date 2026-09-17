package community

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

const contributionReward = 10
const monthlyFreeCredits = 1000

func requestCreditCosts() map[string]int {
	return map[string]int{"search": 1, "answer": 2, "research": 3}
}

// The ledger key makes the monthly grant atomic across processes and channels.
func (s *Server) grantMonthlyCredits(ctx context.Context, userID string, now time.Time) error {
	if userID == "" {
		return errors.New("monthly credits require an account")
	}
	month := now.UTC().Format("2006-01")
	_, err := s.db.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) SELECT ?,id,?,'monthly_free',? FROM users WHERE id=? ON CONFLICT(id) DO NOTHING`, "monthly-free:"+userID+":"+month, monthlyFreeCredits, now.Unix(), userID)
	return err
}

func (s *Server) credits(w http.ResponseWriter, r *http.Request, u User) {
	var balance int
	var free, earned, purchased, spent int
	now := time.Now().UTC()
	if err := s.grantMonthlyCredits(r.Context(), u.ID, now); err != nil {
		problem(w, 500, "credits unavailable")
		return
	}
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	if err := s.db.QueryRowContext(r.Context(), `SELECT COALESCE(sum(delta),0),
COALESCE(sum(CASE WHEN created_at>=? AND created_at<? AND reason='monthly_free' THEN delta ELSE 0 END),0),
COALESCE(sum(CASE WHEN created_at>=? AND created_at<? AND reason='verified_contribution' THEN delta ELSE 0 END),0),
COALESCE(sum(CASE WHEN created_at>=? AND created_at<? AND reason='stripe_purchase' THEN delta ELSE 0 END),0),
COALESCE(sum(CASE WHEN created_at>=? AND created_at<? AND reason='extra_request' THEN -delta ELSE 0 END),0)
FROM credit_ledger WHERE user_id=?`, start.Unix(), end.Unix(), start.Unix(), end.Unix(), start.Unix(), end.Unix(), start.Unix(), end.Unix(), u.ID).Scan(&balance, &free, &earned, &purchased, &spent); err != nil {
		problem(w, 500, "credits unavailable")
		return
	}
	respond(w, 200, map[string]any{"balance": balance, "monthly_free_credits": monthlyFreeCredits, "monthly": map[string]any{"month": start.Format("2006-01"), "starts_at": start.Format(time.RFC3339), "timezone": "UTC", "free": free, "earned": earned, "purchased": purchased, "spent": spent}, "free_requests_per_minute": s.cfg.MemberFreeRPM, "limits": s.limitPolicy(), "extra_request_cost": 1, "request_credit_costs": requestCreditCosts(), "verified_contribution_reward": contributionReward, "payments_enabled": s.paymentsEnabled(), "payment_mode": s.paymentMode(), "credit_pack": creditPack()})
}

// reserveCredit performs a conditional debit atomically. Refunds have an
// idempotency key derived from the debit, so a retry cannot mint credits.
func (s *Server) reserveCredit(w http.ResponseWriter, r *http.Request, u User, mode string) (func(bool), bool) {
	cost := requestCreditCosts()[mode]
	if cost == 0 {
		problem(w, 400, "mode must be search, answer or research")
		return nil, false
	}
	if err := s.grantMonthlyCredits(r.Context(), u.ID, time.Now()); err != nil {
		problem(w, 500, "credits unavailable")
		return nil, false
	}
	id := "request:" + randomID()
	res, err := s.db.ExecContext(r.Context(), `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at)
SELECT ?,?,-?,'extra_request',? WHERE (SELECT COALESCE(sum(delta),0) FROM credit_ledger WHERE user_id=?)>=?`, id, u.ID, cost, time.Now().Unix(), u.ID, cost)
	if err != nil {
		problem(w, 500, "credits unavailable")
		return nil, false
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		w.Header().Set("Retry-After", "60")
		message := fmt.Sprintf("free request limit reached; %s requires %d credits; contribute verified new webpages or try again in a minute", mode, cost)
		if s.paymentsEnabled() {
			message = fmt.Sprintf("free request limit reached; %s requires %d credits; buy credits in the web app, contribute verified webpages, or try again in a minute", mode, cost)
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
		_, _ = s.db.ExecContext(ctx, `DELETE FROM credit_ledger WHERE id=? AND user_id=? AND delta=?`, id, u.ID, -cost)
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
