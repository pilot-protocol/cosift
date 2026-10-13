package community

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

const contributionReward = 10
const defaultContributionRewardDailyCap = 10
const monthlyFreeCredits = 1000

func requestCreditCosts() map[string]int {
	return map[string]int{"search": 1, "answer": 2, "research": 3}
}

// meterCreditCosts prices what the meter debits; it never feeds guest intervals, pack prices or retrieval caps.
func meterCreditCosts() map[string]int { return map[string]int{"article": 2} }

// reportedCreditCosts is what the API shows; nothing that enforces a limit iterates it.
func reportedCreditCosts() map[string]int {
	out := requestCreditCosts()
	for kind, cost := range meterCreditCosts() {
		out[kind] = cost
	}
	return out
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
COALESCE(sum(CASE WHEN created_at>=? AND created_at<? AND reason IN ('verified_contribution','contribution_award','royalty','fetch_verified') THEN delta ELSE 0 END),0),
COALESCE(sum(CASE WHEN created_at>=? AND created_at<? AND reason='stripe_purchase' THEN delta ELSE 0 END),0),
COALESCE(sum(CASE WHEN created_at>=? AND created_at<? AND reason IN ('extra_request','article_read','clawback') THEN -delta ELSE 0 END),0)
FROM credit_ledger WHERE user_id=?`, start.Unix(), end.Unix(), start.Unix(), end.Unix(), start.Unix(), end.Unix(), start.Unix(), end.Unix(), u.ID).Scan(&balance, &free, &earned, &purchased, &spent); err != nil {
		problem(w, 500, "credits unavailable")
		return
	}
	billing, err := s.subscriptionFields(r.Context(), u.ID)
	if err != nil {
		problem(w, 503, "billing status unavailable")
		return
	}
	out := map[string]any{"balance": balance, "monthly_free_credits": monthlyFreeCredits, "monthly": map[string]any{"month": start.Format("2006-01"), "starts_at": start.Format(time.RFC3339), "timezone": "UTC", "free": free, "earned": earned, "purchased": purchased, "spent": spent}, "free_requests_per_minute": 0, "all_authenticated_requests_metered": true, "limits": s.limitPolicy(), "extra_request_cost": 1, "request_credit_costs": reportedCreditCosts(), "payments_enabled": s.paymentsEnabled(), "payment_mode": s.paymentMode(), "credit_pack": creditPack()}
	if s.cfg.ContributionReward {
		out["verified_contribution_reward"] = contributionReward
		out["verified_contribution_daily_cap"] = s.cfg.ContributionRewardDailyCap
	}
	for key, value := range billing {
		out[key] = value
	}
	respond(w, 200, out)
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
		var ways []string
		if s.paymentsEnabled() {
			ways = append(ways, "add credits in Billing")
		}
		if s.cfg.ContributionReward {
			ways = append(ways, "contribute verified new webpages")
		}
		ways = append(ways, "use your next monthly grant")
		problem(w, 429, fmt.Sprintf("insufficient credits: %s requires %d credits; %s", mode, cost, strings.Join(ways, " or ")))
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

// rewardContribution mints the reward only behind the switch, for a complete content index, and under the account's rolling daily cap.
func (s *Server) rewardContribution(ctx context.Context, submissionID, contentHash string, indexComplete bool) error {
	if !s.cfg.ContributionReward {
		return nil
	}
	if !indexComplete {
		log.Printf("community: contribution reward withheld: the backend's content index is incomplete")
		return nil
	}
	// Global content uniqueness prevents the same page, mirrored URLs, retries,
	// and separate accounts from receiving the same reward twice.
	now := time.Now()
	_, err := s.db.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at)
SELECT ?,user_id,?,'verified_contribution',? FROM submissions WHERE id=? AND user_id IS NOT NULL
AND (SELECT count(*) FROM credit_ledger WHERE user_id=submissions.user_id AND reason='verified_contribution' AND created_at>?)<?
ON CONFLICT(id) DO NOTHING`, "contribution:"+contentHash, contributionReward, now.Unix(), submissionID, now.Add(-24*time.Hour).Unix(), s.cfg.ContributionRewardDailyCap)
	return err
}
