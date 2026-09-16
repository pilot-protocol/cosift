package community

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	stripe "github.com/stripe/stripe-go/v86"
	"github.com/stripe/stripe-go/v86/webhook"
)

// A single prepaid pack; all amounts and quantities are server-owned integers.
const packAmountCents = 500
const packCredits = 50000

var checkoutKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)

func creditPack() map[string]any {
	return map[string]any{"amount_cents": packAmountCents, "currency": "usd", "credits": packCredits, "usd_per_1000_requests": "0.10"}
}
func (s *Server) paymentsEnabled() bool {
	key := s.cfg.StripeSecretKey
	return (strings.HasPrefix(key, "sk_test_") || strings.HasPrefix(key, "sk_live_") || strings.HasPrefix(key, "rk_test_") || strings.HasPrefix(key, "rk_live_")) && strings.HasPrefix(s.cfg.StripeWebhookSecret, "whsec_")
}
func (s *Server) stripeLive() bool { return strings.Contains(s.cfg.StripeSecretKey, "_live_") }

type checkoutOrder struct {
	ID, UserID, Currency, SessionID, CheckoutURL string
	Amount, Credits                              int64
	Created                                      int64
}

func (s *Server) checkout(w http.ResponseWriter, r *http.Request, u User) {
	if !s.paymentsEnabled() {
		problem(w, 503, "credit purchases are not configured yet")
		return
	}
	var in struct {
		IdempotencyKey string `json:"idempotency_key"`
	}
	if decode(r, &in) != nil || !checkoutKeyPattern.MatchString(in.IdempotencyKey) {
		problem(w, 400, "a checkout idempotency key of 16–64 letters, digits, underscores or hyphens is required")
		return
	}
	if !s.allow("checkout:"+u.ID, 10, time.Minute) {
		w.Header().Set("Retry-After", "60")
		problem(w, 429, "too many checkout attempts; try again in a minute")
		return
	}
	id := "checkout:" + tokenHash(u.ID+":"+in.IdempotencyKey)
	_, err := s.db.ExecContext(r.Context(), `INSERT INTO payment_checkouts(id,user_id,amount_cents,credits,currency,created_at) VALUES(?,?,?,?,'usd',?) ON CONFLICT(id) DO NOTHING`, id, u.ID, packAmountCents, packCredits, time.Now().Unix())
	if err != nil {
		problem(w, 500, "could not prepare checkout")
		return
	}
	var order checkoutOrder
	err = s.db.QueryRowContext(r.Context(), `SELECT id,user_id,amount_cents,credits,currency,COALESCE(session_id,''),checkout_url,created_at FROM payment_checkouts WHERE id=?`, id).Scan(&order.ID, &order.UserID, &order.Amount, &order.Credits, &order.Currency, &order.SessionID, &order.CheckoutURL, &order.Created)
	if err != nil {
		problem(w, 500, "could not load checkout")
		return
	}
	if time.Now().Unix()-order.Created > 23*3600 {
		problem(w, 409, "this checkout has expired; refresh the page to start a new purchase")
		return
	}
	if order.SessionID != "" && order.CheckoutURL != "" {
		respond(w, 200, map[string]string{"url": order.CheckoutURL})
		return
	}
	form := url.Values{
		"mode": {"payment"}, "payment_method_types[0]": {"card"},
		"adaptive_pricing[enabled]":                      {"false"},
		"payment_intent_data[metadata][cosift_order_id]": {id},
		"client_reference_id":                            {u.ID}, "metadata[cosift_order_id]": {id},
		"line_items[0][price_data][currency]":                  {order.Currency},
		"line_items[0][price_data][unit_amount]":               {strconv.FormatInt(order.Amount, 10)},
		"line_items[0][price_data][product_data][name]":        {"Cosift prepaid credits"},
		"line_items[0][price_data][product_data][description]": {fmt.Sprintf("%d credits; one per extra request within account rate limits. One-time purchase.", order.Credits)},
		"line_items[0][quantity]":                              {"1"},
		"success_url":                                          {s.cfg.PublicURL + "/?payment=success"}, "cancel_url": {s.cfg.PublicURL + "/?payment=cancelled"},
	}
	req, _ := http.NewRequestWithContext(r.Context(), "POST", "https://api.stripe.com/v1/checkout/sessions", strings.NewReader(form.Encode()))
	req.SetBasicAuth(s.cfg.StripeSecretKey, "")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Idempotency-Key", id)
	req.Header.Set("Stripe-Version", stripe.APIVersion)
	res, err := s.paymentClient.Do(req)
	if err != nil {
		problem(w, 502, "Stripe checkout is unavailable; retry this purchase")
		return
	}
	defer res.Body.Close()
	var session struct {
		ID       string `json:"id"`
		URL      string `json:"url"`
		LiveMode bool   `json:"livemode"`
	}
	if res.StatusCode != 200 || json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&session) != nil || !strings.HasPrefix(session.ID, "cs_") || session.LiveMode != s.stripeLive() || !validCheckoutURL(session.URL) {
		problem(w, 502, "Stripe returned no valid checkout; retry this purchase")
		return
	}
	// A webhook may win this race. Never replace an order's existing session.
	result, err := s.db.ExecContext(r.Context(), `UPDATE payment_checkouts SET session_id=?,checkout_url=? WHERE id=? AND (session_id IS NULL OR session_id=?)`, session.ID, session.URL, id, session.ID)
	if err != nil {
		problem(w, 500, "could not save checkout; retry this purchase")
		return
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		problem(w, 409, "checkout session mismatch")
		return
	}
	respond(w, 200, map[string]string{"url": session.URL})
}
func validCheckoutURL(raw string) bool {
	u, e := url.Parse(raw)
	return e == nil && u.Scheme == "https" && u.Host == "checkout.stripe.com" && u.User == nil
}

type stripeEvent struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	LiveMode bool   `json:"livemode"`
	Data     struct {
		Object json.RawMessage `json:"object"`
	} `json:"data"`
}

func (s *Server) stripeWebhook(w http.ResponseWriter, r *http.Request) {
	if !s.paymentsEnabled() {
		problem(w, 503, "payments are not configured")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 256<<10))
	if err != nil || webhook.ValidatePayload(body, r.Header.Get("Stripe-Signature"), s.cfg.StripeWebhookSecret) != nil {
		problem(w, 400, "invalid Stripe signature or payload")
		return
	}
	var event stripeEvent
	if json.Unmarshal(body, &event) != nil || event.ID == "" || event.LiveMode != s.stripeLive() {
		problem(w, 400, "invalid Stripe event")
		return
	}
	switch event.Type {
	case "checkout.session.completed", "checkout.session.async_payment_succeeded":
		err = s.fulfillCheckout(r.Context(), event)
	case "charge.refunded":
		err = s.refundCheckout(r.Context(), event)
	default:
		respond(w, 200, map[string]bool{"received": true})
		return
	}
	if err != nil {
		// Return a retryable failure for unknown/mismatched orders as well as DB
		// failures. Never silently discard a paid order or log payment details.
		problem(w, 500, "payment could not be reconciled; Stripe should retry")
		return
	}
	respond(w, 200, map[string]bool{"received": true})
}

func (s *Server) fulfillCheckout(ctx context.Context, event stripeEvent) error {
	var session struct {
		ID            string            `json:"id"`
		Mode          string            `json:"mode"`
		Status        string            `json:"status"`
		PaymentStatus string            `json:"payment_status"`
		Currency      string            `json:"currency"`
		Amount        int64             `json:"amount_total"`
		UserID        string            `json:"client_reference_id"`
		PaymentIntent string            `json:"payment_intent"`
		LiveMode      bool              `json:"livemode"`
		Metadata      map[string]string `json:"metadata"`
	}
	if err := json.Unmarshal(event.Data.Object, &session); err != nil {
		return err
	}
	if session.Metadata["cosift_order_id"] == "" {
		return nil
	}
	if session.PaymentStatus != "paid" {
		return nil
	}
	if session.Mode != "payment" || session.Status != "complete" || !strings.HasPrefix(session.ID, "cs_") || !strings.HasPrefix(session.PaymentIntent, "pi_") || session.LiveMode != s.stripeLive() {
		return errors.New("invalid paid session")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var order checkoutOrder
	err = tx.QueryRowContext(ctx, `SELECT id,user_id,amount_cents,credits,currency,COALESCE(session_id,'') FROM payment_checkouts WHERE id=?`, session.Metadata["cosift_order_id"]).Scan(&order.ID, &order.UserID, &order.Amount, &order.Credits, &order.Currency, &order.SessionID)
	if err != nil {
		return err
	}
	if session.UserID != order.UserID || session.Amount != order.Amount || session.Currency != order.Currency || (order.SessionID != "" && order.SessionID != session.ID) {
		return errors.New("purchase does not match order")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE payment_checkouts SET session_id=?,payment_intent=? WHERE id=?`, session.ID, session.PaymentIntent, order.ID); err != nil {
		return err
	}
	// Session ID deduplication covers separate event IDs and concurrent delivery.
	if _, err = tx.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,?,'stripe_purchase',?) ON CONFLICT(id) DO NOTHING`, "stripe:"+session.ID, order.UserID, order.Credits, time.Now().Unix()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO payment_events(provider,event_id,user_id,credits,created_at) VALUES('stripe',?,?,?,?) ON CONFLICT(provider,event_id) DO NOTHING`, event.ID, order.UserID, order.Credits, time.Now().Unix()); err != nil {
		return err
	}
	return tx.Commit()
}

// Refunds are initiated in Stripe's dashboard. Reconcile their cumulative
// amount, so duplicate or out-of-order notifications cannot revoke twice.
func (s *Server) refundCheckout(ctx context.Context, event stripeEvent) error {
	var charge struct {
		Metadata      map[string]string `json:"metadata"`
		PaymentIntent string            `json:"payment_intent"`
		Amount        int64             `json:"amount"`
		Refunded      int64             `json:"amount_refunded"`
		Currency      string            `json:"currency"`
		LiveMode      bool              `json:"livemode"`
	}
	if err := json.Unmarshal(event.Data.Object, &charge); err != nil {
		return err
	}
	if charge.Metadata["cosift_order_id"] == "" {
		return nil
	}
	if charge.LiveMode != s.stripeLive() || charge.PaymentIntent == "" || charge.Refunded < 0 || charge.Refunded > charge.Amount {
		return errors.New("invalid refund")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id, user, currency string
	var amount, credits, previous int64
	err = tx.QueryRowContext(ctx, `SELECT id,user_id,amount_cents,credits,currency,refunded_cents FROM payment_checkouts WHERE id=? AND payment_intent=?`, charge.Metadata["cosift_order_id"], charge.PaymentIntent).Scan(&id, &user, &amount, &credits, &currency, &previous)
	if err != nil {
		return err
	}
	if amount <= 0 || charge.Amount != amount || currency != charge.Currency {
		return errors.New("refund does not match purchase")
	}
	if charge.Refunded <= previous {
		return nil
	}
	revoke := credits*charge.Refunded/amount - credits*previous/amount
	if _, err = tx.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,?,'stripe_refund',?)`, fmt.Sprintf("stripe-refund:%s:%d", id, charge.Refunded), user, -revoke, time.Now().Unix()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE payment_checkouts SET refunded_cents=? WHERE id=?`, charge.Refunded, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO payment_events(provider,event_id,user_id,credits,created_at) VALUES('stripe',?,?,?,?) ON CONFLICT(provider,event_id) DO NOTHING`, event.ID, user, -revoke, time.Now().Unix()); err != nil {
		return err
	}
	return tx.Commit()
}
