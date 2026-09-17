package community

import (
	"context"
	"database/sql"
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
)

var stripeIDPattern = regexp.MustCompile(`^[a-z]+_[A-Za-z0-9_]+$`)
var errSubscriptionRequired = errors.New("an active paid subscription is required for top-ups")

func subscriptionSchema(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS subscription_orders (
id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),livemode INTEGER NOT NULL,
amount_cents INTEGER NOT NULL,credits INTEGER NOT NULL,currency TEXT NOT NULL,created_at INTEGER NOT NULL,
session_id TEXT UNIQUE,checkout_url TEXT NOT NULL DEFAULT '',subscription_id TEXT UNIQUE,customer_id TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS subscription_checkout_pending (
user_id TEXT NOT NULL REFERENCES users(id),livemode INTEGER NOT NULL,order_id TEXT NOT NULL REFERENCES subscription_orders(id),PRIMARY KEY(user_id,livemode));
CREATE TABLE IF NOT EXISTS billing_subscriptions (
id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),order_id TEXT NOT NULL UNIQUE REFERENCES subscription_orders(id),
customer_id TEXT NOT NULL,livemode INTEGER NOT NULL,status TEXT NOT NULL,cancel_at_period_end INTEGER NOT NULL,
current_period_end INTEGER NOT NULL,paid_until INTEGER NOT NULL DEFAULT 0,price_id TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS subscriptions_user ON billing_subscriptions(user_id,livemode,created_at);
CREATE TABLE IF NOT EXISTS subscription_invoices (
id TEXT PRIMARY KEY,subscription_id TEXT NOT NULL REFERENCES billing_subscriptions(id),user_id TEXT NOT NULL REFERENCES users(id),
amount_cents INTEGER NOT NULL,credits INTEGER NOT NULL,currency TEXT NOT NULL,period_end INTEGER NOT NULL,
refunded_cents INTEGER NOT NULL DEFAULT 0,livemode INTEGER NOT NULL);`)
	return err
}

type billingSubscription struct {
	ID, UserID, OrderID, CustomerID, Status, PriceID string
	LiveMode, CancelAtPeriodEnd                      bool
	CurrentPeriodEnd, PaidUntil, Created             int64
}

func (b billingSubscription) active() bool {
	return b.Status == "active" && b.PaidUntil > time.Now().Unix()
}

func subscriptionPlan() map[string]any {
	return map[string]any{"amount_cents": packAmountCents, "credits": packCredits, "currency": "usd", "interval": "month"}
}

func (s *Server) accountSubscription(ctx context.Context, userID string) (billingSubscription, error) {
	var b billingSubscription
	err := s.db.QueryRowContext(ctx, `SELECT id,user_id,order_id,customer_id,livemode,status,cancel_at_period_end,current_period_end,paid_until,price_id,created_at
FROM billing_subscriptions WHERE user_id=? AND livemode=? ORDER BY CASE WHEN status IN ('canceled','incomplete_expired') THEN 1 ELSE 0 END,created_at DESC,id DESC LIMIT 1`, userID, s.stripeLive()).Scan(&b.ID, &b.UserID, &b.OrderID, &b.CustomerID, &b.LiveMode, &b.Status, &b.CancelAtPeriodEnd, &b.CurrentPeriodEnd, &b.PaidUntil, &b.PriceID, &b.Created)
	if errors.Is(err, sql.ErrNoRows) {
		return billingSubscription{Status: "none"}, nil
	}
	return b, err
}

func (s *Server) subscriptionFields(ctx context.Context, userID string) (map[string]any, error) {
	b, err := s.accountSubscription(ctx, userID)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"subscription":      map[string]any{"status": b.Status, "active": b.active(), "cancel_at_period_end": b.CancelAtPeriodEnd, "current_period_end": b.CurrentPeriodEnd},
		"can_top_up":        s.paymentsEnabled() && b.active(),
		"portal_available":  s.paymentsEnabled() && b.CustomerID != "" && strings.HasPrefix(s.cfg.StripePortalConfigurationID, "bpc_"),
		"subscription_plan": subscriptionPlan(),
	}, nil
}

// Basil-or-newer snapshot events are routed by stable IDs, then objects are
// retrieved at the installed SDK API version. No Stripe keys are logged.
func (s *Server) stripeRequest(ctx context.Context, method, path string, form url.Values, idempotency string, out any) error {
	if !strings.HasPrefix(path, "/v1/") {
		return errors.New("invalid Stripe API path")
	}
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, "https://api.stripe.com"+path, body)
	if err != nil {
		return err
	}
	req.SetBasicAuth(s.cfg.StripeSecretKey, "")
	req.Header.Set("Stripe-Version", stripe.APIVersion)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if idempotency != "" {
		req.Header.Set("Idempotency-Key", idempotency)
	}
	res, err := s.paymentClient.Do(req)
	if err != nil {
		return errors.New("Stripe API unavailable")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("Stripe API returned HTTP %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return errors.New("Stripe response unavailable")
	}
	if json.Unmarshal(data, out) != nil {
		return errors.New("invalid Stripe response")
	}
	return nil
}

func (s *Server) requireTopup(ctx context.Context, userID string) (billingSubscription, error) {
	b, err := s.accountSubscription(ctx, userID)
	if err != nil {
		return b, err
	}
	if b.ID == "" {
		return b, errSubscriptionRequired
	}
	current, err := s.syncSubscription(ctx, b.ID)
	if err != nil {
		return b, err
	}
	if current == nil || !current.active() {
		return b, errSubscriptionRequired
	}
	return *current, nil
}

func (s *Server) checkoutSubscription(w http.ResponseWriter, r *http.Request, u User, key string) {
	b, err := s.accountSubscription(r.Context(), u.ID)
	if err != nil {
		problem(w, 503, "subscription unavailable")
		return
	}
	if b.ID != "" {
		current, e := s.syncSubscription(r.Context(), b.ID)
		if e != nil {
			problem(w, 502, "could not verify subscription; retry")
			return
		}
		if current != nil && current.Status != "canceled" && current.Status != "incomplete_expired" {
			problem(w, 409, "a subscription already exists; manage it in billing")
			return
		}
	}
	id := "subscription:" + tokenHash(strconv.FormatBool(s.stripeLive())+":"+u.ID+":"+key)
	now := time.Now().Unix()
	tx, err := s.db.BeginTx(r.Context(), nil)
	if err != nil {
		problem(w, 500, "could not prepare subscription")
		return
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(r.Context(), `DELETE FROM subscription_checkout_pending WHERE user_id=? AND livemode=? AND order_id IN (SELECT id FROM subscription_orders WHERE created_at<=?)`, u.ID, s.stripeLive(), now-23*3600)
	if err == nil {
		_, err = tx.ExecContext(r.Context(), `INSERT INTO subscription_orders(id,user_id,livemode,amount_cents,credits,currency,created_at) VALUES(?,?,?,?,?,'usd',?) ON CONFLICT(id) DO NOTHING`, id, u.ID, s.stripeLive(), packAmountCents, packCredits, now)
	}
	if err == nil {
		_, err = tx.ExecContext(r.Context(), `INSERT INTO subscription_checkout_pending(user_id,livemode,order_id) VALUES(?,?,?) ON CONFLICT(user_id,livemode) DO NOTHING`, u.ID, s.stripeLive(), id)
	}
	var created int64
	var sessionID, checkoutURL string
	if err == nil {
		err = tx.QueryRowContext(r.Context(), `SELECT o.id,o.created_at,COALESCE(o.session_id,''),o.checkout_url FROM subscription_checkout_pending p JOIN subscription_orders o ON o.id=p.order_id WHERE p.user_id=? AND p.livemode=?`, u.ID, s.stripeLive()).Scan(&id, &created, &sessionID, &checkoutURL)
	}
	if err != nil || tx.Commit() != nil {
		problem(w, 500, "could not prepare subscription")
		return
	}
	if now-created >= 23*3600 {
		problem(w, 409, "this checkout expired; start a new subscription checkout")
		return
	}
	if sessionID != "" && checkoutURL != "" {
		respond(w, 200, map[string]string{"url": checkoutURL})
		return
	}
	form := url.Values{
		"mode": {"subscription"}, "payment_method_types[0]": {"card"}, "adaptive_pricing[enabled]": {"false"},
		"client_reference_id": {u.ID}, "metadata[cosift_order_id]": {id}, "subscription_data[metadata][cosift_order_id]": {id},
		"line_items[0][price_data][currency]": {"usd"}, "line_items[0][price_data][unit_amount]": {"500"}, "line_items[0][price_data][recurring][interval]": {"month"}, "line_items[0][price_data][recurring][interval_count]": {"1"},
		"line_items[0][price_data][product_data][name]": {"Cosift monthly credits"}, "line_items[0][price_data][product_data][description]": {"50,000 credits per paid month. Unused credits carry over. Search 1, Answer 2, Research 3 credits per request."}, "line_items[0][quantity]": {"1"},
		"expires_at": {strconv.FormatInt(created+23*3600, 10)}, "success_url": {s.cfg.PublicURL + "/?payment=success"}, "cancel_url": {s.cfg.PublicURL + "/?payment=cancelled"},
	}
	var session stripe.CheckoutSession
	if s.stripeRequest(r.Context(), "POST", "/v1/checkout/sessions", form, id, &session) != nil || !strings.HasPrefix(session.ID, "cs_") || session.Livemode != s.stripeLive() || !validCheckoutURL(session.URL) {
		problem(w, 502, "subscription checkout unavailable; retry this purchase")
		return
	}
	res, err := s.db.ExecContext(r.Context(), `UPDATE subscription_orders SET session_id=?,checkout_url=? WHERE id=? AND (session_id IS NULL OR session_id=?)`, session.ID, session.URL, id, session.ID)
	if err != nil {
		problem(w, 500, "could not save checkout; retry")
		return
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		problem(w, 409, "checkout session mismatch")
		return
	}
	respond(w, 200, map[string]string{"url": session.URL})
}

func (s *Server) portal(w http.ResponseWriter, r *http.Request, u User) {
	if !s.paymentsEnabled() || !strings.HasPrefix(s.cfg.StripePortalConfigurationID, "bpc_") {
		problem(w, 503, "billing management is not configured")
		return
	}
	var in struct{}
	if decode(r, &in) != nil {
		problem(w, 400, "expected an empty billing request")
		return
	}
	if !s.allow("portal:"+u.ID, 10, time.Minute) {
		problem(w, 429, "too many billing requests")
		return
	}
	b, err := s.accountSubscription(r.Context(), u.ID)
	if err != nil || b.CustomerID == "" {
		problem(w, 409, "no subscription to manage")
		return
	}
	var session struct {
		URL string `json:"url"`
	}
	form := url.Values{"customer": {b.CustomerID}, "configuration": {s.cfg.StripePortalConfigurationID}, "return_url": {s.cfg.PublicURL + "/?payment=manage"}}
	if s.stripeRequest(r.Context(), "POST", "/v1/billing_portal/sessions", form, "", &session) != nil {
		problem(w, 502, "billing portal unavailable")
		return
	}
	parsed, err := url.Parse(session.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "billing.stripe.com" || parsed.User != nil {
		problem(w, 502, "billing portal unavailable")
		return
	}
	respond(w, 200, map[string]string{"url": session.URL})
}

func (s *Server) syncSubscription(ctx context.Context, id string) (*billingSubscription, error) {
	s.billingMu.Lock()
	defer s.billingMu.Unlock()
	if !strings.HasPrefix(id, "sub_") || !stripeIDPattern.MatchString(id) {
		return nil, errors.New("invalid subscription id")
	}
	var sub stripe.Subscription
	if err := s.stripeRequest(ctx, "GET", "/v1/subscriptions/"+id, nil, "", &sub); err != nil {
		return nil, err
	}
	if sub.ID != id || sub.Livemode != s.stripeLive() {
		return nil, errors.New("subscription environment mismatch")
	}
	orderID := sub.Metadata["cosift_order_id"]
	if orderID == "" {
		var known int
		err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM billing_subscriptions WHERE id=?`, id).Scan(&known)
		if err != nil {
			return nil, err
		}
		if known == 0 {
			return nil, nil
		}
		return nil, errors.New("subscription lost its order binding")
	}
	var user, existingSub, existingCustomer string
	var live bool
	var amount, credits, created int64
	var currency string
	err := s.db.QueryRowContext(ctx, `SELECT user_id,livemode,amount_cents,credits,currency,created_at,COALESCE(subscription_id,''),customer_id FROM subscription_orders WHERE id=?`, orderID).Scan(&user, &live, &amount, &credits, &currency, &created, &existingSub, &existingCustomer)
	if err != nil {
		return nil, err
	}
	if live != s.stripeLive() || sub.Customer == nil || sub.Customer.ID == "" || (existingSub != "" && existingSub != id) || (existingCustomer != "" && existingCustomer != sub.Customer.ID) || sub.Items == nil || sub.Items.HasMore || len(sub.Items.Data) != 1 {
		return nil, errors.New("subscription does not match order")
	}
	item := sub.Items.Data[0]
	if item == nil || item.ID == "" || item.Quantity != 1 || item.Price == nil || !strings.HasPrefix(item.Price.ID, "price_") || item.Price.UnitAmount != amount || string(item.Price.Currency) != currency || item.Price.Recurring == nil || string(item.Price.Recurring.Interval) != "month" || item.Price.Recurring.IntervalCount != 1 || item.CurrentPeriodEnd <= 0 {
		return nil, errors.New("subscription price does not match monthly plan")
	}
	b := billingSubscription{ID: id, UserID: user, OrderID: orderID, CustomerID: sub.Customer.ID, LiveMode: live, Status: string(sub.Status), CancelAtPeriodEnd: sub.CancelAtPeriodEnd, CurrentPeriodEnd: item.CurrentPeriodEnd, PriceID: item.Price.ID, Created: created}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var oldPrice string
	err = tx.QueryRowContext(ctx, `SELECT price_id FROM billing_subscriptions WHERE id=?`, id).Scan(&oldPrice)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if oldPrice != "" && oldPrice != b.PriceID {
		return nil, errors.New("subscription price changed outside Cosift")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE subscription_orders SET subscription_id=?,customer_id=? WHERE id=?`, id, b.CustomerID, orderID); err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO billing_subscriptions(id,user_id,order_id,customer_id,livemode,status,cancel_at_period_end,current_period_end,price_id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET status=excluded.status,cancel_at_period_end=excluded.cancel_at_period_end,current_period_end=excluded.current_period_end`, id, user, orderID, b.CustomerID, live, b.Status, b.CancelAtPeriodEnd, b.CurrentPeriodEnd, b.PriceID, created)
	if err != nil {
		return nil, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT paid_until FROM billing_subscriptions WHERE id=?`, id).Scan(&b.PaidUntil); err != nil {
		return nil, err
	}
	if b.Status == "canceled" || b.Status == "incomplete_expired" {
		if _, err = tx.ExecContext(ctx, `DELETE FROM subscription_checkout_pending WHERE order_id=?`, orderID); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &b, nil
}

func (s *Server) subscriptionEvent(ctx context.Context, event stripeEvent) error {
	var sub stripe.Subscription
	if json.Unmarshal(event.Data.Object, &sub) != nil {
		return errors.New("invalid subscription event")
	}
	if sub.Metadata["cosift_order_id"] == "" {
		var n int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM billing_subscriptions WHERE id=?`, sub.ID).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
	_, err := s.syncSubscription(ctx, sub.ID)
	return err
}

func (s *Server) completeSubscriptionCheckout(ctx context.Context, event stripeEvent) error {
	var session stripe.CheckoutSession
	if json.Unmarshal(event.Data.Object, &session) != nil {
		return errors.New("invalid subscription checkout")
	}
	order := session.Metadata["cosift_order_id"]
	if order == "" {
		return nil
	}
	if session.Mode != "subscription" || session.Status != "complete" || session.Subscription == nil || session.Customer == nil || session.Livemode != s.stripeLive() {
		return errors.New("invalid subscription checkout")
	}
	var user, existing string
	var live bool
	if err := s.db.QueryRowContext(ctx, `SELECT user_id,livemode,COALESCE(session_id,'') FROM subscription_orders WHERE id=?`, order).Scan(&user, &live, &existing); err != nil {
		return err
	}
	if user != session.ClientReferenceID || live != s.stripeLive() || (existing != "" && existing != session.ID) {
		return errors.New("subscription checkout ownership mismatch")
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE subscription_orders SET session_id=? WHERE id=? AND (session_id IS NULL OR session_id=?)`, session.ID, order, session.ID); err != nil {
		return err
	}
	b, err := s.syncSubscription(ctx, session.Subscription.ID)
	if err != nil {
		return err
	}
	if b == nil || b.OrderID != order || b.CustomerID != session.Customer.ID {
		return errors.New("subscription checkout binding mismatch")
	}
	// invoice.paid is the sole subscription credit authority.
	return nil
}
