package community

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	stripe "github.com/stripe/stripe-go/v86"
)

type subscriptionReviewFixture struct {
	server         *Server
	cookie         *http.Cookie
	user           User
	subscription   map[string]any
	invoices       map[string]map[string]any
	price          map[string]any
	paymentLookups int
	mu             sync.Mutex
}

func reviewClone(in map[string]any) map[string]any {
	b, _ := json.Marshal(in)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

func newSubscriptionReviewFixture(t *testing.T) *subscriptionReviewFixture {
	t.Helper()
	s, cookie, user := stripeTestServer(t)
	now := time.Now().Unix()
	end := time.Now().AddDate(0, 1, 0).Unix()
	price := map[string]any{"id": "price_review", "object": "price", "active": true, "currency": "usd", "unit_amount": 500, "product": "prod_review", "type": "recurring", "recurring": map[string]any{"interval": "month", "interval_count": 1}}
	subscription := map[string]any{"id": "sub_review", "object": "subscription", "livemode": false, "customer": "cus_review", "status": "active", "cancel_at_period_end": false, "metadata": map[string]any{"cosift_order_id": "subscription-order-review"}, "latest_invoice": "in_review", "items": map[string]any{"object": "list", "has_more": false, "data": []any{map[string]any{"id": "si_review", "object": "subscription_item", "quantity": 1, "current_period_start": now - 60, "current_period_end": end, "price": price}}}}
	line := map[string]any{"id": "il_review", "object": "line_item", "amount": 500, "currency": "usd", "quantity": 1, "period": map[string]any{"start": now - 60, "end": end}, "parent": map[string]any{"type": "subscription_item_details", "subscription_item_details": map[string]any{"subscription": "sub_review", "subscription_item": "si_review", "proration": false}}, "pricing": map[string]any{"type": "price_details", "price_details": map[string]any{"price": "price_review", "product": "prod_review"}}}
	invoice := map[string]any{"id": "in_review", "object": "invoice", "livemode": false, "customer": "cus_review", "status": "paid", "currency": "usd", "total": 500, "subtotal": 500, "amount_due": 500, "amount_paid": 500, "amount_paid_off_stripe": 0, "amount_remaining": 0, "billing_reason": "subscription_cycle", "parent": map[string]any{"type": "subscription_details", "subscription_details": map[string]any{"subscription": "sub_review", "metadata": map[string]any{"cosift_order_id": "subscription-order-review"}}}, "lines": map[string]any{"object": "list", "has_more": false, "data": []any{line}}, "payments": map[string]any{"object": "list", "has_more": false, "data": []any{map[string]any{"id": "inpay_review", "invoice": "in_review", "amount_paid": 500, "currency": "usd", "status": "paid", "payment": map[string]any{"type": "payment_intent", "payment_intent": "pi_review"}}}}}
	if _, err := s.db.Exec(`INSERT INTO subscription_orders(id,user_id,livemode,amount_cents,credits,currency,created_at,session_id,checkout_url,subscription_id,customer_id) VALUES(?,?,0,500,50000,'usd',?,'cs_review','https://checkout.stripe.com/c/pay/cs_review','sub_review','cus_review')`, "subscription-order-review", user.ID, now); err != nil {
		t.Fatal(err)
	}
	f := &subscriptionReviewFixture{server: s, cookie: cookie, user: user, subscription: subscription, invoices: map[string]map[string]any{"in_review": invoice}, price: price}
	s.paymentClient.Transport = pageTransport(func(r *http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var body any
		switch {
		case r.Method == "GET" && r.URL.Path == "/v1/subscriptions/sub_review":
			body = f.subscription
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/v1/invoices/"):
			id, _ := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/v1/invoices/"))
			body = f.invoices[id]
		case r.Method == "GET" && r.URL.Path == "/v1/prices/price_review":
			body = f.price
		case r.Method == "GET" && r.URL.Path == "/v1/invoice_payments":
			if r.URL.Query().Get("payment[payment_intent]") != "pi_review" || r.URL.Query().Get("payment[type]") != "payment_intent" {
				t.Errorf("refund used incorrect current-schema mapping filter: %s", r.URL.RawQuery)
			}
			f.paymentLookups++
			body = map[string]any{"object": "list", "has_more": false, "data": []any{map[string]any{"id": "inpay_review", "invoice": "in_review", "amount_paid": 500, "currency": "usd", "status": "paid", "livemode": false, "payment": map[string]any{"type": "payment_intent", "payment_intent": "pi_review"}}}}
		default:
			return nil, fmt.Errorf("unexpected Stripe request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Stripe-Version") != stripe.APIVersion {
			t.Error("Stripe objects were not normalized to the installed API version")
		}
		if body == nil {
			return nil, fmt.Errorf("missing fixture for %s", r.URL.Path)
		}
		data, _ := json.Marshal(body)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(data))), Request: r}, nil
	})
	return f
}

func reviewInvoiceLine(invoice map[string]any) map[string]any {
	return invoice["lines"].(map[string]any)["data"].([]any)[0].(map[string]any)
}

func TestSubscriptionReviewInvoicePaidExactlyOnceAndRestart(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	s := f.server
	for _, id := range []string{"evt_review_paid", "evt_review_paid", "evt_review_paid_other"} {
		expect(t, deliver(s, id, "invoice.paid", f.invoices["in_review"]), 200)
	}
	if got := paymentBalance(t, s, f.user); got != 50000 {
		t.Fatalf("duplicate invoice minted credits: %d", got)
	}
	reopened, err := Open(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.paymentClient = s.paymentClient
	expect(t, deliver(reopened, "evt_review_after_restart", "invoice.paid", f.invoices["in_review"]), 200)
	if got := paymentBalance(t, reopened, f.user); got != 50000 {
		t.Fatalf("restart minted credits: %d", got)
	}
	renewal := reviewClone(f.invoices["in_review"])
	renewal["id"] = "in_review_renewal"
	period := reviewInvoiceLine(renewal)["period"].(map[string]any)
	period["start"] = period["end"]
	period["end"] = time.Now().AddDate(0, 2, 0).Unix()
	f.invoices["in_review_renewal"] = renewal
	expect(t, deliver(s, "evt_review_renewal", "invoice.paid", renewal), 200)
	expect(t, deliver(s, "evt_review_old_again", "invoice.paid", f.invoices["in_review"]), 200)
	if got := paymentBalance(t, s, f.user); got != 100000 {
		t.Fatalf("out-of-order recurring invoice grants=%d", got)
	}
}

func TestSubscriptionReviewInvalidPaidInvoicesNeverMintCredits(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*subscriptionReviewFixture)
	}{
		{"unpaid", func(f *subscriptionReviewFixture) { f.invoices["in_review"]["status"] = "open" }},
		{"partial", func(f *subscriptionReviewFixture) {
			f.invoices["in_review"]["amount_paid"] = 499
			f.invoices["in_review"]["amount_remaining"] = 1
		}},
		{"off Stripe", func(f *subscriptionReviewFixture) { f.invoices["in_review"]["amount_paid_off_stripe"] = 500 }},
		{"currency", func(f *subscriptionReviewFixture) { f.invoices["in_review"]["currency"] = "eur" }},
		{"customer", func(f *subscriptionReviewFixture) { f.invoices["in_review"]["customer"] = "cus_another_account" }},
		{"line quantity", func(f *subscriptionReviewFixture) { reviewInvoiceLine(f.invoices["in_review"])["quantity"] = 2 }},
		{"line price", func(f *subscriptionReviewFixture) {
			reviewInvoiceLine(f.invoices["in_review"])["pricing"].(map[string]any)["price_details"].(map[string]any)["price"] = "price_unrelated"
		}},
		{"line currency", func(f *subscriptionReviewFixture) { reviewInvoiceLine(f.invoices["in_review"])["currency"] = "eur" }},
		{"missing subscription item", func(f *subscriptionReviewFixture) {
			delete(reviewInvoiceLine(f.invoices["in_review"])["parent"].(map[string]any)["subscription_item_details"].(map[string]any), "subscription_item")
		}},
		{"subscription owner", func(f *subscriptionReviewFixture) {
			f.subscription["customer"] = "cus_another_account"
		}},
		{"subscription price identity", func(f *subscriptionReviewFixture) {
			delete(f.price, "id")
		}},
		{"subscription item identity", func(f *subscriptionReviewFixture) {
			delete(f.subscription["items"].(map[string]any)["data"].([]any)[0].(map[string]any), "id")
		}},
		{"proration", func(f *subscriptionReviewFixture) {
			reviewInvoiceLine(f.invoices["in_review"])["parent"].(map[string]any)["subscription_item_details"].(map[string]any)["proration"] = true
		}},
		{"manual", func(f *subscriptionReviewFixture) { f.invoices["in_review"]["billing_reason"] = "manual" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSubscriptionReviewFixture(t)
			tc.mutate(f)
			w := deliver(f.server, "evt_review_invalid", "invoice.paid", f.invoices["in_review"])
			if got := paymentBalance(t, f.server, f.user); got != 0 {
				t.Fatalf("invalid invoice credited%d status%d: %s", got, w.Code, w.Body)
			}
		})
	}
}

func TestSubscriptionReviewInvoiceRefundUsesCurrentPaymentMapping(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	s := f.server
	expect(t, deliver(s, "evt_review_paid", "invoice.paid", f.invoices["in_review"]), 200)
	charge := map[string]any{"id": "ch_review", "object": "charge", "customer": "cus_review", "payment_intent": "pi_review", "amount": 500, "amount_refunded": 100, "currency": "usd", "livemode": false, "metadata": map[string]any{}}
	for _, step := range []struct {
		refunded int
		balance  int64
	}{{100, 40000}, {100, 40000}, {50, 40000}, {500, 0}} {
		charge["amount_refunded"] = step.refunded
		expect(t, deliver(s, fmt.Sprintf("evt_review_refund_%d", step.refunded), "charge.refunded", charge), 200)
		if got := paymentBalance(t, s, f.user); got != step.balance {
			t.Fatalf("refund reconciliation=%d want%d", got, step.balance)
		}
	}
	expect(t, deliver(s, "evt_review_paid_after_refund", "invoice.paid", f.invoices["in_review"]), 200)
	if got := paymentBalance(t, s, f.user); got != 0 {
		t.Fatalf("late paid event restored refunded credits%d", got)
	}
	if f.paymentLookups == 0 {
		t.Fatal("refund relied on removed charge.invoice instead of current payment mapping")
	}
}

func TestSubscriptionReviewCanceledSubscriberCannotStartTopup(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	s := f.server
	expect(t, deliver(s, "evt_review_paid", "invoice.paid", f.invoices["in_review"]), 200)
	if _, err := s.requireTopup(context.Background(), f.user.ID); err != nil {
		t.Fatal("paid active subscription denied", err)
	}
	// Stripe's fresh state is authoritative even before a cancellation webhook.
	previousActive := reviewClone(f.subscription)
	f.subscription["status"] = "canceled"
	denied := request(t, s, "POST", "/api/payments/checkout", map[string]any{"kind": "topup", "idempotency_key": "review-inactive-topup-001"}, f.cookie)
	if denied.Code < 400 {
		t.Fatalf("canceled subscriber started topup: %d %s", denied.Code, denied.Body)
	}
	var checkouts int
	if err := s.db.QueryRow(`SELECT count(*) FROM payment_checkouts`).Scan(&checkouts); err != nil || checkouts != 0 {
		t.Fatalf("inactive checkout persisted: %d %v", checkouts, err)
	}
	expect(t, deliver(s, "evt_review_cancel", "customer.subscription.deleted", f.subscription), 200)
	expect(t, deliver(s, "evt_review_stale_active", "customer.subscription.updated", previousActive), 200)
	b, err := s.accountSubscription(context.Background(), f.user.ID)
	if err != nil || b.active() {
		t.Fatalf("stale event reactivated canceled subscription: %+v %v", b, err)
	}
	// Payment completed for an order created while active must still be honored.
	seedOrder(t, s, f.user, "review-paid-before-cancel")
	expect(t, deliver(s, "evt_review_paid_topup", "checkout.session.completed", paidObject(f.user, "review-paid-before-cancel")), 200)
	if got := paymentBalance(t, s, f.user); got != 100000 {
		t.Fatalf("already-paid topup was lost after cancellation: %d", got)
	}
}

func TestSubscriptionReviewUnrelatedSubscriptionEventsAreIgnored(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	for _, event := range []string{"customer.subscription.updated", "customer.subscription.deleted"} {
		object := map[string]any{"id": "sub_pilot_unrelated", "object": "subscription", "livemode": false, "status": "active", "metadata": map[string]any{"pilot_order_id": "another-product-order"}}
		expect(t, deliver(f.server, "evt_review_unrelated_"+event, event, object), 200)
	}
	if got := paymentBalance(t, f.server, f.user); got != 0 {
		t.Fatalf("another product changed Cosift credit balance%d", got)
	}
}

func TestSubscriptionReviewConcurrentInvoiceDelivery(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	var wg sync.WaitGroup
	for i := range 12 {
		wg.Go(func() {
			w := deliver(f.server, fmt.Sprintf("evt_review_concurrent_%d", i), "invoice.paid", f.invoices["in_review"])
			if w.Code != 200 {
				t.Errorf("concurrent invoice status %d: %s", w.Code, w.Body)
			}
		})
	}
	wg.Wait()
	if got := paymentBalance(t, f.server, f.user); got != 50000 {
		t.Fatalf("concurrent invoice minted %d credits", got)
	}
	var invoiceCount, ledgerCount int
	if err := f.server.db.QueryRow(`SELECT count(*) FROM subscription_invoices`).Scan(&invoiceCount); err != nil {
		t.Fatal(err)
	}
	if err := f.server.db.QueryRow(`SELECT count(*) FROM credit_ledger WHERE reason='stripe_purchase'`).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if invoiceCount != 1 || ledgerCount != 1 {
		t.Fatalf("concurrent durable records: invoices %d, grants %d", invoiceCount, ledgerCount)
	}
}

func TestSubscriptionReviewRefundBeforeInvoiceRetriesWithoutMinting(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	charge := map[string]any{"id": "ch_review", "object": "charge", "customer": "cus_review", "payment_intent": "pi_review", "amount": 500, "amount_refunded": 500, "currency": "usd", "livemode": false, "metadata": map[string]any{}}
	first := deliver(f.server, "evt_review_early_refund", "charge.refunded", charge)
	if first.Code < 500 {
		t.Fatalf("early refund was acknowledged without durable reconciliation: %d", first.Code)
	}
	if got := paymentBalance(t, f.server, f.user); got != 0 {
		t.Fatalf("early refund changed ledger %d", got)
	}
	expect(t, deliver(f.server, "evt_review_later_paid", "invoice.paid", f.invoices["in_review"]), 200)
	expect(t, deliver(f.server, "evt_review_early_refund", "charge.refunded", charge), 200)
	if got := paymentBalance(t, f.server, f.user); got != 0 {
		t.Fatalf("retried refund left incorrect ledger %d", got)
	}
	if _, err := f.server.requireTopup(context.Background(), f.user.ID); err == nil {
		t.Fatal("fully refunded subscription permitted a top-up")
	}
}

func TestSubscriptionReviewRefundCustomerMismatchNeverDebits(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	expect(t, deliver(f.server, "evt_review_paid", "invoice.paid", f.invoices["in_review"]), 200)
	charge := map[string]any{"id": "ch_review", "object": "charge", "customer": "cus_another_account", "payment_intent": "pi_review", "amount": 500, "amount_refunded": 500, "currency": "usd", "livemode": false, "metadata": map[string]any{}}
	w := deliver(f.server, "evt_review_refund_other_customer", "charge.refunded", charge)
	if w.Code < 400 {
		t.Fatalf("mismatched customer refund accepted: %d", w.Code)
	}
	if got := paymentBalance(t, f.server, f.user); got != 50000 {
		t.Fatalf("another customer's refund debited %d", got)
	}
}

func TestSubscriptionReviewUnrelatedInvoiceAndRefundAreIgnored(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	unrelated := reviewClone(f.invoices["in_review"])
	unrelated["id"] = "in_pilot_unrelated"
	unrelated["customer"] = "cus_pilot_unrelated"
	unrelated["parent"] = map[string]any{"type": "subscription_details", "subscription_details": map[string]any{"subscription": "sub_pilot_unrelated", "metadata": map[string]any{"pilot_order_id": "another-product-order"}}}
	expect(t, deliver(f.server, "evt_review_pilot_paid", "invoice.paid", unrelated), 200)
	expect(t, deliver(f.server, "evt_review_pilot_failed", "invoice.payment_failed", unrelated), 200)
	f.server.paymentClient.Transport = pageTransport(func(r *http.Request) (*http.Response, error) {
		var body any
		switch r.URL.Path {
		case "/v1/invoice_payments":
			body = map[string]any{"object": "list", "has_more": false, "data": []any{map[string]any{"id": "inpay_pilot", "invoice": "in_pilot_unrelated", "amount_paid": 500, "currency": "usd", "status": "paid", "livemode": false, "payment": map[string]any{"type": "payment_intent", "payment_intent": "pi_pilot_unrelated"}}}}
		case "/v1/invoices/in_pilot_unrelated":
			body = unrelated
		default:
			t.Errorf("unrelated payment reached Cosift subscription: %s", r.URL.Path)
			return nil, fmt.Errorf("unexpected request")
		}
		data, _ := json.Marshal(body)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(data))), Request: r}, nil
	})
	charge := map[string]any{"id": "ch_pilot_unrelated", "object": "charge", "customer": "cus_pilot_unrelated", "payment_intent": "pi_pilot_unrelated", "amount": 500, "amount_refunded": 500, "currency": "usd", "livemode": false, "metadata": map[string]any{"pilot_order_id": "another-product-order"}}
	expect(t, deliver(f.server, "evt_review_pilot_refund", "charge.refunded", charge), 200)
	if got := paymentBalance(t, f.server, f.user); got != 0 {
		t.Fatalf("another product changed Cosift credit balance %d", got)
	}
}

func TestSubscriptionReviewCanonicalInvoiceAndFailedRenewal(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	// A stale open snapshot does not override Stripe's current paid invoice.
	snapshot := reviewClone(f.invoices["in_review"])
	snapshot["status"] = "open"
	snapshot["amount_paid"] = 0
	expect(t, deliver(f.server, "evt_review_normalized_paid", "invoice.paid", snapshot), 200)
	if got := paymentBalance(t, f.server, f.user); got != 50000 {
		t.Fatalf("canonical paid invoice was not credited: %d", got)
	}
	failed := reviewClone(f.invoices["in_review"])
	failed["id"] = "in_review_failed_renewal"
	failed["status"] = "open"
	failed["amount_paid"] = 0
	failed["amount_remaining"] = 500
	f.invoices["in_review_failed_renewal"] = failed
	f.subscription["status"] = "past_due"
	expect(t, deliver(f.server, "evt_review_failed_renewal", "invoice.payment_failed", failed), 200)
	// A misleading paid snapshot still cannot mint for the canonical unpaid invoice.
	falsePaid := reviewClone(failed)
	falsePaid["status"] = "paid"
	falsePaid["amount_paid"] = 500
	falsePaid["amount_remaining"] = 0
	w := deliver(f.server, "evt_review_stale_paid", "invoice.paid", falsePaid)
	if w.Code < 400 {
		t.Fatalf("snapshot overrode canonical unpaid invoice: %d", w.Code)
	}
	if got := paymentBalance(t, f.server, f.user); got != 50000 {
		t.Fatalf("failed renewal altered purchased carryover: %d", got)
	}
	if _, err := f.server.requireTopup(context.Background(), f.user.ID); err == nil {
		t.Fatal("past-due subscription permitted a top-up")
	}
}
