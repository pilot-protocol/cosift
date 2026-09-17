package community

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSubscriptionCheckoutOwnsPlanAndReusesPendingSession(t *testing.T) {
	s, cookie, u := stripeTestServer(t)
	expect(t, request(t, s, "POST", "/api/payments/checkout", map[string]string{"kind": "topup", "idempotency_key": "topup-without-membership"}, cookie), 403)
	var rows int
	s.db.QueryRow(`SELECT count(*) FROM payment_checkouts`).Scan(&rows)
	if rows != 0 {
		t.Fatal("blocked top-up created an order")
	}
	calls := 0
	var order string
	s.paymentClient.Transport = pageTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/v1/checkout/sessions" {
			t.Fatal("unexpected Stripe request")
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		for field, want := range map[string]string{"mode": "subscription", "client_reference_id": u.ID, "line_items[0][price_data][currency]": "usd", "line_items[0][price_data][unit_amount]": "500", "line_items[0][price_data][recurring][interval]": "month", "line_items[0][price_data][recurring][interval_count]": "1", "line_items[0][quantity]": "1", "success_url": s.cfg.PublicURL + "/?payment=success", "cancel_url": s.cfg.PublicURL + "/?payment=cancelled"} {
			if r.Form.Get(field) != want {
				t.Errorf("%s=%q", field, r.Form.Get(field))
			}
		}
		order = r.Form.Get("metadata[cosift_order_id]")
		if order == "" || order != r.Header.Get("Idempotency-Key") || order != r.Form.Get("subscription_data[metadata][cosift_order_id]") {
			t.Fatal("subscription order is not bound")
		}
		if r.Form.Get("payment_intent_data[metadata][cosift_order_id]") != "" {
			t.Fatal("subscription used unsupported PaymentIntent form")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"id":"cs_subscription_new","url":"https://checkout.stripe.com/c/pay/cs_subscription_new","livemode":false}`))}, nil
	})
	for _, key := range []string{"monthly-subscription-001", "monthly-subscription-001", "different-browser-tab-002"} {
		expect(t, request(t, s, "POST", "/api/payments/checkout", map[string]string{"kind": "subscription", "idempotency_key": key}, cookie), 200)
	}
	if calls != 1 {
		t.Fatalf("duplicate sessions=%d", calls)
	}
	if paymentBalance(t, s, u) != 0 {
		t.Fatal("creating checkout minted credits")
	}
	var saved string
	if err := s.db.QueryRow(`SELECT order_id FROM subscription_checkout_pending WHERE user_id=?`, u.ID).Scan(&saved); err != nil || saved != order {
		t.Fatal("pending checkout reservation missing")
	}
}

func TestSubscriptionCheckoutTimeoutRetriesStableProviderOrder(t *testing.T) {
	s, cookie, _ := stripeTestServer(t)
	calls := 0
	first := ""
	s.paymentClient.Transport = pageTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		key := r.Header.Get("Idempotency-Key")
		if first == "" {
			first = key
		} else if key != first {
			t.Fatal("retry changed order")
		}
		if calls == 1 {
			return nil, fmt.Errorf("timeout after provider accepted request")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"id":"cs_subscription_retry","url":"https://checkout.stripe.com/c/pay/cs_subscription_retry","livemode":false}`))}, nil
	})
	for _, code := range []int{502, 200} {
		expect(t, request(t, s, "POST", "/api/payments/checkout", map[string]string{"kind": "subscription", "idempotency_key": "subscription-timeout-retry"}, cookie), code)
	}
}

func TestSubscriptionCheckoutCompletionNeverGrantsMonthlyCredits(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	s := f.server
	object := map[string]any{"id": "cs_review", "mode": "subscription", "status": "complete", "payment_status": "paid", "livemode": false, "customer": "cus_review", "subscription": "sub_review", "client_reference_id": f.user.ID, "metadata": map[string]string{"cosift_order_id": "subscription-order-review"}}
	expect(t, deliver(s, "evt_subscription_checkout", "checkout.session.completed", object), 200)
	if paymentBalance(t, s, f.user) != 0 {
		t.Fatal("subscription checkout granted credits before invoice")
	}
	w := request(t, s, "GET", "/api/credits", nil, f.cookie)
	expect(t, w, 200)
	var info map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info["can_top_up"] != false || info["monthly_free_credits"] != float64(1000) || info["free_requests_per_minute"] != float64(0) || info["all_authenticated_requests_metered"] != true {
		t.Fatal("unpaid subscription changed free plan or enabled top-ups")
	}
	if info["subscription"].(map[string]any)["active"] != false {
		t.Fatal("unpaid active provider status unlocked paid access")
	}
}

func TestSubscriptionPortalUsesOnlyBoundCustomerAndDedicatedConfiguration(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	s := f.server
	expect(t, deliver(s, "evt_portal_sub", "customer.subscription.created", f.subscription), 200)
	s.cfg.StripePortalConfigurationID = "bpc_cosift_private_config"
	s.paymentClient.Transport = pageTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method != "POST" || r.URL.Path != "/v1/billing_portal/sessions" {
			t.Fatal("unexpected portal request")
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("customer") != "cus_review" || r.Form.Get("configuration") != s.cfg.StripePortalConfigurationID || r.Form.Get("return_url") != s.cfg.PublicURL+"/?payment=manage" {
			t.Fatal("portal crossed customer/configuration boundary")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"url":"https://billing.stripe.com/p/session/test"}`))}, nil
	})
	expect(t, request(t, s, "POST", "/api/payments/portal", map[string]any{}, nil), 401)
	expect(t, request(t, s, "POST", "/api/payments/portal", map[string]string{"customer": "cus_someone_else"}, f.cookie), 400)
	expect(t, request(t, s, "POST", "/api/payments/portal", map[string]any{}, f.cookie), 200)
	s.cfg.StripePortalConfigurationID = ""
	expect(t, request(t, s, "POST", "/api/payments/portal", map[string]any{}, f.cookie), 503)
}

func TestSubscriptionActiveAndExpiredPaidPeriodGateTopups(t *testing.T) {
	f := newSubscriptionReviewFixture(t)
	s := f.server
	expect(t, deliver(s, "evt_initial_paid", "invoice.paid", f.invoices["in_review"]), 200)
	w := request(t, s, "GET", "/api/credits", nil, f.cookie)
	expect(t, w, 200)
	var out map[string]any
	json.Unmarshal(w.Body.Bytes(), &out)
	if out["can_top_up"] != true {
		t.Fatal("paid active subscription did not unlock top-ups")
	}
	s.db.Exec(`UPDATE billing_subscriptions SET paid_until=?`, time.Now().Unix()-1)
	expect(t, request(t, s, "POST", "/api/payments/checkout", map[string]string{"kind": "topup", "idempotency_key": "expired-paid-period-topup"}, f.cookie), 403)
}
