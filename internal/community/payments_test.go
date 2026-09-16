package community

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stripe/stripe-go/v86/webhook"
)

func stripeTestServer(t *testing.T) (*Server, *http.Cookie, User) {
	t.Helper()
	s := testServer(t, nil)
	s.cfg.StripeSecretKey = "sk_test_fake_for_unit_tests"
	s.cfg.StripeWebhookSecret = "whsec_fake_for_unit_tests"
	cookie := account(t, s, "payments@example.com")
	var u User
	if err := json.Unmarshal(request(t, s, "GET", "/api/me", nil, cookie).Body.Bytes(), &u); err != nil {
		t.Fatal(err)
	}
	s.paymentClient.Transport = pageTransport(func(r *http.Request) (*http.Response, error) {
		t.Error("unexpected Stripe network call")
		return nil, fmt.Errorf("network disabled")
	})
	return s, cookie, u
}
func paymentBalance(t *testing.T, s *Server, u User) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRow(`SELECT COALESCE(SUM(delta),0) FROM credit_ledger WHERE user_id=?`, u.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func seedOrder(t *testing.T, s *Server, u User, id string) {
	t.Helper()
	if _, err := s.db.Exec(`INSERT INTO payment_checkouts(id,user_id,amount_cents,credits,currency,created_at) VALUES(?,?,500,50000,'usd',?)`, id, u.ID, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
}
func paidObject(u User, order string) map[string]any {
	return map[string]any{"id": "cs_test_payment", "mode": "payment", "status": "complete", "payment_status": "paid", "currency": "usd", "amount_total": 500, "client_reference_id": u.ID, "payment_intent": "pi_test_payment", "livemode": false, "metadata": map[string]string{"cosift_order_id": order}}
}
func signedEvent(s *Server, id, kind string, object any, stamp time.Time, secret string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]any{"id": id, "type": kind, "livemode": false, "data": map[string]any{"object": object}})
	signed := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{Payload: body, Secret: secret, Timestamp: stamp})
	req := httptest.NewRequest("POST", "/api/payments/webhook", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", signed.Header)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	return w
}
func deliver(s *Server, id, kind string, object any) *httptest.ResponseRecorder {
	return signedEvent(s, id, kind, object, time.Now(), s.cfg.StripeWebhookSecret)
}

func TestStripeCheckoutConfigurationAuthAndServerPrice(t *testing.T) {
	s, cookie, u := stripeTestServer(t)
	s.cfg.StripeWebhookSecret = ""
	expect(t, request(t, s, "POST", "/api/payments/checkout", map[string]string{"idempotency_key": "test-checkout-key-0001"}, cookie), 503)
	if s.paymentsEnabled() {
		t.Fatal("partial config enabled payments")
	}
	s.cfg.StripeWebhookSecret = "whsec_fake_for_unit_tests"
	expect(t, request(t, s, "POST", "/api/payments/checkout", map[string]string{"idempotency_key": "test-checkout-key-0001"}, nil), 401)
	expect(t, request(t, s, "POST", "/api/payments/checkout", map[string]any{"idempotency_key": "test-checkout-key-0001", "amount": 1, "credits": 999999}, cookie), 400)
	expect(t, request(t, s, "POST", "/api/payments/checkout", map[string]string{"idempotency_key": "short"}, cookie), 400)
	calls := 0
	s.paymentClient.Transport = pageTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != "https://api.stripe.com/v1/checkout/sessions" || r.Method != "POST" {
			t.Error("wrong Stripe endpoint")
		}
		key, pass, ok := r.BasicAuth()
		if !ok || key != s.cfg.StripeSecretKey || pass != "" {
			t.Error("missing secret authentication")
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		for key, want := range map[string]string{"mode": "payment", "payment_method_types[0]": "card", "line_items[0][price_data][unit_amount]": "500", "line_items[0][price_data][currency]": "usd", "line_items[0][quantity]": "1", "client_reference_id": u.ID, "adaptive_pricing[enabled]": "false"} {
			if r.Form.Get(key) != want {
				t.Errorf("%s=%q want %q", key, r.Form.Get(key), want)
			}
		}
		order := r.Form.Get("metadata[cosift_order_id]")
		if order == "" || r.Header.Get("Idempotency-Key") != order || r.Form.Get("payment_intent_data[metadata][cosift_order_id]") != order {
			t.Error("missing stable order binding")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"id":"cs_test_created","url":"https://checkout.stripe.com/c/pay/cs_test_created","livemode":false}`))}, nil
	})
	for range 2 {
		expect(t, request(t, s, "POST", "/api/payments/checkout", map[string]string{"idempotency_key": "test-checkout-key-0001"}, cookie), 200)
	}
	if calls != 1 {
		t.Fatalf("retry created %d sessions", calls)
	}
	expect(t, request(t, s, "GET", "/?payment=success", nil, cookie), 200)
	if paymentBalance(t, s, u) != 0 {
		t.Fatal("browser return credited unpaid order")
	}
}

func TestStripePaidWebhookExactlyOnceAndRestart(t *testing.T) {
	s, _, u := stripeTestServer(t)
	seedOrder(t, s, u, "our-order")
	object := paidObject(u, "our-order")
	var wg sync.WaitGroup
	var failures atomic.Int32
	for i := range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if deliver(s, fmt.Sprintf("evt_%d", i), "checkout.session.completed", object).Code != 200 {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("concurrent failures: %d", failures.Load())
	}
	if paymentBalance(t, s, u) != 50000 {
		t.Fatal("duplicate notification minted credits")
	}
	reopened, err := Open(s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	expect(t, deliver(reopened, "evt_another", "checkout.session.async_payment_succeeded", object), 200)
	if paymentBalance(t, s, u) != 50000 {
		t.Fatal("restart minted credits")
	}
}

func TestStripeRejectsForgedUnpaidAndMismatchedEvents(t *testing.T) {
	s, _, u := stripeTestServer(t)
	seedOrder(t, s, u, "our-order")
	expect(t, signedEvent(s, "evt_bad", "checkout.session.completed", paidObject(u, "our-order"), time.Now(), "whsec_wrong"), 400)
	expect(t, signedEvent(s, "evt_old", "checkout.session.completed", paidObject(u, "our-order"), time.Now().Add(-10*time.Minute), s.cfg.StripeWebhookSecret), 400)
	for _, change := range []struct {
		key    string
		value  any
		status int
	}{
		{"payment_status", "unpaid", 200}, {"amount_total", 1, 500}, {"currency", "eur", 500}, {"client_reference_id", "someone-else", 500}, {"livemode", true, 500}, {"mode", "subscription", 500}, {"status", "open", 500}, {"payment_intent", "", 500},
	} {
		obj := paidObject(u, "our-order")
		obj[change.key] = change.value
		expect(t, deliver(s, "evt_mismatch", "checkout.session.completed", obj), change.status)
	}
	expect(t, deliver(s, "evt_foreign", "checkout.session.completed", paidObject(u, "missing-order")), 500)
	obj := paidObject(u, "our-order")
	obj["metadata"] = map[string]string{}
	expect(t, deliver(s, "evt_other_app", "checkout.session.completed", obj), 200)
	if paymentBalance(t, s, u) != 0 {
		t.Fatal("invalid/unpaid event granted credits")
	}
	expect(t, deliver(s, "evt_good", "checkout.session.completed", paidObject(u, "our-order")), 200)
	obj = paidObject(u, "our-order")
	obj["id"] = "cs_test_other"
	expect(t, deliver(s, "evt_wrong_session", "checkout.session.completed", obj), 500)
	if paymentBalance(t, s, u) != 50000 {
		t.Fatal("session mismatch granted credits")
	}
}

func TestStripeFulfillmentTransactionFailureRetries(t *testing.T) {
	s, _, u := stripeTestServer(t)
	seedOrder(t, s, u, "our-order")
	_, err := s.db.Exec(`CREATE TRIGGER fail_payment_event BEFORE INSERT ON payment_events BEGIN SELECT RAISE(ABORT,'simulated disk failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	expect(t, deliver(s, "evt_retry", "checkout.session.completed", paidObject(u, "our-order")), 500)
	if paymentBalance(t, s, u) != 0 {
		t.Fatal("partial payment transaction committed")
	}
	if _, err = s.db.Exec(`DROP TRIGGER fail_payment_event`); err != nil {
		t.Fatal(err)
	}
	expect(t, deliver(s, "evt_retry", "checkout.session.completed", paidObject(u, "our-order")), 200)
	if paymentBalance(t, s, u) != 50000 {
		t.Fatal("retry lost purchase")
	}
}

func TestStripeRefundsPartialFullDuplicateAndOutOfOrder(t *testing.T) {
	s, _, u := stripeTestServer(t)
	seedOrder(t, s, u, "our-order")
	refund := func(cents int) map[string]any {
		return map[string]any{"payment_intent": "pi_test_payment", "amount": 500, "amount_refunded": cents, "currency": "usd", "livemode": false, "metadata": map[string]string{"cosift_order_id": "our-order"}}
	}
	expect(t, deliver(s, "evt_early_refund", "charge.refunded", refund(100)), 500)
	expect(t, deliver(s, "evt_paid", "checkout.session.completed", paidObject(u, "our-order")), 200)
	for _, step := range []struct {
		cents   int
		balance int64
	}{{100, 40000}, {100, 40000}, {50, 40000}, {500, 0}, {500, 0}} {
		expect(t, deliver(s, fmt.Sprintf("evt_refund_%d", step.cents), "charge.refunded", refund(step.cents)), 200)
		if got := paymentBalance(t, s, u); got != step.balance {
			t.Fatalf("refund balance=%d want %d", got, step.balance)
		}
	}
	expect(t, deliver(s, "evt_late_paid", "checkout.session.completed", paidObject(u, "our-order")), 200)
	if paymentBalance(t, s, u) != 0 {
		t.Fatal("late payment restored refunded credits")
	}
}

func TestStripeCheckoutRejectsRedirectsAndLeaksNoKey(t *testing.T) {
	s, cookie, _ := stripeTestServer(t)
	for i, raw := range []string{"https://evil.example/pay", "http://checkout.stripe.com/pay", "https://checkout.stripe.com.evil.example/pay", "https://user@checkout.stripe.com/pay"} {
		s.paymentClient.Transport = pageTransport(func(r *http.Request) (*http.Response, error) {
			b, _ := json.Marshal(map[string]any{"id": "cs_bad", "url": raw, "livemode": false})
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(b))}, nil
		})
		w := request(t, s, "POST", "/api/payments/checkout", map[string]string{"idempotency_key": fmt.Sprintf("checkout-bad-url-%d", i)}, cookie)
		expect(t, w, 502)
		if strings.Contains(w.Body.String(), s.cfg.StripeSecretKey) {
			t.Fatal("key leaked")
		}
	}
	r := httptest.NewRequest("POST", "/api/payments/checkout", strings.NewReader(`{}`))
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	expect(t, w, 403)
	r = httptest.NewRequest("POST", "/api/payments/webhook", strings.NewReader(`{}`))
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	expect(t, w, 400)
}

func TestStripeCheckoutRetryKeepsSameOrderAfterTimeout(t *testing.T) {
	s, cookie, _ := stripeTestServer(t)
	var first string
	calls := 0
	s.paymentClient.Transport = pageTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		key := r.Header.Get("Idempotency-Key")
		if first == "" {
			first = key
		} else if key != first {
			t.Error("retry changed Stripe idempotency key")
		}
		if calls == 1 {
			return nil, fmt.Errorf("simulated timeout after Stripe created session")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"id":"cs_retry","url":"https://checkout.stripe.com/c/pay/cs_retry","livemode":false}`))}, nil
	})
	body := map[string]string{"idempotency_key": "checkout-retry-123456"}
	expect(t, request(t, s, "POST", "/api/payments/checkout", body, cookie), 502)
	expect(t, request(t, s, "POST", "/api/payments/checkout", body, cookie), 200)
	var n int
	s.db.QueryRow(`SELECT COUNT(*) FROM payment_checkouts`).Scan(&n)
	if n != 1 {
		t.Fatalf("retry created %d orders", n)
	}
	s.db.Exec(`UPDATE payment_checkouts SET created_at=?`, time.Now().Add(-24*time.Hour).Unix())
	expect(t, request(t, s, "POST", "/api/payments/checkout", body, cookie), 409)
	if calls != 2 {
		t.Fatal("expired key sent to Stripe again")
	}
}
