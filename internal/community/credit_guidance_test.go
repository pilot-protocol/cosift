package community

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInsufficientCreditsGuidanceListsOnlyTheWaysThatExist(t *testing.T) {
	for name, tc := range map[string]struct {
		payments, reward bool
		want, absent     []string
	}{
		"monthly grant only": {false, false, []string{"use your next monthly grant"}, []string{"Billing", "contribute"}},
		"with billing":       {true, false, []string{"add credits in Billing", "use your next monthly grant"}, []string{"contribute"}},
		"with rewards":       {false, true, []string{"contribute verified new webpages"}, []string{"Billing"}},
	} {
		t.Run(name, func(t *testing.T) {
			s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) }))
			if tc.payments {
				s.cfg.StripeSecretKey, s.cfg.StripeWebhookSecret, s.cfg.AllowTestPayments = "sk_test_fake_for_unit_tests", "whsec_fake_for_unit_tests", true
			}
			s.cfg.ContributionReward = tc.reward
			_, cookie := fundedCreditAccount(t, s, 0)
			w := request(t, s, "GET", "/api/search?q=test", nil, cookie)
			expect(t, w, 429)
			for _, want := range tc.want {
				if !strings.Contains(w.Body.String(), want) {
					t.Errorf("guidance lacks %q: %s", want, w.Body.String())
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(w.Body.String(), absent) {
					t.Errorf("guidance offers %q although it is unavailable: %s", absent, w.Body.String())
				}
			}
		})
	}
}

func TestBearerLogoutClearsTheSessionCookieName(t *testing.T) {
	s := httpsServer(t)
	s.cfg.Shared = &fakeShared{}
	req := httptest.NewRequest("POST", "/api/logout", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+sharedTestToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Cosift-Client", "community")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	expect(t, w, 200)
	if c := cookieNamed(w, s.sessionCookieName()); c == nil || c.MaxAge != -1 || c.Value != "" {
		t.Fatalf("session cookie not cleared: %+v", c)
	}
}
