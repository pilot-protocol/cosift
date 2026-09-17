package community

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGuestContributionRejectedBeforeParsingAndQuota(t *testing.T) {
	s := testServer(t, nil)
	for _, kind := range []string{"application/json", "multipart/form-data; boundary=bad"} {
		r := httptest.NewRequest("POST", "/api/submissions", strings.NewReader("invalid upload"))
		r.Header.Set("Content-Type", kind)
		r.Header.Set("X-Cosift-Client", "community")
		r.Header.Set("X-Forwarded-For", "127.0.0.1, 192.222.56.72")
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		expect(t, w, 401)
	}
	w := httptest.NewRecorder()
	s.submit(w, httptest.NewRequest("POST", "/api/submissions", nil), User{})
	expect(t, w, 401)
	for _, table := range []string{"submissions", "guest_usage"} {
		var n int
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s changed: %d %v", table, n, err)
		}
	}
}

func TestCreditsMonthlyLedgerBoundaries(t *testing.T) {
	s := testServer(t, nil)
	cookie := account(t, s, "monthly@example.com")
	var fresh struct {
		Balance int
		Monthly struct{ Free, Earned, Purchased, Spent int }
	}
	if err := json.Unmarshal(request(t, s, "GET", "/api/credits", nil, cookie).Body.Bytes(), &fresh); err != nil {
		t.Fatal(err)
	}
	if fresh.Balance != monthlyFreeCredits || fresh.Monthly.Free != monthlyFreeCredits || fresh.Monthly.Earned != 0 || fresh.Monthly.Purchased != 0 || fresh.Monthly.Spent != 0 {
		t.Fatalf("new account received an incorrect monthly grant: %+v", fresh)
	}
	var user User
	json.Unmarshal(request(t, s, "GET", "/api/me", nil, cookie).Body.Bytes(), &user)
	now := time.Now().UTC()
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	rows := []struct {
		id     string
		delta  int
		reason string
		at     int64
	}{
		{"old", 100, "verified_contribution", start.Unix() - 1},
		{"earned", 10, "verified_contribution", start.Unix()},
		{"bought", 50000, "stripe_purchase", end.Unix() - 1},
		{"spent", -2, "extra_request", now.Unix()},
		{"refund", -500, "stripe_refund", now.Unix()},
		{"future", 20, "verified_contribution", end.Unix()},
	}
	for _, row := range rows {
		if _, err := s.db.Exec(`INSERT INTO credit_ledger VALUES(?,?,?,?,?)`, row.id, user.ID, row.delta, row.reason, row.at); err != nil {
			t.Fatal(err)
		}
	}
	w := request(t, s, "GET", "/api/credits", nil, cookie)
	expect(t, w, http.StatusOK)
	var got struct {
		Balance int `json:"balance"`
		Monthly struct {
			Month     string `json:"month"`
			StartsAt  string `json:"starts_at"`
			Timezone  string `json:"timezone"`
			Earned    int    `json:"earned"`
			Purchased int    `json:"purchased"`
			Spent     int    `json:"spent"`
		} `json:"monthly"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Balance != 49628+monthlyFreeCredits || got.Monthly.Earned != 10 || got.Monthly.Purchased != 50000 || got.Monthly.Spent != 2 || got.Monthly.Month != start.Format("2006-01") || got.Monthly.StartsAt != start.Format(time.RFC3339) || got.Monthly.Timezone != "UTC" {
		t.Fatalf("incorrect monthly ledger: %+v", got)
	}
}

func TestAnalyticsConfigurationAndCSP(t *testing.T) {
	base := testServer(t, nil)
	for _, id := range []string{"", "G-ABC123"} {
		cfg := base.cfg
		cfg.DataDir = t.TempDir()
		cfg.GAMeasurementID = id
		s, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		w := request(t, s, "GET", "/api/analytics", nil, nil)
		expect(t, w, 200)
		var got map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || len(got) != 1 || got["measurement_id"] != id {
			t.Fatalf("public analytics metadata: %s %v", w.Body, err)
		}
		csp := w.Header().Get("Content-Security-Policy")
		enabled := id != ""
		if strings.Contains(csp, "https://www.googletagmanager.com/gtag/js") != enabled || strings.Contains(csp, "https://www.google-analytics.com/g/collect") != enabled || strings.Contains(csp, "https://region1.google-analytics.com/g/collect") != enabled {
			t.Fatalf("incorrect analytics CSP: %s", csp)
		}
		if strings.Contains(csp, "unsafe-inline") || strings.Contains(csp, "unsafe-eval") {
			t.Fatal("CSP weakened")
		}
	}
	for _, id := range []string{"G-", "G-abc", " G-ABC", "G-ABC; script-src *", "https://example.com"} {
		cfg := base.cfg
		cfg.GAMeasurementID = id
		if s, err := Open(cfg); err == nil {
			s.Close()
			t.Fatalf("accepted invalid measurement id %q", id)
		}
	}
}
