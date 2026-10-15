package community

import (
	"encoding/json"
	"net/http"
	"testing"
)

type refusal struct {
	Code            string `json:"code"`
	CreditsRequired int    `json:"credits_required"`
	Balance         int    `json:"balance"`
	TopupURL        string `json:"topup_url"`
	RetryAfter      int    `json:"retry_after_seconds"`
	Error           string `json:"error"`
}

func okEngine() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) })
}

func TestSearchRefusalsCarryAStableCode(t *testing.T) {
	s := testServer(t, okEngine())
	_, cookie := fundedCreditAccount(t, s, 1)
	w := request(t, s, "GET", "/api/answer?q=test", nil, cookie)
	expect(t, w, 429)
	var got refusal
	json.Unmarshal(w.Body.Bytes(), &got)
	if got.Code != "insufficient_credits" || got.CreditsRequired != 2 || got.Balance != 1 || got.TopupURL != "http://localhost:7780/" || got.Error == "" {
		t.Fatalf("credits refusal: %s", w.Body.String())
	}
	limited := testServer(t, okEngine())
	limited.cfg.SearchRPM = 1
	_, cookie = fundedCreditAccount(t, limited, 100)
	expect(t, request(t, limited, "GET", "/api/search?q=test", nil, cookie), 200)
	w = request(t, limited, "GET", "/api/search?q=test", nil, cookie)
	expect(t, w, 429)
	got = refusal{}
	json.Unmarshal(w.Body.Bytes(), &got)
	if got.Code != "rate_limited" || got.RetryAfter < 1 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("rate refusal: %s", w.Body.String())
	}
}
