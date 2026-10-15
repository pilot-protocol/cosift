package community

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestRouteLimitsHoldFiftyThousandKeysAndNeverRefuseANewOne(t *testing.T) {
	s := testServer(t, nil)
	for i := 0; i < 60000; i++ {
		if !s.allowRoute(httptest.NewRecorder(), "meter", fmt.Sprint("user", i), meterPerMinute) {
			t.Fatalf("new key %d refused", i)
		}
	}
	if n := len(s.routeLimits.keys); n != 50000 {
		t.Fatalf("store holds %d keys, want 50000", n)
	}
	if len(s.anonLimits.keys) != 0 || len(s.userLimits.keys) != 0 {
		t.Fatalf("the shared limiters were touched: %d anon, %d user", len(s.anonLimits.keys), len(s.userLimits.keys))
	}
}

func TestRouteLimitAnswers429WithRetryAfterAndKeepsUsersApart(t *testing.T) {
	s := testServer(t, nil)
	for i := 0; i < 6; i++ {
		if !s.allowRoute(httptest.NewRecorder(), "claim", "alice", 6) {
			t.Fatalf("call %d refused inside the limit", i)
		}
	}
	w := httptest.NewRecorder()
	if s.allowRoute(w, "claim", "alice", 6) {
		t.Fatal("the seventh call was allowed")
	}
	var body struct {
		Code  string `json:"code"`
		Retry int    `json:"retry_after_seconds"`
	}
	json.Unmarshal(w.Body.Bytes(), &body)
	header, _ := strconv.Atoi(w.Header().Get("Retry-After"))
	if w.Code != 429 || body.Code != "rate_limited" || header < 1 || header > 60 || header != body.Retry {
		t.Fatalf("status %d header %q body %+v", w.Code, w.Header().Get("Retry-After"), body)
	}
	if !s.allowRoute(httptest.NewRecorder(), "claim", "bob", 6) || !s.allowRoute(httptest.NewRecorder(), "submit", "alice", 6) {
		t.Fatal("another user or route shares the window")
	}
}
