package community

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type engineCall struct{ method, path, auth, other string }

func recordingEngine(t *testing.T, calls *[]engineCall, mu *sync.Mutex) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		*calls = append(*calls, engineCall{r.Method, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Cookie") + r.Header.Get("X-Cosift-Client")})
		mu.Unlock()
		switch r.URL.Path {
		case "/v1/community/moderate":
			w.Write([]byte(`{"decision":"allow","category":"safe"}`))
		case "/v1/community/enqueue":
			w.Write([]byte(`{"indexed":true,"novel":false,"content_hash":"` + strings.Repeat("a", 64) + `"}`))
		}
	})
}

func TestDeliveryUsesOnlyTheBrokerKeyOnTheV1Routes(t *testing.T) {
	logs := captureLog(t)
	var calls []engineCall
	var mu sync.Mutex
	s := testServer(t, recordingEngine(t, &calls, &mu))
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 || calls[0].path != "/v1/community/moderate" || calls[1].path != "/v1/community/enqueue" {
		t.Fatalf("calls = %+v", calls)
	}
	for _, c := range calls {
		if c.method != "POST" || c.auth != "Bearer "+testBrokerKey || c.other != "" {
			t.Fatalf("call %+v carries more or less than the broker key", c)
		}
	}
	if strings.Contains(logs.String(), testBrokerKey) {
		t.Fatal("the broker key was logged")
	}
}

func TestEngineClientDoesNotFollowRedirects(t *testing.T) {
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("a redirect was followed to %s with %q", r.URL, r.Header.Get("Authorization"))
	}))
	defer elsewhere.Close()
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	res, err := s.engine.post(context.Background(), moderateRoute, []byte(`{}`), moderateTimeout)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestEngineURLMustBeALoopbackHTTPOrigin(t *testing.T) {
	for raw, ok := range map[string]bool{
		"": true, "http://127.0.0.1:7779": true, "http://localhost:7779": true, "http://[::1]:7779/": true,
		"https://127.0.0.1:7779": false, "http://example.com:7779": false, "http://10.0.0.5:7779": false,
		"http://127.0.0.1:7779/v1": false, "http://u:p@127.0.0.1:7779": false, "http://127.0.0.1:7779?x=1": false,
	} {
		_, err := newEngineClient(raw, testBrokerKey)
		if (err == nil) != ok {
			t.Errorf("%q: err = %v, want ok=%v", raw, err, ok)
		}
	}
	if c, _ := newEngineClient("", ""); c.base != defaultEngineURL {
		t.Fatalf("default base %q", c.base)
	}
}

func TestBrokerKeyMustBeAnEngineKeyAndStaysOutOfErrors(t *testing.T) {
	for _, key := range []string{"peer-token-0123456789", "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "csk_short_ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRST"} {
		_, err := newEngineClient("", key)
		if err == nil || strings.Contains(err.Error(), key) {
			t.Errorf("key %q: err = %v", key, err)
		}
	}
	if _, err := newEngineClient("", testBrokerKey); err != nil {
		t.Fatal(err)
	}
	if c, err := newEngineClient("", ""); err != nil || c.configured() {
		t.Fatalf("an absent key must be accepted and unconfigured: %v", err)
	}
}

func TestMissingBrokerKeyIsAConfigErrorWithoutCallsOrAttempts(t *testing.T) {
	logs := captureLog(t)
	var calls []engineCall
	var mu sync.Mutex
	s := testServer(t, recordingEngine(t, &calls, &mu))
	s.engine.key = ""
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := listSubmissions(t, s, cookie); got[0].Status != statusConfigError {
		t.Fatalf("status %q", got[0].Status)
	}
	var attempts int
	s.db.QueryRow(`SELECT attempts FROM submissions`).Scan(&attempts)
	mu.Lock()
	n := len(calls)
	mu.Unlock()
	if attempts != 0 || n != 0 || !strings.Contains(logs.String(), "COSIFT_COMMUNITY_BROKER_KEY") {
		t.Fatalf("attempts=%d calls=%d log=%q", attempts, n, logs.String())
	}
}
