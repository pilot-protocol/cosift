package community

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func navigate(s *Server, method, path string, headers map[string]string, cookie *http.Cookie) int {
	r := httptest.NewRequest(method, path, nil)
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	r.Header.Set("Sec-Fetch-Mode", "navigate")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w.Code
}

func TestCookieAuthenticatedDebitNeedsTheClientHeader(t *testing.T) {
	hits := 0
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; io.WriteString(w, `{"hits":[]}`) }))
	cookie := account(t, s, "victim@example.com")
	for _, method := range []string{"GET", "HEAD"} {
		for _, path := range []string{"/search?q=x", "/answer?q=x", "/research?q=x", "/api/search?q=x", "/api/answer?q=x", "/api/research?q=x"} {
			if c := navigate(s, method, path, nil, cookie); c != 403 {
				t.Errorf("cross-site %s %s with a session cookie: %d", method, path, c)
			}
		}
	}
	var credits, usage int
	s.db.QueryRow(`SELECT count(*) FROM credit_ledger WHERE reason='extra_request'`).Scan(&credits)
	s.db.QueryRow(`SELECT count(*) FROM retrieval_usage`).Scan(&usage)
	if hits != 0 || credits != 0 || usage != 0 {
		t.Fatalf("rejected navigations still spent: backend=%d debits=%d caps=%d", hits, credits, usage)
	}
	if c := navigate(s, "GET", "/api/research?q=x", map[string]string{"Sec-Fetch-Site": "same-origin", "X-Cosift-Client": "community"}, cookie); c != 200 {
		t.Fatalf("the app's own request: %d", c)
	}
	if c := navigate(s, "GET", "/api/search?q=x", map[string]string{"X-Cosift-Client": "community"}, cookie); c != 403 {
		t.Fatalf("header without same-origin context: %d", c)
	}
	if c := navigate(s, "GET", "/api/search?q=x", map[string]string{"Sec-Fetch-Site": "same-origin", "X-Cosift-Client": "community", "Origin": "https://evil.example"}, cookie); c != 403 {
		t.Fatalf("foreign origin: %d", c)
	}
}

func TestBearerAndGuestRetrievalNeedNoClientHeader(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"hits":[]}`) }))
	s.cfg.Shared = &fakeShared{}
	bearer := map[string]string{"Sec-Fetch-Site": "none", "Authorization": "Bearer " + sharedTestToken}
	if c := navigate(s, "GET", "/api/search?q=x", bearer, nil); c != 200 {
		t.Fatalf("bearer-only call: %d", c)
	}
	both := map[string]string{"Authorization": "Bearer " + sharedTestToken}
	if c := navigate(s, "GET", "/api/search?q=x", both, &http.Cookie{Name: cookieName, Value: sharedOtherToken}); c != 200 {
		t.Fatalf("explicit bearer next to a cookie: %d", c)
	}
	if c := navigate(s, "GET", "/api/search?q=x", nil, &http.Cookie{Name: cookieName, Value: sharedTestToken}); c != 403 {
		t.Fatalf("shared-mode cookie without the header: %d", c)
	}
	if c := navigate(s, "GET", "/api/search?q=x", map[string]string{"Sec-Fetch-Site": "none"}, nil); c != 200 {
		t.Fatalf("guest call: %d", c)
	}
}

func TestCookieDebitWithoutFetchMetadataNeedsTheClientHeader(t *testing.T) {
	hits := 0
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; io.WriteString(w, `{"hits":[]}`) }))
	cookie := account(t, s, "victim@example.com")
	get := func(path string, headers map[string]string) int {
		r := httptest.NewRequest("GET", path, nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w.Code
	}
	for _, path := range []string{"/search?q=x", "/api/search?q=x", "/api/answer?q=x", "/api/research?q=x"} {
		if c := get(path, nil); c != 403 {
			t.Errorf("a cookie-authenticated %s with no fetch metadata and no client header: %d", path, c)
		}
	}
	if hits != 0 {
		t.Fatalf("rejected requests reached the backend %d times", hits)
	}
	if c := get("/api/search?q=x", map[string]string{"X-Cosift-Client": "community"}); c != 200 {
		t.Fatalf("a client that sends the header and no fetch metadata: %d", c)
	}
}
