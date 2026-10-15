package community

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const hostName = "__Host-cosift_session"

func httpsServer(t *testing.T) *Server {
	t.Helper()
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"hits":[]}`) }))
	s.cfg.PublicURL = "https://community.example.com"
	return s
}

func cookieNamed(w *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, c := range w.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func hostToken(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	c := cookieNamed(w, hostName)
	if c == nil {
		t.Fatalf("no %s cookie issued: %v", hostName, w.Result().Cookies())
	}
	return c.Value
}

func withCookies(t *testing.T, s *Server, method, path string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Cosift-Client", "community")
	for _, c := range cookies {
		r.AddCookie(c)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

func register(t *testing.T, s *Server, email string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", "/api/register", strings.NewReader(`{"email":"`+email+`","name":"Curious Person","password":"a-test-password-123"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Cosift-Client", "community")
	for _, c := range cookies {
		r.AddCookie(c)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	expect(t, w, 200)
	return w
}

func TestHTTPSSessionCookieUsesTheHostPrefix(t *testing.T) {
	s := httpsServer(t)
	w := register(t, s, "host@example.com")
	c := cookieNamed(w, hostName)
	if c == nil || c.Value == "" || !c.Secure || !c.HttpOnly || c.Path != "/" || c.Domain != "" || c.SameSite != http.SameSiteLaxMode || c.MaxAge <= 0 {
		t.Fatalf("session cookie is not a valid __Host- cookie: %+v", c)
	}
	if old := cookieNamed(w, cookieName); old == nil || old.MaxAge != -1 || old.Value != "" {
		t.Fatalf("the legacy cookie was not cleared: %+v", old)
	}
	if got := request(t, s, "GET", "/api/me", nil, c); got.Code != 200 {
		t.Fatalf("the prefixed cookie does not authenticate: %d", got.Code)
	}
}

func TestLegacySessionCookieIsStillAccepted(t *testing.T) {
	s := httpsServer(t)
	token := hostToken(t, register(t, s, "legacy@example.com"))
	legacy := &http.Cookie{Name: cookieName, Value: token}
	if c := request(t, s, "GET", "/api/me", nil, legacy).Code; c != 200 {
		t.Fatalf("legacy cookie after the rename: %d", c)
	}
}

func TestPlantedLegacyCookieCannotOverrideThePrefixedOne(t *testing.T) {
	s := httpsServer(t)
	token := hostToken(t, register(t, s, "victim@example.com"))
	attacker := hostToken(t, register(t, s, "attacker@example.com"))
	planted := &http.Cookie{Name: cookieName, Value: attacker}
	good := &http.Cookie{Name: hostName, Value: token}
	for _, order := range [][]*http.Cookie{{planted, good}, {good, planted}} {
		w := withCookies(t, s, "GET", "/api/me", order...)
		if w.Code != 200 || !strings.Contains(w.Body.String(), "victim@example.com") {
			t.Fatalf("a planted legacy cookie won: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestLoginReplacesTheLegacyCookieAndItsSession(t *testing.T) {
	s := httpsServer(t)
	old := hostToken(t, register(t, s, "move@example.com"))
	legacy := &http.Cookie{Name: cookieName, Value: old}
	w := register(t, s, "other@example.com", legacy)
	if cookieNamed(w, hostName) == nil {
		t.Fatal("no prefixed cookie issued")
	}
	if c := request(t, s, "GET", "/api/me", nil, legacy).Code; c != 401 {
		t.Fatalf("the replaced legacy session still works: %d", c)
	}
}

func TestLogoutClearsEverySessionCookie(t *testing.T) {
	s := httpsServer(t)
	token := hostToken(t, register(t, s, "bye@example.com"))
	w := withCookies(t, s, "POST", "/api/logout", &http.Cookie{Name: hostName, Value: token}, &http.Cookie{Name: cookieName, Value: token})
	expect(t, w, 200)
	for _, name := range []string{hostName, cookieName} {
		if c := cookieNamed(w, name); c == nil || c.MaxAge != -1 || c.Value != "" {
			t.Fatalf("%s not cleared: %+v", name, c)
		}
	}
	if c := request(t, s, "GET", "/api/me", nil, &http.Cookie{Name: hostName, Value: token}).Code; c != 401 {
		t.Fatalf("the session survived logout: %d", c)
	}
}

func TestExpiredSessionClearsTheCookieItArrivedWith(t *testing.T) {
	s := httpsServer(t)
	for _, name := range []string{hostName, cookieName} {
		w := request(t, s, "GET", "/api/me", nil, &http.Cookie{Name: name, Value: "revoked"})
		expect(t, w, 401)
		if c := cookieNamed(w, name); c == nil || c.MaxAge != -1 || len(w.Result().Cookies()) != 1 {
			t.Fatalf("%s: stale cookie not cleared: %v", name, w.Result().Cookies())
		}
	}
}

func TestSharedModeUsesThePrefixedCookie(t *testing.T) {
	s := httpsServer(t)
	s.cfg.Shared = &fakeShared{}
	w := postFrom(s, "/api/auth/verify", `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456"}`, "", "198.51.100.7:1234")
	expect(t, w, 200)
	c := cookieNamed(w, hostName)
	if c == nil || c.Value != sharedTestToken || !c.Secure || c.Path != "/" || c.Domain != "" {
		t.Fatalf("shared session cookie: %+v", c)
	}
	if cookieNamed(w, cookieName) == nil {
		t.Fatal("the legacy cookie was not cleared")
	}
	for _, name := range []string{hostName, cookieName} {
		if got := request(t, s, "GET", "/api/me", nil, &http.Cookie{Name: name, Value: sharedTestToken}); got.Code != 200 {
			t.Fatalf("%s did not authenticate in shared mode: %d", name, got.Code)
		}
	}
}

func TestPlainHTTPKeepsTheUnprefixedCookie(t *testing.T) {
	s := testServer(t, nil)
	w := register(t, s, "local@example.com")
	if c := cookieNamed(w, cookieName); c == nil || c.Secure || len(w.Result().Cookies()) != 1 {
		t.Fatalf("plain-HTTP cookie: %v", w.Result().Cookies())
	}
}

func TestLogoutRevokesTheSessionOfEveryCookieName(t *testing.T) {
	s := httpsServer(t)
	host := hostToken(t, register(t, s, "twice@example.com"))
	login := httptest.NewRequest("POST", "/api/login", strings.NewReader(`{"email":"twice@example.com","password":"a-test-password-123"}`))
	login.Header.Set("Content-Type", "application/json")
	login.Header.Set("X-Cosift-Client", "community")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, login)
	expect(t, w, 200)
	legacy := hostToken(t, w)
	if host == legacy {
		t.Fatal("expected two distinct sessions")
	}
	expect(t, withCookies(t, s, "POST", "/api/logout", &http.Cookie{Name: hostName, Value: host}, &http.Cookie{Name: cookieName, Value: legacy}), 200)
	for _, token := range []string{host, legacy} {
		if c := request(t, s, "GET", "/api/me", nil, &http.Cookie{Name: hostName, Value: token}).Code; c != 401 {
			t.Fatalf("a session survived logout: %d", c)
		}
	}
}

func TestSharedMemberRetrievalIsMeteredWithEitherCookieName(t *testing.T) {
	for _, name := range []string{hostName, cookieName} {
		s := httpsServer(t)
		s.cfg.Shared = &fakeShared{}
		expect(t, withCookies(t, s, "GET", "/api/search?q=x", &http.Cookie{Name: name, Value: sharedTestToken}), 200)
		if charged, _, guests := spent(t, s); charged != 1 || guests != 0 {
			t.Fatalf("%s: a signed-in member was charged %d credits and left %d guest cooldowns, want 1 and 0", name, charged, guests)
		}
	}
}
