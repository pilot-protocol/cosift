package community

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

func testServer(t *testing.T, backend http.Handler) *Server {
	t.Helper()
	if backend == nil {
		backend = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })
	}
	b := httptest.NewServer(backend)
	t.Cleanup(b.Close)
	s, err := Open(Config{DataDir: t.TempDir(), Backend: b.URL, PublicURL: "http://localhost:7780", AdminToken: "backend-secret"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	setTestPage(s, `<html><title>Technical documentation</title><body><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></body></html>`)
	return s
}

type pageTransport func(*http.Request) (*http.Response, error)

func (f pageTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func setTestPage(s *Server, body string) {
	s.pageClient = &http.Client{Transport: pageTransport(func(r *http.Request) (*http.Response, error) {
		value := body
		kind := "text/html"
		if r.URL.Path == "/robots.txt" {
			value = "User-agent: *\nAllow: /\n"
			kind = "text/plain"
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{kind}}, Body: io.NopCloser(strings.NewReader(value)), Request: r}, nil
	})}
	s.moderationRobots = crawler.NewRobots(s.pageClient, "Cosift-Community/1.0")
}

func request(t *testing.T, s *Server, method, path string, body any, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(b)
	}
	r := httptest.NewRequest(method, path, reader)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Cosift-Client", "community")
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func expect(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status %d, want %d: %s", w.Code, status, w.Body.String())
	}
}
func account(t *testing.T, s *Server, email string) *http.Cookie {
	t.Helper()
	w := request(t, s, "POST", "/api/register", map[string]string{"email": email, "name": "Curious Person", "password": "a-test-password-123"}, nil)
	expect(t, w, 200)
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatal("missing session")
	}
	if !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode {
		t.Fatal("unsafe cookie")
	}
	return cookies[0]
}

func TestAccountLifecycleAndIsolation(t *testing.T) {
	s := testServer(t, nil)
	alice := account(t, s, "Alice@example.com")
	bob := account(t, s, "bob@example.com")
	expect(t, request(t, s, "GET", "/api/me", nil, nil), 401)
	w := request(t, s, "PUT", "/api/interests", map[string]any{"interests": []string{" Science ", "science", "Open source"}}, alice)
	expect(t, w, 200)
	var u User
	if err := json.Unmarshal(w.Body.Bytes(), &u); err != nil {
		t.Fatal(err)
	}
	if !u.Onboarded || len(u.Interests) != 2 || u.Email != "alice@example.com" {
		t.Fatalf("profile %+v", u)
	}
	w = request(t, s, "POST", "/api/saved", map[string]string{"query": "distributed databases"}, alice)
	expect(t, w, 200)
	var saved SavedSearch
	json.Unmarshal(w.Body.Bytes(), &saved)
	expect(t, request(t, s, "POST", "/api/saved", map[string]string{"query": saved.Query}, alice), 200)
	w = request(t, s, "GET", "/api/saved", nil, bob)
	expect(t, w, 200)
	if strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatal("cross-account saved search leak")
	}
	expect(t, request(t, s, "DELETE", "/api/saved/"+saved.ID, nil, bob), 404)
	w = request(t, s, "GET", "/api/saved", nil, alice)
	var list []SavedSearch
	json.Unmarshal(w.Body.Bytes(), &list)
	if len(list) != 1 {
		t.Fatalf("saved duplicates: %s", w.Body.String())
	}
	var hash, salt, sessionHash string
	if err := s.db.QueryRow(`SELECT password_hash,salt FROM users WHERE id=?`, u.ID).Scan(&hash, &salt); err != nil {
		t.Fatal(err)
	}
	if hash == "a-test-password-123" || len(hash) != 64 || salt == "" {
		t.Fatal("password not salted and hashed")
	}
	if err := s.db.QueryRow(`SELECT hash FROM sessions WHERE user_id=?`, u.ID).Scan(&sessionHash); err != nil {
		t.Fatal(err)
	}
	if sessionHash == alice.Value {
		t.Fatal("raw session stored")
	}
	expect(t, request(t, s, "POST", "/api/logout", map[string]string{}, alice), 200)
	expect(t, request(t, s, "GET", "/api/me", nil, alice), 401)
	expect(t, request(t, s, "POST", "/api/login", map[string]string{"email": "alice@example.com", "password": "incorrect-password"}, nil), 401)
	w = request(t, s, "POST", "/api/login", map[string]string{"email": "alice@example.com", "password": "a-test-password-123"}, nil)
	expect(t, w, 200)
	alice = w.Result().Cookies()[0]
	expect(t, request(t, s, "DELETE", "/api/saved/"+saved.ID, nil, alice), 200)
	if _, err := s.db.Exec(`UPDATE sessions SET expires_at=? WHERE hash=?`, time.Now().Add(-time.Minute).Unix(), tokenHash(alice.Value)); err != nil {
		t.Fatal(err)
	}
	expect(t, request(t, s, "GET", "/api/me", nil, alice), 401)
}

func TestCSRFAndPrivateAssets(t *testing.T) {
	s := testServer(t, nil)
	for _, headers := range []map[string]string{{}, {"X-Cosift-Client": "community", "Origin": "https://evil.example"}, {"X-Cosift-Client": "community", "Sec-Fetch-Site": "cross-site"}} {
		r := httptest.NewRequest("POST", "/api/login", strings.NewReader(`{}`))
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		expect(t, w, 403)
	}
	for _, path := range []string{"/", "/app.js", "/style.css"} {
		w := request(t, s, "GET", path, nil, nil)
		expect(t, w, 200)
		if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Security-Policy") == "" {
			t.Fatal("missing response protections")
		}
	}
	expect(t, request(t, s, "GET", "/community.db", nil, nil), 404)
}

func TestSearchBackendContract(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" || r.URL.Query().Get("q") != "solar panels" || r.URL.Query().Get("retriever") != "" {
			t.Errorf("wrong search: %s", r.URL.String())
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("account/backend credentials leaked into search")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"hits":[{"title":"Solar guide","url":"https://example.com/solar","excerpt":"A useful guide."}]}`)
	}))
	cookie := account(t, s, "search@example.com")
	w := request(t, s, "GET", "/api/search?q=solar+panels", nil, cookie)
	expect(t, w, 200)
	if !strings.Contains(w.Body.String(), "Solar guide") {
		t.Fatal(w.Body.String())
	}
	expect(t, request(t, s, "GET", "/api/search?q=", nil, cookie), 400)
}

func csvRequest(t *testing.T, s *Server, cookie *http.Cookie, csv string) *httptest.ResponseRecorder {
	t.Helper()
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	f, err := mw.CreateFormFile("file", "sources.csv")
	if err != nil {
		t.Fatal(err)
	}
	io.WriteString(f, csv)
	mw.Close()
	r := httptest.NewRequest("POST", "/api/submissions", &b)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	r.Header.Set("X-Cosift-Client", "community")
	r.AddCookie(cookie)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

func TestCSVAtomicValidationAndAttribution(t *testing.T) {
	s := testServer(t, nil)
	alice := account(t, s, "csv@example.com")
	bob := account(t, s, "other@example.com")
	w := csvRequest(t, s, alice, "title,url\nGood,https://example.com/guide\nBad,http://127.0.0.1/private\n")
	expect(t, w, 400)
	w = request(t, s, "GET", "/api/submissions", nil, alice)
	if strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatal("invalid batch partially saved")
	}
	w = csvRequest(t, s, alice, "title,url\n\"A title, with comma\",https://example.com/guide#part\nAgain,https://example.com/guide\n")
	expect(t, w, 202)
	if !strings.Contains(w.Body.String(), `"accepted":1`) {
		t.Fatal(w.Body.String())
	}
	w = request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, alice)
	expect(t, w, 202)
	if !strings.Contains(w.Body.String(), `"duplicates":1`) {
		t.Fatal(w.Body.String())
	}
	w = request(t, s, "GET", "/api/submissions", nil, bob)
	if strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatal("cross-account contribution leak")
	}
	for _, bad := range []string{"title,link\nGuide,https://example.com\n", "url\n\"unclosed", "url\n"} {
		expect(t, csvRequest(t, s, alice, bad), 400)
	}
}

func TestDeliveryRetrySurvivesRestart(t *testing.T) {
	attempts := 0
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/community-moderate" {
			io.WriteString(w, `{"decision":"allow","category":"safe"}`)
			return
		}
		attempts++
		if r.URL.Path != "/admin/community-enqueue" || r.Header.Get("Authorization") != "Bearer backend-secret" {
			t.Errorf("incorrect delivery: %s", r.URL.Path)
		}
		var in struct {
			URL      string           `json:"url"`
			Artifact *json.RawMessage `json:"artifact"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		if in.URL != "https://example.com/guide" || in.Artifact != nil {
			t.Errorf("wrong payload: %+v", in)
		}
		if attempts == 1 {
			w.WriteHeader(503)
			return
		}
		io.WriteString(w, `{"queued":"https://example.com/guide"}`)
	}))
	cookie := account(t, s, "retry@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	var state string
	var next int64
	s.db.QueryRow(`SELECT status,next_attempt FROM submissions`).Scan(&state, &next)
	if state != "pending" || next <= time.Now().Unix() {
		t.Fatalf("failed delivery not scheduled: %s %d", state, next)
	}
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatal("backoff ignored")
	}
	cfg := s.cfg
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	setTestPage(reopened, `<html><body><article>This educational documentation explains programming language design, safe systems development, and useful public research for software engineers.</article></body></html>`)
	expect(t, request(t, reopened, "GET", "/api/me", nil, cookie), 200)
	if _, err := reopened.db.Exec(`UPDATE submissions SET next_attempt=0`); err != nil {
		t.Fatal(err)
	}
	if err := reopened.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	reopened.db.QueryRow(`SELECT status FROM submissions`).Scan(&state)
	if state != "queued" || attempts != 2 {
		t.Fatalf("retry state %s, attempts %d", state, attempts)
	}
	if err := reopened.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatal("delivered job sent again")
	}
}

func TestContributionQuotaIsAtomic(t *testing.T) {
	s := testServer(t, nil)
	cookie := account(t, s, "quota@example.com")
	var id string
	s.db.QueryRow(`SELECT id FROM users`).Scan(&id)
	_, err := s.db.Exec(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<500) INSERT INTO submissions(id,user_id,url,created_at) SELECT 'id'||i,?,'https://example.com/'||i,? FROM n`, id, time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/new"}}, cookie), 429)
	var count int
	s.db.QueryRow(`SELECT count(*) FROM submissions`).Scan(&count)
	if count != 500 {
		t.Fatalf("over-quota write persisted: %d", count)
	}
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/1"}}, cookie), 202)
}

func TestNormalizeURL(t *testing.T) {
	for _, raw := range []string{"file:///etc/passwd", "http://localhost/x", "http://10.0.0.1", "http://[::1]", "https://user:pass@example.com", "https://example.com:9000", "https://a.internal", "javascript:alert(1)", "https://a.localhost"} {
		if _, err := NormalizeURL(raw); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	got, err := NormalizeURL(" HTTPS://Example.COM/a?b=c#section ")
	if err != nil || got != "https://example.com/a?b=c" {
		t.Fatalf("normalize %q %v", got, err)
	}
}

func TestPermanentArtifactRejectionIsNotRetried(t *testing.T) {
	calls := 0
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin/community-moderate" {
			io.WriteString(w, `{"decision":"allow","category":"safe"}`)
			return
		}
		calls++
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	cookie := account(t, s, "held@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	for i := 0; i < 2; i++ {
		if err := s.dispatch(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	var state string
	if err := s.db.QueryRow(`SELECT status FROM submissions`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "unverified" || calls != 1 {
		t.Fatalf("state=%s deliveries=%d", state, calls)
	}
}
