package community

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const sharedTestToken = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
const sharedOtherToken = "ck_2_MFRGGZDFMZTWQ2LKNNWG23TPOBYXE43UOJUW4ZY"

type fakeShared struct {
	err         error
	calls       int
	token, tool string
	args        map[string]any
	revoked     bool
	namespace   string
}

func (f *fakeShared) Namespace() string {
	if f.namespace != "" {
		return f.namespace
	}
	return "test/staging"
}
func (f *fakeShared) Verify(_ context.Context, token string) (sharedaccount.Identity, error) {
	if f.err != nil {
		return sharedaccount.Identity{}, f.err
	}
	if token == sharedTestToken && !f.revoked {
		return sharedaccount.Identity{UID: "0123456789abcdef", Email: "shared@example.com"}, nil
	}
	if token == sharedOtherToken {
		return sharedaccount.Identity{UID: "fedcba9876543210", Email: "other@example.com"}, nil
	}
	return sharedaccount.Identity{}, sharedaccount.ErrUnauthorized
}
func (f *fakeShared) Start(context.Context, string) (sharedaccount.Challenge, error) {
	return sharedaccount.Challenge{RequestID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", ExpiresAt: time.Now().Add(time.Minute)}, f.err
}
func (f *fakeShared) Finish(context.Context, string, string) (sharedaccount.Issued, error) {
	return sharedaccount.Issued{Token: sharedTestToken, UID: "0123456789abcdef"}, f.err
}
func (f *fakeShared) Revoke(context.Context, string) error {
	if f.err != nil {
		return f.err
	}
	f.revoked = true
	return nil
}
func (f *fakeShared) Call(_ context.Context, token, tool string, args map[string]any) (map[string]any, error) {
	f.calls++
	f.token = token
	f.tool = tool
	f.args = args
	return map[string]any{"action": "list", "topics": []any{}}, f.err
}
func bearerRequest(s *Server, path, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", path, nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func TestSharedTokensAccountIsolationQuotasAndMCPParameters(t *testing.T) {
	hits := 0
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.Header.Get("Authorization") != "" {
			t.Error("credential leaked to engine")
		}
		if r.URL.Query().Get("k") != "20" || r.URL.Query().Get("retriever") != "bm25" {
			t.Error("lost MCP search contract")
		}
		_, _ = w.Write([]byte(`{"hits":[]}`))
	}))
	f := &fakeShared{}
	s.cfg.Shared = f
	s.cfg.MemberFreeRPM = 1
	path := "/search?q=rust&k=20&retriever=bm25"
	expect(t, bearerRequest(s, path, sharedTestToken), 200)
	expect(t, bearerRequest(s, path, sharedTestToken), 200)
	expect(t, bearerRequest(s, path, sharedOtherToken), 200)
	if hits != 3 {
		t.Fatal("quota did not isolate users", hits)
	}
	var creditBalance int
	if err := s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger`).Scan(&creditBalance); err != nil || creditBalance != monthlyFreeCredits-1 {
		t.Fatalf("shared bearer request did not use monthly credits: %d %v", creditBalance, err)
	}
	f.err = sharedaccount.ErrUnavailable
	expect(t, bearerRequest(s, path, sharedOtherToken), 503)
	f.err = sharedaccount.ErrBanned
	expect(t, bearerRequest(s, path, sharedOtherToken), 403)
	f.err = nil
	expect(t, bearerRequest(s, path, "invalid"), 401)
	expect(t, bearerRequest(s, "/search?q=x&k=999", sharedTestToken), 400)
	if hits != 3 {
		t.Fatal("failed authentication reached engine")
	}
}
func TestSharedLinkPreservesLocalDataAndInvalidatesLegacySessions(t *testing.T) {
	s := testServer(t, nil)
	old := account(t, s, "shared@example.com")
	expect(t, request(t, s, "POST", "/api/saved", map[string]string{"query": "my saved query"}, old), 200)
	var localID string
	_ = s.db.QueryRow(`SELECT id FROM users WHERE email='shared@example.com'`).Scan(&localID)
	_, err := s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('shared-credit',?,42,'test',0)`, localID)
	if err != nil {
		t.Fatal(err)
	}
	expect(t, request(t, s, "GET", "/api/credits", nil, old), 200)
	s.cfg.Shared = &fakeShared{}
	expect(t, bearerRequest(s, "/api/me", sharedTestToken), 200)
	w := bearerRequest(s, "/api/saved", sharedTestToken)
	expect(t, w, 200)
	if !strings.Contains(w.Body.String(), "my saved query") {
		t.Fatal("lost saved query")
	}
	w = bearerRequest(s, "/api/credits", sharedTestToken)
	expect(t, w, 200)
	if !strings.Contains(w.Body.String(), `"balance":1042`) {
		t.Fatal("lost credits", w.Body)
	}
	var grants int
	if err := s.db.QueryRow(`SELECT count(*) FROM credit_ledger WHERE user_id=? AND reason='monthly_free'`, localID).Scan(&grants); err != nil || grants != 1 {
		t.Fatalf("account linking duplicated monthly grant: %d %v", grants, err)
	}
	expect(t, request(t, s, "GET", "/api/me", nil, old), 401)
	var sessions int
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE user_id=?`, localID).Scan(&sessions)
	if sessions != 0 {
		t.Fatal("old sessions survived link")
	}
	expect(t, request(t, s, "POST", "/api/login", map[string]string{"email": "shared@example.com", "password": "a-test-password-123"}, nil), 409)
	other := bearerRequest(s, "/api/saved", sharedOtherToken)
	expect(t, other, 200)
	if strings.Contains(other.Body.String(), "my saved query") {
		t.Fatal("cross-account leak")
	}
}
func TestSharedOTPTopicsAndRevocation(t *testing.T) {
	s := testServer(t, nil)
	f := &fakeShared{}
	s.cfg.Shared = f
	expect(t, request(t, s, "POST", "/api/auth/start", map[string]string{"email": "shared@example.com"}, nil), 200)
	w := request(t, s, "POST", "/api/auth/verify", map[string]string{"request_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "code": "123456"}, nil)
	expect(t, w, 200)
	if strings.Contains(w.Body.String(), sharedTestToken) {
		t.Fatal("token exposed to browser JavaScript")
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode {
		t.Fatal("insecure cookie")
	}
	c := cookies[0]
	expect(t, request(t, s, "PUT", "/api/interests", map[string]any{"interests": []string{"Rust"}}, c), 200)
	if f.tool != "cosift_topics" || f.token != sharedTestToken || f.args["action"] != "add" {
		t.Fatal("onboarding not shared")
	}
	expect(t, request(t, s, "POST", "/api/shared", map[string]any{"tool": "cosift_request", "topic": "Rust"}, c), 200)
	if f.tool != "cosift_request" {
		t.Fatal("article request not forwarded")
	}
	expect(t, request(t, s, "POST", "/api/shared", map[string]any{"tool": "arbitrary"}, c), 400)
	f.err = sharedaccount.ErrUnavailable
	expect(t, request(t, s, "POST", "/api/logout", map[string]any{}, c), 503)
	if f.revoked {
		t.Fatal("false revocation")
	}
	f.err = nil
	expect(t, request(t, s, "POST", "/api/logout", map[string]any{}, c), 200)
	expect(t, bearerRequest(s, "/api/me", sharedTestToken), 401)
}
func TestSharedNamespaceCannotSwitchOrDisable(t *testing.T) {
	s := testServer(t, nil)
	s.cfg.Shared = &fakeShared{}
	if err := s.bindSharedNamespace(); err != nil {
		t.Fatal(err)
	}
	s.cfg.Shared = &fakeShared{namespace: "test/(default)"}
	if err := s.bindSharedNamespace(); err == nil {
		t.Fatal("staging account IDs used in production")
	}
	s.cfg.Shared = nil
	if err := s.bindSharedNamespace(); err == nil {
		t.Fatal("password fallback after linking")
	}
}
func TestSharedInterestsFailureDoesNotCompleteOnboarding(t *testing.T) {
	s := testServer(t, nil)
	f := &fakeShared{}
	s.cfg.Shared = f
	expect(t, bearerRequest(s, "/api/me", sharedTestToken), 200)
	f.err = errors.New("upstream disconnected")
	// Call interests directly after authentication to inject an MCP-only outage.
	var id string
	_ = s.db.QueryRow(`SELECT id FROM users WHERE email='shared@example.com'`).Scan(&id)
	r := httptest.NewRequest("PUT", "/api/interests", strings.NewReader(`{"interests":["Rust"]}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+sharedTestToken)
	w := httptest.NewRecorder()
	s.interests(w, r, User{ID: id})
	expect(t, w, 503)
	var onboarded int
	_ = s.db.QueryRow(`SELECT onboarded FROM users WHERE id=?`, id).Scan(&onboarded)
	if onboarded != 0 {
		t.Fatal("failed sync marked complete")
	}
}
