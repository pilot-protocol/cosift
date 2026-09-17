package community

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

type passwordShared struct {
	scriptedShared
	passwordFn func(context.Context, string, string) (sharedaccount.Issued, error)
	enrollFn   func(context.Context, string, string, string) (sharedaccount.Issued, error)
}

func (p *passwordShared) Password(ctx context.Context, email, password string) (sharedaccount.Issued, error) {
	if p.passwordFn != nil {
		return p.passwordFn(ctx, email, password)
	}
	return p.Finish(ctx, "", "")
}
func (p *passwordShared) FinishPassword(ctx context.Context, id, code, password string) (sharedaccount.Issued, error) {
	if p.enrollFn != nil {
		return p.enrollFn(ctx, id, code, password)
	}
	return p.Finish(ctx, id, code)
}
func passwordBody(email, password string) string {
	body, _ := json.Marshal(map[string]string{"email": email, "password": password})
	return string(body)
}
func TestSharedPasswordCapabilityRequiresExplicitReadyProvider(t *testing.T) {
	for _, tc := range []struct {
		name          string
		provider      sharedaccount.Provider
		enabled, want bool
	}{
		{"local", nil, true, false}, {"old OTP provider", &fakeShared{}, true, false},
		{"new provider default off", &passwordShared{}, false, false}, {"ready", &passwordShared{}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testServer(t, nil)
			s.cfg.Shared = tc.provider
			s.cfg.SharedPasswordEnabled = tc.enabled
			w := request(t, s, "GET", "/api/auth/config", nil, nil)
			expect(t, w, 200)
			var cfg map[string]bool
			if json.Unmarshal(w.Body.Bytes(), &cfg) != nil || cfg["supports_password"] != tc.want {
				t.Fatal(w.Body.String())
			}
			if !tc.want {
				expect(t, sharedJSON(s, "/api/auth/password", passwordBody("shared@example.com", "password-valid-123"), "", ""), 404)
			}
			if tc.provider != nil && !tc.want {
				expect(t, sharedJSON(s, "/api/auth/verify", `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456","password":"password-valid-123"}`, "", ""), 404)
				expect(t, sharedJSON(s, "/api/auth/verify", `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456"}`, "", ""), 200)
			}
		})
	}
}
func TestSharedPasswordUsesExistingIdentityAndNeverStoresPassword(t *testing.T) {
	s := testServer(t, nil)
	s.cfg.PublicURL = "https://community.example.com"
	s.cfg.SharedPasswordEnabled = true
	p := &passwordShared{passwordFn: func(_ context.Context, email, password string) (sharedaccount.Issued, error) {
		if email != "shared@example.com" || password != "  password-valid-123  " {
			t.Fatal("credentials changed before forwarding")
		}
		return sharedaccount.Issued{Token: sharedTestToken, UID: "0123456789abcdef"}, nil
	}}
	s.cfg.Shared = p
	u, err := s.sharedUser(context.Background(), sharedaccount.Identity{UID: "0123456789abcdef", Email: "shared@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE users SET onboarded=1,interests='["engineering"]' WHERE id=?`, u.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('password-preservation',?,17,'fixture',0)`, u.ID); err != nil {
		t.Fatal(err)
	}
	w := sharedJSON(s, "/api/auth/password", passwordBody(" SHARED@Example.com ", "  password-valid-123  "), "", "")
	expect(t, w, 200)
	var got User
	if err = json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.ID != u.ID || !got.Onboarded || len(got.Interests) != 1 {
		t.Fatalf("identity changed: %+v %v", got, err)
	}
	if strings.Contains(w.Body.String(), sharedTestToken) || strings.Contains(w.Body.String(), "password-valid") {
		t.Fatal("secret exposed")
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Value != sharedTestToken || !cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode {
		t.Fatal("invalid shared cookie")
	}
	var users, sessions, balance int
	var hash string
	if err = s.db.QueryRow(`SELECT count(*),password_hash FROM users`).Scan(&users, &hash); err != nil || users != 1 || hash != "" {
		t.Fatal("local account/password created", err)
	}
	if err = s.db.QueryRow(`SELECT count(*) FROM sessions`).Scan(&sessions); err != nil || sessions != 0 {
		t.Fatal("local session created", err)
	}
	if err = s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger WHERE user_id=?`, u.ID).Scan(&balance); err != nil || balance != 17 {
		t.Fatal("ledger changed", err)
	}
	expect(t, bearerRequest(s, "/api/me", sharedTestToken), 200)
	p.fakeShared.revoked = true
	expect(t, bearerRequest(s, "/api/me", sharedTestToken), 401)
}
func TestSharedPasswordGenericFailuresAndIssuedTokenRevocation(t *testing.T) {
	for _, tc := range []struct {
		name                string
		issueErr, verifyErr error
		identity            *sharedaccount.Identity
		status              int
		revoke              bool
	}{
		{name: "wrong or unknown", issueErr: sharedaccount.ErrUnauthorized, status: 401},
		{name: "banned", issueErr: sharedaccount.ErrBanned, status: 401},
		{name: "infrastructure", issueErr: errors.New("private upstream information"), status: 503},
		{name: "rate limit", issueErr: sharedaccount.ErrLimited, status: 429},
		{name: "revoked before use", verifyErr: sharedaccount.ErrUnauthorized, status: 401, revoke: true},
		{name: "banned before use", verifyErr: sharedaccount.ErrBanned, status: 401, revoke: true},
		{name: "UID mismatch", identity: &sharedaccount.Identity{UID: "fedcba9876543210", Email: "shared@example.com"}, status: 401, revoke: true},
		{name: "email mismatch", identity: &sharedaccount.Identity{UID: "0123456789abcdef", Email: "other@example.com"}, status: 401, revoke: true},
		{name: "verification unavailable", verifyErr: sharedaccount.ErrUnavailable, status: 503, revoke: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testServer(t, nil)
			s.cfg.SharedPasswordEnabled = true
			revoked := false
			p := &passwordShared{passwordFn: func(context.Context, string, string) (sharedaccount.Issued, error) {
				return sharedaccount.Issued{Token: sharedTestToken, UID: "0123456789abcdef"}, tc.issueErr
			}}
			p.verifyFn = func(context.Context, string) (sharedaccount.Identity, error) {
				if tc.identity != nil {
					return *tc.identity, tc.verifyErr
				}
				return sharedaccount.Identity{UID: "0123456789abcdef", Email: "shared@example.com"}, tc.verifyErr
			}
			p.revokeFn = func(_ context.Context, token string) error {
				revoked = true
				if token != sharedTestToken {
					t.Error("wrong revoked token")
				}
				return nil
			}
			s.cfg.Shared = p
			w := sharedJSON(s, "/api/auth/password", passwordBody("shared@example.com", "password-valid-123"), "", "")
			expect(t, w, tc.status)
			if tc.status == 401 && !strings.Contains(w.Body.String(), "invalid email or password") {
				t.Fatal(w.Body.String())
			}
			if revoked != tc.revoke || len(w.Result().Cookies()) != 0 || strings.Contains(w.Body.String(), "private") {
				t.Fatal("failed login leaked or changed credentials", w.Body.String())
			}
			var n int
			if err := s.db.QueryRow(`SELECT count(*) FROM users`).Scan(&n); err != nil || n != 0 {
				t.Fatal("failed login created account", err)
			}
		})
	}
}
func TestSharedPasswordEnrollmentRequiresValidOTPAndPreservesOptionalFlow(t *testing.T) {
	s := testServer(t, nil)
	s.cfg.SharedPasswordEnabled = true
	enrolls, finishes := 0, 0
	p := &passwordShared{enrollFn: func(_ context.Context, id, code, password string) (sharedaccount.Issued, error) {
		enrolls++
		if id != "01ARZ3NDEKTSV4RRFFQ69G5FAV" || password != "password-valid-123" {
			t.Fatal("changed enrollment")
		}
		if code != "123456" {
			return sharedaccount.Issued{}, sharedaccount.ErrUnauthorized
		}
		return sharedaccount.Issued{Token: sharedTestToken, UID: "0123456789abcdef"}, nil
	}}
	p.finishFn = func(context.Context, string, string) (sharedaccount.Issued, error) {
		finishes++
		return sharedaccount.Issued{Token: sharedTestToken, UID: "0123456789abcdef"}, nil
	}
	s.cfg.Shared = p
	for _, password := range []string{"", strings.Repeat("a", 11), strings.Repeat("a", 257), strings.Repeat("é", 129)} {
		body, _ := json.Marshal(map[string]string{"request_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "code": "123456", "password": password})
		expect(t, sharedJSON(s, "/api/auth/verify", string(body), "", ""), 400)
	}
	if enrolls != 0 || finishes != 0 {
		t.Fatal("invalid enrollment reached provider")
	}
	w := sharedJSON(s, "/api/auth/verify", `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"000000","password":"password-valid-123"}`, "", "")
	expect(t, w, 401)
	if len(w.Result().Cookies()) != 0 {
		t.Fatal("invalid OTP signed in")
	}
	expect(t, sharedJSON(s, "/api/auth/verify", `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456","password":"password-valid-123"}`, "", ""), 200)
	expect(t, sharedJSON(s, "/api/auth/verify", `{"request_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","code":"123456"}`, "", ""), 200)
	if enrolls != 2 || finishes != 1 {
		t.Fatal("OTP and password enrollment confused", enrolls, finishes)
	}
}
func TestSharedPasswordInvalidCredentialsAndIPRateLimit(t *testing.T) {
	s := testServer(t, nil)
	s.cfg.SharedPasswordEnabled = true
	calls := 0
	s.cfg.Shared = &passwordShared{passwordFn: func(context.Context, string, string) (sharedaccount.Issued, error) {
		calls++
		return sharedaccount.Issued{}, sharedaccount.ErrUnauthorized
	}}
	for _, body := range []string{passwordBody("invalid", "password-valid-123"), passwordBody("shared@example.com", "short"), passwordBody("shared@example.com", strings.Repeat("a", 257))} {
		expect(t, sharedJSON(s, "/api/auth/password", body, "", "192.0.2.3:1"), 401)
	}
	if calls != 0 {
		t.Fatal("invalid credentials reached upstream")
	}
	for i := 0; i < 10; i++ {
		expect(t, sharedJSON(s, "/api/auth/password", passwordBody("shared@example.com", "password-valid-123"), "", "192.0.2.1:1"), 401)
	}
	expect(t, sharedJSON(s, "/api/auth/password", passwordBody("shared@example.com", "password-valid-123"), "", "192.0.2.1:2"), 429)
	expect(t, sharedJSON(s, "/api/auth/password", passwordBody("shared@example.com", "password-valid-123"), "", "192.0.2.2:1"), 401)
	if calls != 11 {
		t.Fatal("rate limited request reached upstream", calls)
	}
}
