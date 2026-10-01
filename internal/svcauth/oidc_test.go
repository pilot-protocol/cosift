package svcauth

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/idtoken"

	"github.com/pilot-protocol/cosift/internal/svcauth/svcauthtest"
)

func TestOIDCAccepts(t *testing.T) {
	h := newHarness(t, baseConfig())
	w := h.do("POST", "/v1/search", `{}`, bearerAuth(h.token(synthSub, synthEmail)))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d body %s", w.Code, w.Body.String())
	}
	calls := h.handlerCalls()
	if len(calls) != 1 || calls[0].p.ID != "synth-prod" || calls[0].p.Env != "prod" || !calls[0].p.Has("retrieve:read") {
		t.Fatalf("handler saw %+v", calls)
	}
	for _, iss := range []string{"https://accounts.google.com", "accounts.google.com"} {
		c := svcauthtest.Claims(synthSub, synthEmail, time.Now())
		c["iss"] = iss
		if w := h.do("POST", "/v1/search", `{}`, bearerAuth(h.signer.Token(c))); w.Code != http.StatusOK {
			t.Errorf("iss %s: status %d", iss, w.Code)
		}
	}
}

// Each token fails one check, is the uniform 401 on
// the wire, and names its reason only in the log.
func TestOIDCRejects(t *testing.T) {
	h := newHarness(t, lenient(baseConfig()))
	other := svcauthtest.NewSigner("kid-1")
	now := h.clock.Now()
	claims := func(edit func(c map[string]any)) map[string]any {
		c := svcauthtest.Claims(synthSub, synthEmail, now)
		edit(c)
		return c
	}
	cases := []struct {
		name   string
		token  string
		reason string
	}{
		{"expired", h.signer.Token(claims(func(c map[string]any) {
			c["iat"], c["exp"] = now.Add(-2*time.Hour).Unix(), now.Add(-time.Hour).Unix()
		})), "expired"},
		{"exp missing", h.signer.Token(claims(func(c map[string]any) { delete(c, "exp") })), "expired"},
		{"wrong aud", h.signer.Token(claims(func(c map[string]any) { c["aud"] = "https://cosift.pilotprotocol.network/v1/" })), "bad_aud"},
		{"other service's aud", h.signer.Token(claims(func(c map[string]any) { c["aud"] = "https://example.run.app" })), "bad_aud"},
		{"aud as an array", h.signer.Token(claims(func(c map[string]any) { c["aud"] = []string{svcauthtest.Audience} })), "bad_aud"},
		{"wrong iss", h.signer.Token(claims(func(c map[string]any) { c["iss"] = "https://securetoken.google.com/x" })), "bad_iss"},
		{"iss missing", h.signer.Token(claims(func(c map[string]any) { delete(c, "iss") })), "bad_iss"},
		{"ES256 (IAP)", h.signer.TokenWithHeader(map[string]any{"alg": "ES256", "kid": "kid-1"}, claims(func(map[string]any) {})), "bad_alg"},
		{"ES256 short signature", svcauthtest.Unsigned(`{"alg":"ES256","kid":"kid-1"}`, string(mustJSON(claims(func(map[string]any) {}))), []byte{1, 2, 3}), "bad_alg"},
		{"alg none", svcauthtest.Unsigned(`{"alg":"none","kid":"kid-1"}`, string(mustJSON(claims(func(map[string]any) {}))), []byte{0}), "bad_alg"},
		{"HS256", h.signer.TokenWithHeader(map[string]any{"alg": "HS256", "kid": "kid-1"}, claims(func(map[string]any) {})), "bad_alg"},
		{"kid empty", h.signer.TokenWithHeader(map[string]any{"alg": "RS256", "kid": ""}, claims(func(map[string]any) {})), "bad_alg"},
		{"kid missing", h.signer.TokenWithHeader(map[string]any{"alg": "RS256"}, claims(func(map[string]any) {})), "bad_alg"},
		{"unverified email", h.signer.Token(claims(func(c map[string]any) { c["email_verified"] = false })), "email_unverified"},
		{"email_verified as the string true", h.signer.Token(claims(func(c map[string]any) { c["email_verified"] = "true" })), "email_unverified"},
		{"email_verified missing", h.signer.Token(claims(func(c map[string]any) { delete(c, "email_verified") })), "email_unverified"},
		{"unknown sub", h.signer.Token(claims(func(c map[string]any) { c["sub"] = "999999999999999999999" })), "unknown_principal"},
		{"known email, different sub", h.signer.Token(claims(func(c map[string]any) { c["sub"] = "100000000000000000099" })), "unknown_principal"},
		{"known sub, different email", h.signer.Token(claims(func(c map[string]any) { c["email"] = mcpEmail })), "unknown_principal"},
		{"known sub, upper-case email", h.signer.Token(claims(func(c map[string]any) { c["email"] = strings.ToUpper(synthEmail) })), "unknown_principal"},
		{"no email claim", h.signer.Token(claims(func(c map[string]any) { delete(c, "email") })), "unknown_principal"},
		{"exp - iat over 3600", h.signer.Token(claims(func(c map[string]any) { c["iat"] = now.Add(-2 * time.Minute).Unix() })), "bad_lifetime"},
		{"iat missing", h.signer.Token(claims(func(c map[string]any) { delete(c, "iat") })), "bad_lifetime"},
		{"bad signature", other.Token(claims(func(map[string]any) {})), "bad_signature"},
		{"signature over another payload", func() string {
			good := strings.Split(h.token(synthSub, synthEmail), ".")
			forged := strings.Split(h.signer.Token(claims(func(c map[string]any) { c["sub"] = mcpSub; c["email"] = mcpEmail })), ".")
			return forged[0] + "." + forged[1] + "." + good[2]
		}(), "bad_signature"},
		{"header not JSON", svcauthtest.Unsigned(`not json`, `{}`, []byte{1}), "malformed_token"},
		{"payload not an object", svcauthtest.Unsigned(`{"alg":"RS256","kid":"kid-1"}`, `[1,2]`, []byte{1}), "malformed_token"},
		{"padded segment", "eyJhbGciOiJSUzI1NiJ9=.e30.c2ln", "unknown_credential"},
		{"two segments", "eyJhbGciOiJSUzI1NiJ9.e30", "unknown_credential"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := h.src.Fetches()
			w := h.do("POST", "/v1/search", `{}`, bearerAuth(tc.token))
			wantUnauthenticated(t, w)
			if got := h.lastReason(); got != tc.reason {
				t.Fatalf("reason %q, want %q", got, tc.reason)
			}
			if h.src.Fetches() != before {
				t.Fatal("a rejected token caused a certificate fetch")
			}
		})
	}
	if n := len(h.handlerCalls()); n != 0 {
		t.Fatalf("handler reached %d times", n)
	}
	m := h.metrics()
	for _, want := range []string{`cosift_v1_auth_failures_total{reason="bad_alg"} 6`, `cosift_v1_auth_failures_total{reason="expired"} 2`, `cosift_v1_requests_total{principal="-",route="POST /v1/search",code="401"} 29`} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics lack %s\n%s", want, m)
		}
	}
}

// The decision is taken on the payload Validate returned, not on the one the
// pre-checks read.
func TestOIDCRechecksVerifiedPayload(t *testing.T) {
	h := newHarness(t, baseConfig())
	good := h.token(synthSub, synthEmail)
	c := svcauthtest.Claims(synthSub, synthEmail, h.clock.Now())
	c["email_verified"] = false
	h.svc.oidc.validator = fixedPayloadValidator{claims: c}
	w := h.do("POST", "/v1/search", `{}`, bearerAuth(good))
	wantUnauthenticated(t, w)
	if got := h.lastReason(); got != "email_unverified" {
		t.Fatalf("reason %q", got)
	}
}

// fixedPayloadValidator stands in for the library and returns claims.
type fixedPayloadValidator struct{ claims map[string]any }

func (v fixedPayloadValidator) Validate(_ context.Context, _, _ string) (*idtoken.Payload, error) {
	return idtoken.ParsePayload(svcauthtest.Unsigned(`{"alg":"RS256","kid":"kid-1"}`, string(mustJSON(v.claims)), []byte{1}))
}
