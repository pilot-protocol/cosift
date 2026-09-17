package sharedaccount

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

func TestRemoteSharedPasswordWireAndOTPCompatibility(t *testing.T) {
	const token = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Serverless-Authorization") != "Bearer infrastructure-fixture" || r.Header.Get("X-Forwarded-For") != "203.0.113.7" {
			t.Error("credential or IP forwarding mismatch")
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		switch calls {
		case 1:
			if r.URL.Path != "/auth/verify" || body["request_id"] != "request" || body["code"] != "123456" || body["password"] != "  password-valid-123  " {
				t.Error("enrollment payload mismatch")
			}
		case 2:
			if r.URL.Path != "/auth/password" || body["email"] != "shared@example.com" || body["password"] != "  password-valid-123  " {
				t.Error("login payload mismatch")
			}
		case 3:
			if r.URL.Path != "/auth/verify" {
				t.Error("OTP route changed")
			}
			if _, ok := body["password"]; ok {
				t.Error("old OTP flow sent enrollment field")
			}
		default:
			t.Error("invalid request reached upstream")
		}
		_ = json.NewEncoder(w).Encode(Issued{Token: token, UID: "0123456789abcdef"})
	}))
	defer srv.Close()
	remote, err := newRemote(context.Background(), srv.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}
	remote.identity = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "infrastructure-fixture"})
	c := Client{auth: remote}
	ctx := WithClientIP(context.Background(), "203.0.113.7")
	if _, err = c.FinishPassword(ctx, "request", "123456", "  password-valid-123  "); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Password(ctx, "shared@example.com", "  password-valid-123  "); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Finish(ctx, "request", "123456"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.FinishPassword(ctx, "request", "123456", "short"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err = c.Password(ctx, "shared@example.com", strings.Repeat("é", 129)); !errors.Is(err, ErrUnauthorized) {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatal(calls)
	}
}
func TestRemoteSharedPasswordErrorContractAndMalformedCredentials(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   error
	}{
		{"wrong credentials", 401, `{"error":"Unauthorized","status":401,"detail":"invalid email or password"}`, ErrUnauthorized},
		{"Google HTML denial", 401, `<html>private infrastructure</html>`, ErrUnavailable},
		{"unrecognized denial", 401, `{"error":"Unauthorized","status":401,"detail":"private service error"}`, ErrUnavailable},
		{"limited", 429, `{}`, ErrLimited},
		{"unavailable", 503, `{}`, ErrUnavailable},
		{"bad token", 200, `{"token":"not-canonical","account_uid":"0123456789abcdef"}`, ErrUnavailable},
		{"bad UID", 200, `{"token":"ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","account_uid":"bad"}`, ErrUnavailable},
		{"redirect", 307, `{}`, ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Location", "/unexpected")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			remote, err := newRemote(context.Background(), srv.URL, "", false)
			if err != nil {
				t.Fatal(err)
			}
			c := Client{auth: remote}
			_, err = c.Password(context.Background(), "shared@example.com", "password-valid-123")
			if !errors.Is(err, tc.want) || calls != 1 {
				t.Fatalf("error=%v calls=%d", err, calls)
			}
		})
	}
}
