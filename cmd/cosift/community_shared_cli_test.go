package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedTokenCLIRejectsCredentialConflictsBeforeNetwork(t *testing.T) {
	const token = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	t.Setenv("COSIFT_EMAIL", "")
	t.Setenv("COSIFT_PASSWORD", "")
	t.Setenv("COSIFT_SESSION_FILE", "")
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	for _, tc := range []struct {
		name, credential, wantError string
		args                        []string
	}{
		{"malformed token", "invalid", "not a canonical Cosift token", []string{"-request", "-query", "Go"}},
		{"noncanonical token", strings.ToLower(token), "not a canonical Cosift token", []string{"-credits"}},
		{"conflicting saved session", token, "choose COSIFT_TOKEN or a saved session", []string{"-credits", "-session-file", "missing-session"}},
		{"login needs destination", token, "provide -session-file", []string{"-login"}},
		{"login and request", token, "cannot be combined", []string{"-login", "-request", "-query", "Go"}},
		{"login and logout", token, "cannot be combined", []string{"-login", "-logout"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("COSIFT_TOKEN", tc.credential)
			err := runContribute(context.Background(), append([]string{"-server", srv.URL}, tc.args...))
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("got %v, want %q", err, tc.wantError)
			}
			if calls.Load() != 0 {
				t.Fatal("invalid credential selection caused network traffic")
			}
		})
	}
}

func TestSharedTokenCLIUpstreamFailureDoesNotFallBackOrRevoke(t *testing.T) {
	const token = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	t.Setenv("COSIFT_TOKEN", token)
	// Existing standalone credentials must not cause a failed shared request to
	// log in as a different identity.
	t.Setenv("COSIFT_EMAIL", "standalone@example.invalid")
	t.Setenv("COSIFT_PASSWORD", "standalone-test-password")
	t.Setenv("COSIFT_SESSION_FILE", "")
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		for _, login := range []bool{false, true} {
			name := http.StatusText(status)
			if login {
				name += "/save session"
			}
			t.Run(name, func(t *testing.T) {
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					wantPath := "/api/credits"
					if login {
						wantPath = "/api/me"
					}
					if r.Method != http.MethodGet || r.URL.Path != wantPath || r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("Cookie") != "" {
						t.Errorf("unexpected authentication request: %s %s", r.Method, r.URL.Path)
					}
					w.WriteHeader(status)
					_, _ = w.Write([]byte(`{"error":"request rejected"}`))
				}))
				defer srv.Close()
				sessionFile := filepath.Join(t.TempDir(), "session")
				args := []string{"-server", srv.URL, "-credits"}
				if login {
					args = []string{"-server", srv.URL, "-login", "-session-file", sessionFile}
				}
				err := runContribute(context.Background(), args)
				var apiErr *communityHTTPError
				if !errors.As(err, &apiErr) || apiErr.status != status {
					t.Fatalf("got %v, want HTTP %d", err, status)
				}
				if calls.Load() != 1 {
					t.Fatal("shared request attempted fallback authentication or token revocation")
				}
				if _, err := os.Stat(sessionFile); !os.IsNotExist(err) {
					t.Fatalf("failed token validation retained a session file: %v", err)
				}
				if os.Getenv("COSIFT_TOKEN") != token {
					t.Fatal("upstream error discarded the installer's credential")
				}
			})
		}
	}
}

func TestSharedTokenCLIDoesNotFollowCredentialRedirect(t *testing.T) {
	const token = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	t.Setenv("COSIFT_TOKEN", token)
	t.Setenv("COSIFT_EMAIL", "")
	t.Setenv("COSIFT_PASSWORD", "")
	t.Setenv("COSIFT_SESSION_FILE", "")
	var redirected atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer destination.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("missing shared credential on configured origin")
		}
		w.Header().Set("Location", destination.URL+"/api/credits")
		w.WriteHeader(http.StatusTemporaryRedirect)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	err := runContribute(context.Background(), []string{"-server", srv.URL, "-credits"})
	var apiErr *communityHTTPError
	if !errors.As(err, &apiErr) || apiErr.status != http.StatusTemporaryRedirect {
		t.Fatalf("redirect did not fail closed: %v", err)
	}
	if redirected.Load() != 0 {
		t.Fatal("followed a redirect with the shared credential")
	}
}

func TestCommunityCLILogoutRetainsSessionUntilRevoked(t *testing.T) {
	t.Setenv("COSIFT_TOKEN", "")
	t.Setenv("COSIFT_EMAIL", "")
	t.Setenv("COSIFT_PASSWORD", "")
	t.Setenv("COSIFT_SESSION_FILE", "")
	for _, tc := range []struct {
		name        string
		status      int
		body        string
		wantRemoved bool
	}{
		{"revoked", http.StatusOK, `{"revoked":true}`, true},
		{"already revoked", http.StatusUnauthorized, `{"error":"unauthorized"}`, true},
		{"temporary auth failure", http.StatusServiceUnavailable, `{"error":"unavailable"}`, false},
		{"unusable success response", http.StatusOK, `{`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				cookie, err := r.Cookie("cosift_session")
				if r.Method != http.MethodPost || r.URL.Path != "/api/logout" || err != nil || cookie.Value != "expired-session" {
					t.Error("logout did not send the expired session for revocation")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			file := filepath.Join(t.TempDir(), "session")
			data, err := json.Marshal(communitySession{Origin: srv.URL, Token: "expired-session", Expires: time.Now().Add(-time.Hour)})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, data, 0600); err != nil {
				t.Fatal(err)
			}
			err = runContribute(context.Background(), []string{"-server", srv.URL, "-logout", "-session-file", file})
			if (err == nil) != tc.wantRemoved {
				t.Fatalf("logout error = %v, wantRemoved %v", err, tc.wantRemoved)
			}
			if calls.Load() != 1 {
				t.Fatal("logout retried or performed extra authentication requests")
			}
			remaining, err := os.ReadFile(file)
			if tc.wantRemoved {
				if !os.IsNotExist(err) {
					t.Fatalf("revoked session retained: %v", err)
				}
			} else if err != nil || string(remaining) != string(data) {
				t.Fatalf("failed logout lost the session needed to retry: %v", err)
			}
		})
	}
}
