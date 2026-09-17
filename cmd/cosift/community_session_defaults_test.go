package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func isolateInstalledCommunitySession(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	for _, name := range []string{"COSIFT_TOKEN", "COSIFT_EMAIL", "COSIFT_PASSWORD", "COSIFT_SESSION_FILE"} {
		t.Setenv(name, "")
	}
	return filepath.Join(home, "config", "cosift", "community-session.json")
}

func writeInstalledCommunitySession(t *testing.T, path, origin, token string, expires time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(communitySession{Origin: origin, Token: token, Expires: expires})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestInstalledCommunitySessionUsesRecordedOrigin(t *testing.T) {
	for _, xdg := range []bool{true, false} {
		name := "XDG config"
		if !xdg {
			name = "home fallback"
		}
		t.Run(name, func(t *testing.T) {
			file := isolateInstalledCommunitySession(t)
			if !xdg {
				t.Setenv("XDG_CONFIG_HOME", "")
				file = filepath.Join(os.Getenv("HOME"), ".config", "cosift", "community-session.json")
			}
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				cookie, err := r.Cookie("cosift_session")
				if r.URL.Path != "/api/credits" || err != nil || cookie.Value != "installed-token" || r.Header.Get("Authorization") != "" {
					t.Error("default CLI command did not use the installed session")
				}
				_, _ = w.Write([]byte(`{"balance":0}`))
			}))
			defer srv.Close()
			writeInstalledCommunitySession(t, file, srv.URL, "installed-token", time.Now().Add(time.Hour))
			if err := runContribute(context.Background(), []string{"-credits"}); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatal("default session caused extra authentication requests")
			}
		})
	}
}

func TestInstalledCommunitySessionAbsentHasNoImplicitIdentity(t *testing.T) {
	isolateInstalledCommunitySession(t)
	path, _, err := installedCommunitySession(false)
	if err != nil || path != "" {
		t.Fatalf("missing session should leave explicit or anonymous auth available: %q %v", path, err)
	}
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("HOME", "")
	path, _, err = installedCommunitySession(false)
	if err != nil || path != "" {
		t.Fatalf("process without a home acquired a default identity: %q %v", path, err)
	}
}

func TestInstalledCommunitySessionDoesNotOverrideExplicitServer(t *testing.T) {
	file := isolateInstalledCommunitySession(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	writeInstalledCommunitySession(t, file, "https://other.example.invalid", "installed-token", time.Now().Add(time.Hour))
	err := runContribute(context.Background(), []string{"-server", srv.URL, "-credits"})
	if err == nil || !strings.Contains(err.Error(), "different server") {
		t.Fatalf("explicit server did not preserve the origin boundary: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("sent the installed credential to a different origin")
	}
}

func TestInstalledCommunitySessionExplicitCredentialsAndGuestWin(t *testing.T) {
	const token = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	for _, selection := range []string{"guest", "token", "session flag", "session environment", "empty session flag", "password"} {
		t.Run(selection, func(t *testing.T) {
			file := isolateInstalledCommunitySession(t)
			// An unreadable default must not break an explicit choice, and no
			// installed identity may leak into an explicitly anonymous request.
			if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, []byte("invalid default session"), 0600); err != nil {
				t.Fatal(err)
			}
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/login":
					if selection != "password" {
						t.Error("unexpected password fallback")
					}
					http.SetCookie(w, &http.Cookie{Name: "cosift_session", Value: "explicit-session", Path: "/"})
				case "/api/logout":
					if selection != "password" {
						t.Error("unexpected token revocation")
					}
				case "/api/search":
					requests.Add(1)
					cookie, _ := r.Cookie("cosift_session")
					if selection == "token" {
						if r.Header.Get("Authorization") != "Bearer "+token || cookie != nil {
							t.Error("explicit token lost precedence")
						}
					} else if selection == "guest" || selection == "empty session flag" {
						if r.Header.Get("Authorization") != "" || cookie != nil {
							t.Error("anonymous request acquired an installed credential")
						}
					} else if cookie == nil || cookie.Value != "explicit-session" {
						t.Error("explicit session lost precedence")
					}
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
				}
				_, _ = w.Write([]byte(`{}`))
			}))
			defer srv.Close()
			args := []string{"-server", srv.URL, "-request", "-query", "Go"}
			switch selection {
			case "guest":
				args = append(args, "-guest")
			case "token":
				t.Setenv("COSIFT_TOKEN", token)
			case "session flag", "session environment":
				explicit := filepath.Join(t.TempDir(), "explicit-session")
				writeInstalledCommunitySession(t, explicit, srv.URL, "explicit-session", time.Now().Add(time.Hour))
				if selection == "session flag" {
					args = append(args, "-session-file", explicit)
				} else {
					t.Setenv("COSIFT_SESSION_FILE", explicit)
				}
			case "empty session flag":
				args = append(args, "-session-file", "")
			case "password":
				t.Setenv("COSIFT_EMAIL", "explicit@example.invalid")
				t.Setenv("COSIFT_PASSWORD", "explicit-test-password")
			}
			if err := runContribute(context.Background(), args); err != nil {
				t.Fatal(err)
			}
			if requests.Load() != 1 {
				t.Fatal("expected one selected-account search")
			}
		})
	}
}

func TestInstalledCommunitySessionFailsClosed(t *testing.T) {
	for _, kind := range []string{"expired", "exposed", "malformed", "symlink", "insecure origin"} {
		t.Run(kind, func(t *testing.T) {
			file := isolateInstalledCommunitySession(t)
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
			defer srv.Close()
			origin := srv.URL
			expires := time.Now().Add(time.Hour)
			if kind == "expired" {
				expires = time.Now().Add(-time.Hour)
			}
			if kind == "insecure origin" {
				origin = "http://example.invalid"
			}
			writeInstalledCommunitySession(t, file, origin, "installed-token", expires)
			switch kind {
			case "exposed":
				if err := os.Chmod(file, 0644); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				if err := os.WriteFile(file, []byte(`{`), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(file, file+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(file+".original", file); err != nil {
					t.Fatal(err)
				}
			}
			if err := runContribute(context.Background(), []string{"-credits"}); err == nil {
				t.Fatal("invalid default session was accepted")
			}
			if calls.Load() != 0 {
				t.Fatal("invalid session caused network traffic")
			}
		})
	}
}

func TestInstalledCommunitySessionLogoutUsesExpiredCredential(t *testing.T) {
	file := isolateInstalledCommunitySession(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("cosift_session")
		if r.URL.Path != "/api/logout" || err != nil || cookie.Value != "expired-installed-token" {
			t.Error("default logout did not revoke the saved credential")
		}
		_, _ = w.Write([]byte(`{"revoked":true}`))
	}))
	defer srv.Close()
	writeInstalledCommunitySession(t, file, srv.URL, "expired-installed-token", time.Now().Add(-time.Hour))
	if err := runContribute(context.Background(), []string{"-logout"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatalf("revoked installed session was retained: %v", err)
	}
}
