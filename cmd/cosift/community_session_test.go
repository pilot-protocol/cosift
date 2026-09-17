package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCommunitySessionFileBoundaries(t *testing.T) {
	origin := "https://community.example.com"
	good := communitySession{Origin: origin, Token: "test-session", Expires: time.Now().Add(time.Hour)}
	for _, tc := range []struct {
		name      string
		change    func(*communitySession)
		mode      os.FileMode
		expired   bool
		wantError bool
	}{
		{"valid", nil, 0600, false, false},
		{"readable by others", nil, 0644, false, true},
		{"wrong origin", func(s *communitySession) { s.Origin = "https://other.example.com" }, 0600, false, true},
		{"expired", func(s *communitySession) { s.Expires = time.Now().Add(-time.Hour) }, 0600, false, true},
		{"expired logout", func(s *communitySession) { s.Expires = time.Now().Add(-time.Hour) }, 0600, true, false},
		{"invalid cookie", func(s *communitySession) { s.Token = "bad\r\nvalue" }, 0600, false, true},
		{"empty token", func(s *communitySession) { s.Token = "" }, 0600, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := good
			if tc.change != nil {
				tc.change(&value)
			}
			data, _ := json.Marshal(value)
			path := filepath.Join(t.TempDir(), "session")
			if err := os.WriteFile(path, data, tc.mode); err != nil {
				t.Fatal(err)
			}
			if _, err := readCommunitySession(path, origin, tc.expired); (err != nil) != tc.wantError {
				t.Fatalf("error = %v", err)
			}
		})
	}
	path := filepath.Join(t.TempDir(), "session")
	data, _ := json.Marshal(good)
	os.WriteFile(path, data, 0600)
	link := path + "-link"
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readCommunitySession(link, origin, false); err == nil {
		t.Fatal("accepted symlink")
	}
}

func TestCommunitySessionCLIIsolation(t *testing.T) {
	t.Setenv("COSIFT_EMAIL", "ignored@example.invalid")
	t.Setenv("COSIFT_PASSWORD", "") // A stored session needs no password.
	t.Setenv("COSIFT_SESSION_FILE", "")
	requests := 0
	guest := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/search" {
			t.Errorf("unexpected request: %s", r.URL.Path)
		}
		cookie, err := r.Cookie("cosift_session")
		if guest {
			if err == nil {
				t.Error("guest leaked saved session")
			}
		} else if err != nil || cookie.Value != "test-session" {
			t.Error("saved session missing")
		}
		w.Write([]byte(`{"hits":[]}`))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "session")
	data, _ := json.Marshal(communitySession{Origin: server.URL, Token: "test-session", Expires: time.Now().Add(time.Hour)})
	os.WriteFile(path, data, 0600)
	args := []string{"-request", "-server", server.URL, "-session-file", path, "-query", "Go"}
	if err := runContribute(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	guest = true
	if err := runContribute(context.Background(), append(args, "-guest")); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatal("unexpected authentication round trips")
	}
	// Do not fall back to guest or password auth if the requested session is bad.
	os.WriteFile(path, []byte(`{}`), 0600)
	if err := runContribute(context.Background(), args); err == nil {
		t.Fatal("invalid session accepted")
	}
	if requests != 2 {
		t.Fatal("invalid session caused network traffic")
	}
}

func TestCommunityLoginDoesNotOverwriteAndCleansFailure(t *testing.T) {
	t.Setenv("COSIFT_EMAIL", "test@example.invalid")
	t.Setenv("COSIFT_PASSWORD", "test-password")
	t.Setenv("COSIFT_SESSION_FILE", "")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(401)
		w.Write([]byte(`{"error":"incorrect credentials"}`))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "session")
	os.WriteFile(path, []byte("keep-existing"), 0600)
	args := []string{"-login", "-server", server.URL, "-session-file", path}
	if err := runContribute(context.Background(), args); err == nil {
		t.Fatal("overwrote existing session")
	}
	if calls != 0 {
		t.Fatal("logged in before validating destination")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "keep-existing" {
		t.Fatal("existing session changed")
	}
	os.Remove(path)
	if err := runContribute(context.Background(), args); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("login error: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("failed login retained file")
	}
}

func TestSharedTokenCLIUsesExistingCredentialWithoutLoginOrRevoke(t *testing.T) {
	const token = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	t.Setenv("COSIFT_TOKEN", token)
	t.Setenv("COSIFT_EMAIL", "")
	t.Setenv("COSIFT_PASSWORD", "")
	t.Setenv("COSIFT_SESSION_FILE", "")
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/api/search" || r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("unexpected credential lifecycle: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"hits":[]}`))
	}))
	defer srv.Close()
	for i := 0; i < 2; i++ {
		if err := runContribute(context.Background(), []string{"-server", srv.URL, "-request", "-query", "rust"}); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatal("temporary login/revoke with agent's persistent token", calls)
	}
}

func TestSharedTokenCLISavedSessionAndGuestIsolation(t *testing.T) {
	const token = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	t.Setenv("COSIFT_TOKEN", token)
	t.Setenv("COSIFT_EMAIL", "")
	t.Setenv("COSIFT_PASSWORD", "")
	t.Setenv("COSIFT_SESSION_FILE", "")
	guest := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _ := r.Cookie("cosift_session")
		if guest {
			if r.Header.Get("Authorization") != "" || c != nil {
				t.Error("guest leaked token")
			}
		} else if r.URL.Path == "/api/me" {
			if r.Header.Get("Authorization") != "Bearer "+token {
				t.Error("login did not verify token")
			}
		} else if c == nil || c.Value != token {
			t.Error("saved session not sent")
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	file := filepath.Join(t.TempDir(), "session")
	if err := runContribute(context.Background(), []string{"-server", srv.URL, "-login", "-session-file", file}); err != nil {
		t.Fatal(err)
	}
	saved, err := readCommunitySession(file, srv.URL, false)
	if err != nil || saved.Token != token {
		t.Fatal("token persistence", err)
	}
	guest = true
	if err := runContribute(context.Background(), []string{"-server", srv.URL, "-guest", "-request", "-query", "rust"}); err != nil {
		t.Fatal(err)
	}
	guest = false
	t.Setenv("COSIFT_TOKEN", "")
	if err := runContribute(context.Background(), []string{"-server", srv.URL, "-session-file", file, "-credits"}); err != nil {
		t.Fatal(err)
	}
}
