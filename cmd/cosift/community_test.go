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

	"github.com/pilot-protocol/cosift/internal/config"
)

func TestCommunityEnqueueRequiresGuardAndAuth(t *testing.T) {
	called := 0
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}, crawlSeedLane: func(raw string, lane byte) error {
		called++
		if raw != "https://example.com/guide" || lane != parseLaneName("submitted") {
			t.Errorf("bad contribution %s lane %d", raw, lane)
		}
		return nil
	}}
	call := func(token, url string) int {
		r := httptest.NewRequest("POST", "/admin/community-enqueue", strings.NewReader(`{"url":"`+url+`"}`))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.handleCommunityEnqueue(w, r)
		return w.Code
	}
	if got := call("wrong", "https://example.com/guide"); got != 401 {
		t.Fatalf("auth %d", got)
	}
	if got := call("secret", "https://example.com/guide"); got != 503 {
		t.Fatalf("unguarded %d", got)
	}
	s.crawlPublicOnly.Store(true)
	if got := call("secret", "https://example.com/guide"); got != 503 {
		t.Fatalf("adult filter must also be enabled: %d", got)
	}
	s.crawlCommunityReady.Store(true)
	if got := call("secret", "http://localhost/private"); got != 400 {
		t.Fatalf("local URL %d", got)
	}
	if got := call("secret", "https://example.com/guide"); got != 200 {
		t.Fatalf("valid %d", got)
	}
	if called != 1 {
		t.Fatalf("unsafe enqueue: %d calls", called)
	}
}

func TestCommunityContributeCLI(t *testing.T) {
	t.Setenv("COSIFT_EMAIL", "cli@example.com")
	t.Setenv("COSIFT_PASSWORD", "cli-test-password")
	submitted, loggedOut := false, false
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Cosift-Client") != "community" {
			t.Error("missing CSRF client header")
		}
		switch r.URL.Path {
		case "/api/login":
			var v map[string]string
			json.NewDecoder(r.Body).Decode(&v)
			if v["email"] != "cli@example.com" || v["password"] != "cli-test-password" {
				t.Error("bad credentials")
			}
			http.SetCookie(w, &http.Cookie{Name: "cosift_session", Value: "test-session", Path: "/"})
			w.Write([]byte(`{}`))
		case "/api/submissions":
			c, err := r.Cookie("cosift_session")
			if err != nil || c.Value != "test-session" {
				t.Error("missing session")
			}
			var v struct {
				URLs []string `json:"urls"`
			}
			json.NewDecoder(r.Body).Decode(&v)
			if len(v.URLs) != 1 || v.URLs[0] != "https://example.com/guide" {
				t.Errorf("bad URLs %+v", v)
			}
			submitted = true
			w.WriteHeader(202)
			w.Write([]byte(`{"accepted":1}`))
		case "/api/logout":
			loggedOut = true
			w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer backend.Close()
	file := filepath.Join(t.TempDir(), "urls.csv")
	if err := os.WriteFile(file, []byte("title,url\nGuide,https://example.com/guide\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := runContribute(context.Background(), []string{"-server", backend.URL, "-csv", file}); err != nil {
		t.Fatal(err)
	}
	if !submitted || !loggedOut {
		t.Fatal("CLI did not submit/revoke session")
	}
	if err := runContribute(context.Background(), []string{"-server", "http://example.com", "https://example.com/guide"}); err == nil {
		t.Fatal("credentials allowed over public HTTP")
	}
}

func TestCommunityGuestCLI(t *testing.T) {
	t.Setenv("COSIFT_EMAIL", "")
	t.Setenv("COSIFT_PASSWORD", "")
	called := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		if r.URL.Path != "/api/submissions" || r.Header.Get("Cookie") != "" {
			t.Errorf("guest attempted auth: %s", r.URL.Path)
		}
		w.WriteHeader(202)
		w.Write([]byte(`{"accepted":1}`))
	}))
	defer backend.Close()
	if err := runContribute(context.Background(), []string{"-server", backend.URL, "-guest", "https://example.com/guide"}); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("requests %d", called)
	}
}
