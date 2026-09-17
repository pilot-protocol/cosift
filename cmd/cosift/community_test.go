package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
)

func TestCommunityEnqueueRequiresGuardAndAuth(t *testing.T) {
	called := 0
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}, crawlCommunityFetch: func(ctx context.Context, raw string, artifact *crawler.LocalArtifact) (crawler.ContributionReceipt, error) {
		called++
		if raw != "https://example.com/guide" {
			t.Errorf("bad contribution %s", raw)
		}
		return crawler.ContributionReceipt{Indexed: true, Novel: true}, nil
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

func TestCommunityRequestCLI(t *testing.T) {
	for _, mode := range []string{"search", "answer", "research"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("COSIFT_EMAIL", "")
			t.Setenv("COSIFT_PASSWORD", "")
			called := false
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if r.Method != "GET" || r.URL.Path != "/api/"+mode || r.URL.Query().Get("q") != "Go channels & select?" || r.Header.Get("X-Cosift-Client") != "community" {
					t.Errorf("incorrect request: %s %s", r.Method, r.URL)
				}
				w.Write([]byte(`{"results":[]}`))
			}))
			defer backend.Close()
			if err := runContribute(context.Background(), []string{"-server", backend.URL, "-request", "-guest", "-mode", mode, "-query", "Go channels & select?"}); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("no API request")
			}
		})
	}
}

func TestCommunityCLIRejectsInvalidIntentBeforeNetwork(t *testing.T) {
	t.Setenv("COSIFT_EMAIL", "cli@example.com")
	t.Setenv("COSIFT_PASSWORD", "test-password")
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write([]byte(`{}`))
	}))
	defer backend.Close()
	for _, args := range [][]string{
		{"-request", "-query", "hello", "-index-locally", "https://example.com/"},
		{"-credits", "https://example.com/"},
		{"-credits", "-csv", "/does-not-exist"},
		{"-request", "-mode", "invalid", "-query", "hello"},
		{"-request", "-query", strings.Repeat("x", 501)},
		{"-query", "ignored", "https://example.com/"},
	} {
		before := calls.Load()
		err := runContribute(context.Background(), append([]string{"-server", backend.URL}, args...))
		if err == nil {
			t.Errorf("accepted incompatible flags %v", args)
		}
		if calls.Load() != before {
			t.Errorf("network side effects before validation: %v", args)
		}
	}
}

func TestCommunityCLIResponseBounds(t *testing.T) {
	t.Setenv("COSIFT_EMAIL", "")
	t.Setenv("COSIFT_PASSWORD", "")
	output, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	previous := os.Stdout
	os.Stdout = output
	defer func() { os.Stdout = previous }()
	for _, tc := range []struct {
		name, body string
		wantError  bool
	}{
		{"large valid answer", `{"answer":"` + strings.Repeat("x", (1<<20)+100) + `"}`, false},
		{"too large", `{"answer":"` + strings.Repeat("x", 4<<20) + `"}`, true},
		{"invalid JSON", `{"answer":`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output.Truncate(0)
			output.Seek(0, 0)
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) }))
			defer backend.Close()
			err := runContribute(context.Background(), []string{"-server", backend.URL, "-request", "-guest", "-mode", "answer", "-query", "hello"})
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError %v", err, tc.wantError)
			}
			output.Seek(0, 0)
			got, _ := io.ReadAll(output)
			if !tc.wantError && string(got) != tc.body {
				t.Fatalf("response truncated: got %d, want %d bytes", len(got), len(tc.body))
			}
			if tc.wantError && len(got) != 0 {
				t.Fatal("printed unsuccessful response")
			}
		})
	}
}

func TestCommunityCLILogoutAfterCancellation(t *testing.T) {
	t.Setenv("COSIFT_EMAIL", "cli@example.com")
	t.Setenv("COSIFT_PASSWORD", "test-password")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var loggedOut atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/login":
			http.SetCookie(w, &http.Cookie{Name: "cosift_session", Value: "session", Path: "/"})
		case "/api/search":
			cancel()
		case "/api/logout":
			if _, err := r.Cookie("cosift_session"); err != nil {
				t.Error("logout lost session")
			}
			loggedOut.Store(true)
		}
		w.Write([]byte(`{}`))
	}))
	defer backend.Close()
	_ = runContribute(ctx, []string{"-server", backend.URL, "-request", "-query", "hello"})
	if !loggedOut.Load() {
		t.Fatal("cancelled CLI left session active")
	}
}
