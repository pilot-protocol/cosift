package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/pilot-protocol/cosift/internal/community"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Exercise real command dispatch, real HTTP cookie sessions,
// persisted account state, CSV input and all three CLI modes. Only the retrieval
// backend is a deterministic HTTP fixture; the community app and CLI are real.
// The contribution worker is not started, so this regression needs no internet.
func TestCommunityBinaryEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("build and subprocess test")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "cosift")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "" {
			t.Error("account cookies forwarded to retrieval backend")
		}
		if r.URL.Query().Get("q") != "Go documentation" || r.URL.Query().Get("stream") != "false" {
			t.Errorf("bad retrieval contract: %s", r.URL)
		}
		switch r.URL.Path {
		case "/search":
			json.NewEncoder(w).Encode(searchResponse{Hits: []searchHit{{URL: "https://go.dev/doc/", Title: "Go documentation"}}})
		case "/answer":
			json.NewEncoder(w).Encode(answerResponse{Answer: "Go documentation [1].", Sources: []answerSource{{ID: 1, URL: "https://go.dev/doc/", Title: "Go documentation"}}})
		case "/research":
			io.WriteString(w, `{"plan":["Go documentation"],"answer":"Go documentation [1].","sources":[{"id":1,"url":"https://go.dev/doc/"}]}`)
		default:
			w.WriteHeader(503)
		}
	}))
	defer backend.Close()
	app, err := community.Open(community.Config{DataDir: filepath.Join(dir, "accounts"), Backend: backend.URL, PublicURL: "http://localhost:7780", AdminToken: "local-e2e-token"})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	portal := httptest.NewServer(app)
	defer portal.Close()
	origin := portal.URL
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir}
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, Timeout: 5 * time.Second}
	call := func(method, path string, body any, want int) []byte {
		t.Helper()
		data, _ := json.Marshal(body)
		req, _ := http.NewRequest(method, origin+path, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Cosift-Client", "community")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		out, _ := io.ReadAll(res.Body)
		if res.StatusCode != want {
			t.Fatalf("%s %s: %d %s", method, path, res.StatusCode, out)
		}
		return out
	}
	call("POST", "/api/register", map[string]string{"name": "CLI test", "email": "binary@example.invalid", "password": "local-e2e-password-123"}, 200)
	call("PUT", "/api/interests", map[string]any{"interests": []string{"Go"}}, 200)
	call("POST", "/api/saved", map[string]string{"query": "Go documentation", "mode": "research"}, 200)
	cli := func(args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, bin, args...)
		command.Dir = dir
		command.Env = append(append([]string{}, env...), "COSIFT_EMAIL=binary@example.invalid", "COSIFT_PASSWORD=local-e2e-password-123")
		out, err := command.Output()
		if err != nil {
			t.Fatalf("CLI %v: %v", args, err)
		}
		if !json.Valid(out) {
			t.Fatalf("invalid CLI JSON %q", out)
		}
		return out
	}
	for _, mode := range []string{"search", "answer", "research"} {
		out := cli("request", "-server", origin, "-mode", mode, "-query", "Go documentation")
		if !bytes.Contains(out, []byte("https://go.dev/doc/")) {
			t.Fatalf("missing source for %s: %s", mode, out)
		}
	}
	csv := filepath.Join(dir, "urls.csv")
	os.WriteFile(csv, []byte("title,url\nGuide,https://go.dev/doc/\n"), 0600)
	out := cli("contribute", "-server", origin, "-csv", csv)
	if !bytes.Contains(out, []byte(`"accepted":1`)) {
		t.Fatalf("CSV not accepted: %s", out)
	}
	if out := cli("contribute", "-server", origin, "-credits"); !bytes.Contains(out, []byte(`"payments_enabled":false`)) {
		t.Fatal("payments enabled without keys")
	}
	// CLI logout must not revoke the web session; both see the same saved data.
	call("GET", "/api/me", nil, 200)
	if out := call("GET", "/api/saved", nil, 200); !bytes.Contains(out, []byte(`"mode":"research"`)) {
		t.Fatalf("saved mode lost: %s", out)
	}
	if out := call("GET", "/api/submissions", nil, 200); !bytes.Contains(out, []byte("https://go.dev/doc/")) {
		t.Fatal("CLI contribution missing from web history")
	}
	sample := call("GET", "/sample.csv", nil, 200)
	if !bytes.Contains(sample, []byte("url")) {
		t.Fatal("sample unavailable")
	}
	call("POST", "/api/logout", map[string]string{}, 200)
	call("GET", "/api/saved", nil, 401)
	call("GET", "/api/search?q=Go%20documentation", nil, 200)
	call("GET", "/api/research?q=Go%20documentation", nil, 429)
}
