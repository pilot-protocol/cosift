// Real-binary HTTP smoke test.
//
// Builds cosift into a temp dir, runs `cosift serve` as a subprocess on an
// OS-allocated free port, hits /health + /stats over TCP, sends SIGTERM,
// confirms clean exit. Catches the class of bug found in production
// (binary didn't honor PORT) — that bug passed every httptest unit test
// because httptest never goes through the binary's bind path.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// freePort grabs a free TCP port via the standard listen-then-close trick.
// Small race window between close and subprocess bind; in practice reliable.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// waitReady polls until the server answers /health or the deadline passes.
func waitReady(url string, deadline time.Time) error {
	for time.Now().Before(deadline) {
		resp, err := http.Get(url + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("server never became ready")
}

func TestBinaryEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e build+run in -short mode")
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM/exec semantics differ on Windows")
	}

	tmp := t.TempDir()
	bin := filepath.Join(tmp, "cosift-e2e")

	// 1. Build into tempdir. `go build` is fast on a warm cache.
	build := exec.Command("go", "build", "-trimpath", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	// 2. Launch on a free port with a temp data dir. PORT exercises the
	//    12-factor env override from (the bug that prompted this test).
	port := freePort(t)
	dataDir := filepath.Join(tmp, "data")
	cmd := exec.Command(bin, "serve")
	cmd.Env = append([]string{},
		"PATH=/usr/bin:/bin",
		fmt.Sprintf("PORT=%d", port),
		fmt.Sprintf("COSIFT_DATA_DIR=%s", dataDir),
	)
	// Capture stderr so failures show the binary's logs.
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", port)

	// 3. Wait for readiness via /health (the alias). 5s deadline is
	//    generous on a fresh boot; the binary needs to do SQLite open + schema migration.
	if err := waitReady(url, time.Now().Add(5*time.Second)); err != nil {
		out, _ := io.ReadAll(stderr)
		t.Fatalf("not ready: %v\nstderr:\n%s", err, out)
	}

	// 4. /health returns JSON.
	resp, err := http.Get(url + "/health")
	if err != nil {
		t.Fatalf("/health: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("/health content-type: %q want application/json", ct)
	}
	var hr struct {
		Status string `json:"status"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&hr)
	if hr.Status != "ok" {
		t.Errorf("/health status: got %q want ok", hr.Status)
	}

	// 5. /stats returns counts.
	resp, err = http.Get(url + "/stats")
	if err != nil {
		t.Fatalf("/stats: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("/stats status: got %d want 200", resp.StatusCode)
	}

	// 6. /metrics contains the version label (proves cosift_info wired through
	//    the real serve path, not just httptest).
	resp, err = http.Get(url + "/metrics")
	if err != nil {
		t.Fatalf("/metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "cosift_info{") {
		t.Errorf("/metrics missing cosift_info line:\n%s", body)
	}
}

// TestQuickstartE2E exercises the full onboarding pipeline through the real
// binary: `cosift init -site URL` → `cosift crawl URL` → `cosift query "text"`.
//
// Catches regressions at the seam between init (writing include_domains)
// and the crawler's domain enforcement. Unit tests cover each piece in
// isolation; this exercises the join.
func TestQuickstartE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e build+crawl in -short mode")
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM/exec semantics differ on Windows")
	}

	tmp := t.TempDir()
	bin := filepath.Join(tmp, "cosift-quickstart-e2e")

	// 1. Build the binary.
	build := exec.Command("go", "build", "-trimpath", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	// 2. Spin up a tiny in-process website with two crawlable pages. Home page
	//    links to /about so the crawler has something to follow at depth 1.
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!doctype html><html><head><title>Test Site Home</title></head>
<body>
<h1>Welcome to the test site</h1>
<p>This page talks about goroutines and concurrent programming patterns in Go.</p>
<p>See also <a href="/about">about</a>.</p>
</body></html>`))
	})
	mux.HandleFunc("/about", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!doctype html><html><head><title>About</title></head>
<body><p>About this site: it covers tessellation patterns, an unusual word for full-text matching.</p></body></html>`))
	})
	// robots.txt is required to be reachable; serve a permissive one.
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("User-agent: *\nAllow: /\n"))
	})
	site := httptest.NewServer(mux)
	defer site.Close()

	cfgPath := filepath.Join(tmp, "cosift.json")

	// 3. Run `cosift init -site <url>`. Verifies the file is written + crawl
	//    domains are populated.
	initCmd := exec.Command(bin, "-config", cfgPath, "init", "-site", site.URL)
	if out, err := initCmd.CombinedOutput(); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	cfgBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read cosift.json: %v", err)
	}
	// Tag-free JSON match — assert include_domains carries the test host:port.
	host := strings.TrimPrefix(site.URL, "http://")
	if !strings.Contains(string(cfgBytes), host) {
		t.Errorf("cosift.json should include host %q:\n%s", host, cfgBytes)
	}

	// 4. Speed up the crawler for testing — the default per_host_delay is 1s
	//    which would slow this test unnecessarily. Edit the freshly-written
	//    config in place. Mirrors what a user would do for high-volume crawls.
	cfgEdited := strings.ReplaceAll(string(cfgBytes), `"per_host_delay_ms": 1000`, `"per_host_delay_ms": 10`)
	// Also point data_dir into TempDir so we don't pollute the cwd.
	dataDir := filepath.Join(tmp, "data")
	cfgEdited = strings.ReplaceAll(cfgEdited, `"data_dir": "./cosift-data"`, fmt.Sprintf(`"data_dir": %q`, dataDir))
	if err := os.WriteFile(cfgPath, []byte(cfgEdited), 0o644); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}

	// 5. Crawl the test site through the binary.
	crawlCmd := exec.Command(bin, "-config", cfgPath, "crawl", site.URL)
	crawlOut, err := crawlCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("crawl: %v\n%s", err, crawlOut)
	}

	// 6. Verify the index actually got populated via `cosift stats`.
	statsCmd := exec.Command(bin, "-config", cfgPath, "stats")
	statsOut, err := statsCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("stats: %v\n%s", err, statsOut)
	}
	if !strings.Contains(string(statsOut), "documents:") {
		t.Errorf("stats output should contain 'documents:': %s", statsOut)
	}
	// Must have ingested at least the home page.
	if strings.Contains(string(statsOut), "documents: 0") {
		t.Fatalf("crawl indexed nothing — stats:\n%s\ncrawl stdout:\n%s", statsOut, crawlOut)
	}

	// 7. Query for a unique word from the about page. Result must include the
	//    about URL — proves end-to-end retrieval against the crawled corpus.
	queryCmd := exec.Command(bin, "-config", cfgPath, "query", "tessellation")
	queryOut, err := queryCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("query: %v\n%s", err, queryOut)
	}
	if !strings.Contains(string(queryOut), "/about") {
		t.Errorf("query for unique 'tessellation' should surface /about; got:\n%s\nstats:\n%s\ncrawl:\n%s",
			queryOut, statsOut, crawlOut)
	}
}

// TestCrawlAutoWiresEmbedderFromConfig — regression test for the gap
// surfaced by the real-runner verification: `cosift crawl` wasn't
// calling WithEmbedder even when cosift.json declared embeddings.model, so the
// vector index stayed empty. This test stubs the OpenAI embeddings endpoint
// with httptest and asserts the binary's crawl subprocess POSTs to it.
func TestCrawlAutoWiresEmbedderFromConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e build+crawl in -short mode")
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM/exec semantics differ on Windows")
	}

	tmp := t.TempDir()
	bin := filepath.Join(tmp, "cosift-auto-embed-e2e")
	build := exec.Command("go", "build", "-trimpath", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	// Tiny site with one crawlable page (enough to trigger one embed batch).
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("User-agent: *\nAllow: /\n"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!doctype html><html><head><title>Embed Test</title></head>
<body><p>The crawler should embed this passage via the configured model.</p></body></html>`))
	})
	site := httptest.NewServer(mux)
	defer site.Close()

	// Mock OpenAI /v1/embeddings — count POSTs, return one fake vector per input.
	var embedCalls int32
	embedMux := http.NewServeMux()
	embedMux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "expected POST", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		// One deterministic 8-dim vector per input. Real OpenAI vectors are
		// 1536-dim, but the dim is set by cfg.Embeddings.Dim — we use 8 here
		// for compactness and configure cfg to match.
		type embedItem struct {
			Embedding []float32 `json:"embedding"`
			Index     int       `json:"index"`
		}
		out := struct {
			Data []embedItem `json:"data"`
		}{Data: make([]embedItem, len(req.Input))}
		for i := range req.Input {
			vec := make([]float32, 8)
			for j := range vec {
				vec[j] = float32((i+j)%7) / 7
			}
			out.Data[i] = embedItem{Embedding: vec, Index: i}
		}
		// Count AFTER successful decode so malformed/probe requests don't
		// inflate the count.
		atomic.AddInt32(&embedCalls, 1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	embedSrv := httptest.NewServer(embedMux)
	defer embedSrv.Close()

	cfgPath := filepath.Join(tmp, "cosift.json")
	dataDir := filepath.Join(tmp, "data")
	cfg := fmt.Sprintf(`{
  "data_dir": %q,
  "server": {"addr": "127.0.0.1:0"},
  "crawler": {
    "user_agent": "CosiftAutoEmbedE2E/0.0",
    "max_concurrent": 4,
    "per_host_delay_ms": 10,
    "max_body_bytes": 1048576,
    "max_depth": 0,
    "respect_robots": true,
    "include_domains": [%q]
  },
  "embeddings": {"model": "test-model", "url": %q, "dim": 8}
}`, dataDir, strings.TrimPrefix(site.URL, "http://"), embedSrv.URL+"/v1/embeddings")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write cfg: %v", err)
	}

	crawlCmd := exec.Command(bin, "-config", cfgPath, "crawl", site.URL)
	crawlCmd.Env = append(os.Environ(), "OPENAI_API_KEY=stub-key-for-e2e-test")
	crawlOut, err := crawlCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("crawl: %v\n%s", err, crawlOut)
	}

	// The fix: runCrawl must wire WithEmbedder when cfg.Embeddings.Model is set
	// and OPENAI_API_KEY is in env. Pre-fix, embedCalls would be 0.
	if got := atomic.LoadInt32(&embedCalls); got == 0 {
		t.Fatalf("expected at least 1 POST to /v1/embeddings (proving runCrawl wired the embedder); got 0\ncrawl output:\n%s", crawlOut)
	}
	if !strings.Contains(string(crawlOut), "dense embeddings enabled") {
		t.Errorf("expected 'dense embeddings enabled' log line in crawl output; got:\n%s", crawlOut)
	}
}
