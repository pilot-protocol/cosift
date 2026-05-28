package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/server"
	"github.com/pilot-protocol/cosift/internal/store"
)

// newAdminStub serves /admin/stats and /admin/config with a single canned
// response each. Captures the Authorization header and request path of the
// last request so tests can assert on auth propagation + dispatch.
func newAdminStub(t *testing.T, stats server.AdminStatsResponse, cfg server.AdminConfigResponse) (*httptest.Server, func() (path string, authHeader string)) {
	t.Helper()
	var lastPath, lastAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastPath = r.URL.Path
		lastAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/admin/stats":
			_ = json.NewEncoder(w).Encode(stats)
		case "/admin/config":
			_ = json.NewEncoder(w).Encode(cfg)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() (string, string) { return lastPath, lastAuth }
}

func captureAdminStdout(t *testing.T, f func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan []byte)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.Bytes()
	}()
	f()
	_ = w.Close()
	os.Stdout = orig
	return string(<-done)
}

func TestAdminUsageError(t *testing.T) {
	// when `cosift admin` is invoked without a subcommand, the
	// usage message must list every known admin subcommand. If a future iter
	// adds a 6th admin sub-op, this test fails unless the help text is also
	// updated — keeping the dispatch switch and the help-text in sync.
	out := adminUsageError()

	// Every subcommand currently registered in runAdmin must appear in the help.
	for _, sub := range []string{"stats", "config", "recrawl", "recrawl-domain", "reembed"} {
		if !strings.Contains(out, sub) {
			t.Errorf("help text missing subcommand %q:\n%s", sub, out)
		}
	}

	// Common flag documentation present.
	for _, flag := range []string{"-server", "-token", "-json", "COSIFT_ADMIN_TOKEN"} {
		if !strings.Contains(out, flag) {
			t.Errorf("help text missing flag/env %q:\n%s", flag, out)
		}
	}

	// Multi-line format (vs the single-line log.Fatal).
	if strings.Count(out, "\n") < 5 {
		t.Errorf("help text should be multi-line; got %d lines:\n%s", strings.Count(out, "\n"), out)
	}

	// Must NOT contain the log-package timestamp prefix (the bug
	// being fixed: log.Fatal added "2026/05/23 ..." noise).
	if strings.Contains(out, "/") && strings.Contains(out[:50], ":") {
		// Heuristic: a date-time prefix would have a slash AND a colon in the first 50 chars.
		// Just check the output doesn't start with a digit (timestamps do).
		if out[0] >= '0' && out[0] <= '9' {
			t.Errorf("help text leaks timestamp prefix:\n%s", out)
		}
	}

	// Stable alphabetic ordering: config appears before recrawl, recrawl
	// before recrawl-domain, recrawl-domain before reembed, reembed before stats.
	checkOrder := func(first, second string) {
		i, j := strings.Index(out, first), strings.Index(out, second)
		if i == -1 || j == -1 || i > j {
			t.Errorf("alphabetic ordering broken: %q should appear before %q\n%s", first, second, out)
		}
	}
	checkOrder("config", "recrawl")
	checkOrder("recrawl ", "recrawl-domain") // "recrawl <url...>" before "recrawl-domain <pattern>"
	checkOrder("recrawl-domain", "reembed")
	checkOrder("reembed", "stats")
}

func TestAdminDispatchUnknownSubcommand(t *testing.T) {
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "frobnicate", nil)
	if err == nil {
		t.Fatal("unknown subcommand should error")
	}
	if !strings.Contains(err.Error(), "frobnicate") {
		t.Errorf("error should name the bad subcommand: %v", err)
	}
}

func TestAdminStatsHumanOutput(t *testing.T) {
	stats := server.AdminStatsResponse{
		Documents: 1000, Terms: 5000, Passages: 3000,
		Frontier:            store.FrontierStats{Queued: 12, InFlight: 2, Done: 980, Errored: 6},
		TopDomains:          map[string]int64{"example.com": 500, "test.org": 300, "docs.io": 200},
		EmbedderModel:       "text-embedding-3-small",
		ChatModel:           "gpt-4o",
		DenseEnabled:        true,
		AnswerEnabled:       true,
		FetcherEnabled:      false,
		DocsWithPublishedAt: 400,
	}
	srv, capture := newAdminStub(t, stats, server.AdminConfigResponse{})
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "stats",
			[]string{"-server", srv.URL, "-token", "secret"}); err != nil {
			t.Fatalf("admin stats: %v", err)
		}
	})
	path, auth := capture()
	if path != "/admin/stats" {
		t.Errorf("wrong path hit: %q", path)
	}
	if auth != "Bearer secret" {
		t.Errorf("bearer header not propagated: %q", auth)
	}
	for _, want := range []string{
		"=== Index ===",
		"Documents:           1000",
		"Terms:               5000",
		"Passages:            3000",
		"Docs w/ published_at: 400 (40.0%)",
		"=== Frontier ===",
		"Queued:    12",
		"Errored:   6",
		"=== Capabilities ===",
		"Dense:    true",
		"Embedder: text-embedding-3-small",
		"=== Top Domains ===",
		"example.com",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output: %q", want, out)
		}
	}
	// Top-domains must be sorted descending by count → example.com (500) before test.org (300).
	idxExample := strings.Index(out, "example.com")
	idxTest := strings.Index(out, "test.org")
	if idxExample == -1 || idxTest == -1 || idxExample > idxTest {
		t.Errorf("top domains not sorted by count desc; example.com idx=%d, test.org idx=%d, out=%q", idxExample, idxTest, out)
	}
	// when paraphrases + hyde_cache are both zero, the LLM-caches
	// section should NOT appear (fresh deployments don't carry the noise).
	if strings.Contains(out, "=== LLM caches ===") {
		t.Errorf("LLM caches section should be hidden when both caches are empty; got %q", out)
	}
}

// when /admin/stats reports non-zero cache counts, the CLI
// renders the new LLM-caches section.
func TestAdminStatsLLMCachesShown(t *testing.T) {
	stats := server.AdminStatsResponse{
		Documents: 100, Terms: 500, Passages: 200,
		Paraphrases: 42, HyDECache: 17,
	}
	srv, _ := newAdminStub(t, stats, server.AdminConfigResponse{})
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "stats",
			[]string{"-server", srv.URL, "-token", "secret"}); err != nil {
			t.Fatalf("admin stats: %v", err)
		}
	})
	for _, want := range []string{
		"=== LLM caches ===",
		"Paraphrases:         42",
		"HyDE passages:       17",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output: %q", want, out)
		}
	}
}

// when only one cache is populated, only that line appears.
func TestAdminStatsLLMCachesPartial(t *testing.T) {
	stats := server.AdminStatsResponse{
		Documents: 100, Terms: 500,
		Paraphrases: 0, HyDECache: 8,
	}
	srv, _ := newAdminStub(t, stats, server.AdminConfigResponse{})
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "stats",
			[]string{"-server", srv.URL, "-token", "secret"}); err != nil {
			t.Fatalf("admin stats: %v", err)
		}
	})
	if !strings.Contains(out, "=== LLM caches ===") {
		t.Errorf("LLM caches section should appear when HyDECache > 0; got %q", out)
	}
	if !strings.Contains(out, "HyDE passages:       8") {
		t.Errorf("HyDE line missing; got %q", out)
	}
	if strings.Contains(out, "Paraphrases:") {
		t.Errorf("Paraphrases line should be hidden when count = 0; got %q", out)
	}
}

func TestAdminStatsTokenFromEnv(t *testing.T) {
	// No -token flag → fall back to COSIFT_ADMIN_TOKEN env.
	t.Setenv("COSIFT_ADMIN_TOKEN", "env-secret")
	srv, capture := newAdminStub(t,
		server.AdminStatsResponse{Documents: 1},
		server.AdminConfigResponse{})
	cfg := config.Default()
	_ = captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "stats",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("admin stats: %v", err)
		}
	})
	_, auth := capture()
	if auth != "Bearer env-secret" {
		t.Errorf("env token not picked up; got %q", auth)
	}
}

func TestAdminStatsTokenFlagOverridesEnv(t *testing.T) {
	t.Setenv("COSIFT_ADMIN_TOKEN", "env-secret")
	srv, capture := newAdminStub(t,
		server.AdminStatsResponse{Documents: 1},
		server.AdminConfigResponse{})
	cfg := config.Default()
	_ = captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "stats",
			[]string{"-server", srv.URL, "-token", "flag-secret"}); err != nil {
			t.Fatalf("admin stats: %v", err)
		}
	})
	_, auth := capture()
	if auth != "Bearer flag-secret" {
		t.Errorf("-token flag should win over env; got %q", auth)
	}
}

func TestAdminStatsSummaryOutput(t *testing.T) {
	// -summary produces a single-line compact output.
	stats := server.AdminStatsResponse{
		Documents:     1234,
		Frontier:      store.FrontierStats{Queued: 56, InFlight: 2, Done: 1170, Errored: 12},
		EmbedderModel: "text-embedding-3-small",
	}
	srv, _ := newAdminStub(t, stats, server.AdminConfigResponse{})
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "stats",
			[]string{"-server", srv.URL, "-token", "k", "-summary"}); err != nil {
			t.Fatalf("admin stats -summary: %v", err)
		}
	})
	// Single line (one trailing newline).
	if strings.Count(out, "\n") != 1 {
		t.Errorf("-summary should produce exactly 1 line; got %d newlines:\n%q", strings.Count(out, "\n"), out)
	}
	// All key fields present.
	for _, want := range []string{
		"1234 docs",
		"56 queued",
		"12 errored",
		"embedder=text-embedding-3-small",
		" · ", // separator
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in summary: %q", want, out)
		}
	}
	// Should NOT contain the multi-section headers from the full output.
	for _, banned := range []string{"=== Index ===", "=== Frontier ===", "=== Capabilities ==="} {
		if strings.Contains(out, banned) {
			t.Errorf("-summary should suppress section headers; found %q in: %q", banned, out)
		}
	}
}

func TestAdminStatsSummaryOmitsAbsentEmbedder(t *testing.T) {
	// When EmbedderModel is empty (e.g. BM25-only deployment), the embedder
	// segment is omitted entirely — no trailing "embedder=" with empty value.
	stats := server.AdminStatsResponse{
		Documents: 100,
		Frontier:  store.FrontierStats{Queued: 0, Errored: 0},
	}
	srv, _ := newAdminStub(t, stats, server.AdminConfigResponse{})
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "stats",
			[]string{"-server", srv.URL, "-summary"}); err != nil {
			t.Fatalf("admin stats -summary (no embedder): %v", err)
		}
	})
	if strings.Contains(out, "embedder=") {
		t.Errorf("empty embedder should be omitted; got: %q", out)
	}
	// Other fields still present.
	if !strings.Contains(out, "100 docs") {
		t.Errorf("docs count missing: %q", out)
	}
}

func TestAdminStatsSummaryJSONMutex(t *testing.T) {
	// -summary and -json have incompatible output shapes; mutex catches the
	// confusion before the HTTP call.
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "stats",
		[]string{"-server", "http://127.0.0.1:1", "-summary", "-json"})
	if err == nil {
		t.Fatal("-summary and -json should be mutually exclusive")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("error should explain the conflict: %v", err)
	}
}

func TestAdminStatsJSONPassthrough(t *testing.T) {
	stats := server.AdminStatsResponse{Documents: 42, Terms: 100}
	srv, _ := newAdminStub(t, stats, server.AdminConfigResponse{})
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "stats",
			[]string{"-server", srv.URL, "-json"}); err != nil {
			t.Fatalf("admin stats -json: %v", err)
		}
	})
	var decoded server.AdminStatsResponse
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("decode: %v\nout=%q", err, out)
	}
	if decoded.Documents != 42 {
		t.Errorf("roundtrip mismatch: %+v", decoded)
	}
}

func TestAdminConfigHumanOutput(t *testing.T) {
	cfgResp := server.AdminConfigResponse{
		Version: "0.0.1-dev",
		Defaults: server.Defaults{
			Retriever: "hybrid",
			Expand:    true,
		},
		Capabilities: server.Caps{
			DenseEnabled:  true,
			ChatEnabled:   true,
			RerankEnabled: false,
			AdminEnabled:  true,
			EmbedderModel: "text-embedding-3-small",
			ChatModel:     "gpt-4o",
		},
	}
	srv, capture := newAdminStub(t, server.AdminStatsResponse{}, cfgResp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "config",
			[]string{"-server", srv.URL, "-token", "secret"}); err != nil {
			t.Fatalf("admin config: %v", err)
		}
	})
	path, _ := capture()
	if path != "/admin/config" {
		t.Errorf("wrong path hit: %q", path)
	}
	for _, want := range []string{
		"Cosift version: 0.0.1-dev",
		"=== Defaults ===",
		"Retriever: hybrid",
		"Expand:    true",
		"=== Capabilities ===",
		"Dense:        true",
		"Rerank:       false",
		"Admin:        true",
		"Embedder model:  text-embedding-3-small",
		"Chat model:      gpt-4o",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output: %q", want, out)
		}
	}
}

func TestAdminUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "stats", []string{"-server", srv.URL})
	if err == nil {
		t.Fatal("expected error from 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("status should be in error: %v", err)
	}
}

func newRecrawlStub(t *testing.T, resp server.AdminRecrawlResponse) (*httptest.Server, func() (method, auth string, body *server.AdminRecrawlRequest)) {
	t.Helper()
	var lastMethod, lastAuth string
	var lastBody *server.AdminRecrawlRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/recrawl" {
			http.NotFound(w, r)
			return
		}
		lastMethod = r.Method
		lastAuth = r.Header.Get("Authorization")
		var req server.AdminRecrawlRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		lastBody = &req
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv, func() (string, string, *server.AdminRecrawlRequest) {
		return lastMethod, lastAuth, lastBody
	}
}

func TestAdminRecrawlRequiresConfirm(t *testing.T) {
	// Without -y, the CLI errors WITHOUT hitting the server. Verify by giving
	// an unreachable server URL and ensuring the error names the -y guard.
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "recrawl",
		[]string{"-server", "http://127.0.0.1:1", "https://x/a"})
	if err == nil {
		t.Fatal("recrawl without -y should error")
	}
	if !strings.Contains(err.Error(), "-y") {
		t.Errorf("error should mention -y guard: %v", err)
	}
	// The error message should also include the URL count.
	if !strings.Contains(err.Error(), "1 URL") {
		t.Errorf("error should report URL count: %v", err)
	}
}

func TestAdminRecrawlRequiresURLs(t *testing.T) {
	// -y set but no URLs given → error.
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "recrawl",
		[]string{"-server", "http://127.0.0.1:1", "-y"})
	if err == nil {
		t.Fatal("recrawl without URLs should error")
	}
	if !strings.Contains(err.Error(), "URL") {
		t.Errorf("error should mention URLs: %v", err)
	}
}

func TestAdminRecrawlHappyPath(t *testing.T) {
	resp := server.AdminRecrawlResponse{
		Queued: []string{"https://x/a", "https://x/b"},
	}
	srv, capture := newRecrawlStub(t, resp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl",
			[]string{"-server", srv.URL, "-token", "secret", "-y", "https://x/a", "https://x/b"}); err != nil {
			t.Fatalf("admin recrawl: %v", err)
		}
	})
	method, auth, body := capture()
	if method != http.MethodPost {
		t.Errorf("want POST, got %s", method)
	}
	if auth != "Bearer secret" {
		t.Errorf("bearer header not propagated: %q", auth)
	}
	if body == nil || len(body.URLs) != 2 {
		t.Fatalf("body not captured / wrong URL count: %+v", body)
	}
	if body.URLs[0] != "https://x/a" || body.URLs[1] != "https://x/b" {
		t.Errorf("URLs not forwarded in order: %+v", body.URLs)
	}
	if !strings.Contains(out, "Queued 2 URL(s) for recrawl.") {
		t.Errorf("missing queued summary: %q", out)
	}
}

func TestAdminRecrawlFromFile(t *testing.T) {
	// -file with comments + blanks should produce a clean URL list in the POST body.
	dir := t.TempDir()
	path := dir + "/urls.txt"
	if err := os.WriteFile(path, []byte("# header\nhttps://x/a\n\nhttps://x/b\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	srv, capture := newRecrawlStub(t, server.AdminRecrawlResponse{
		Queued: []string{"https://x/a", "https://x/b"},
	})
	cfg := config.Default()
	_ = captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl",
			[]string{"-server", srv.URL, "-y", "-file", path}); err != nil {
			t.Fatalf("admin recrawl: %v", err)
		}
	})
	_, _, body := capture()
	if body == nil || len(body.URLs) != 2 {
		t.Fatalf("expected 2 URLs (blanks + comments stripped): %+v", body)
	}
}

func TestAdminRecrawlPerURLErrorsRendered(t *testing.T) {
	// Server returns some queued + some errors. Both should render, errors
	// sorted alphabetically for stable output.
	resp := server.AdminRecrawlResponse{
		Queued: []string{"https://x/a"},
		Errors: map[string]string{
			"https://x/c": "bad scheme",
			"https://x/b": "url too long",
		},
	}
	srv, _ := newRecrawlStub(t, resp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl",
			[]string{"-server", srv.URL, "-y", "https://x/a", "https://x/b", "https://x/c"}); err != nil {
			t.Fatalf("admin recrawl: %v", err)
		}
	})
	if !strings.Contains(out, "Queued 1 URL(s)") {
		t.Errorf("missing queued count: %q", out)
	}
	if !strings.Contains(out, "2 URL(s) failed to enqueue:") {
		t.Errorf("missing errors header: %q", out)
	}
	for _, want := range []string{
		"https://x/b: url too long",
		"https://x/c: bad scheme",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q: %q", want, out)
		}
	}
	// Sorted output → x/b appears before x/c.
	if strings.Index(out, "x/b") > strings.Index(out, "x/c") {
		t.Errorf("errors should be sorted alphabetically: %q", out)
	}
}

func TestAdminRecrawlJSONPassthrough(t *testing.T) {
	resp := server.AdminRecrawlResponse{Queued: []string{"https://x/a"}}
	srv, _ := newRecrawlStub(t, resp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl",
			[]string{"-server", srv.URL, "-y", "-json", "https://x/a"}); err != nil {
			t.Fatalf("admin recrawl -json: %v", err)
		}
	})
	var decoded server.AdminRecrawlResponse
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("decode: %v\nout=%q", err, out)
	}
	if len(decoded.Queued) != 1 || decoded.Queued[0] != "https://x/a" {
		t.Errorf("roundtrip mismatch: %+v", decoded)
	}
}

func TestAdminRecrawlServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "too many urls", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "recrawl",
		[]string{"-server", srv.URL, "-y", "https://x/a"})
	if err == nil {
		t.Fatal("400 should surface")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("status should be in error: %v", err)
	}
}

func TestAdminNoTokenOmitsAuthHeader(t *testing.T) {
	// No -token flag AND no env → no Authorization header sent. The server
	// will 401, but the wire-level check is that we don't accidentally send
	// `Authorization: Bearer ` (empty bearer is a bug, not a feature).
	os.Unsetenv("COSIFT_ADMIN_TOKEN")
	srv, capture := newAdminStub(t,
		server.AdminStatsResponse{Documents: 1},
		server.AdminConfigResponse{})
	cfg := config.Default()
	_ = captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "stats",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("admin stats: %v", err)
		}
	})
	_, auth := capture()
	if auth != "" {
		t.Errorf("no token → no Authorization header expected, got %q", auth)
	}
}
