package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/server"
)

// newRecrawlDomainStub serves /admin/recrawl-by-domain with a canned response
// and captures the last request body so tests can verify the dry_run flag
// propagated correctly.
func newRecrawlDomainStub(t *testing.T, resp server.AdminRecrawlByDomainResponse) (*httptest.Server, func() (method, auth string, body *server.AdminRecrawlByDomainRequest)) {
	t.Helper()
	var lastMethod, lastAuth string
	var lastBody *server.AdminRecrawlByDomainRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/recrawl-by-domain" {
			http.NotFound(w, r)
			return
		}
		lastMethod = r.Method
		lastAuth = r.Header.Get("Authorization")
		var req server.AdminRecrawlByDomainRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		lastBody = &req
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv, func() (string, string, *server.AdminRecrawlByDomainRequest) {
		return lastMethod, lastAuth, lastBody
	}
}

func TestAdminRecrawlDomainRequiresGuardFlag(t *testing.T) {
	// Neither -y nor -dry-run → error WITHOUT hitting the server.
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "recrawl-domain",
		[]string{"-server", "http://127.0.0.1:1", "example.com"})
	if err == nil {
		t.Fatal("recrawl-domain without -y or -dry-run should error")
	}
	if !strings.Contains(err.Error(), "-y") || !strings.Contains(err.Error(), "-dry-run") {
		t.Errorf("error should mention both guards: %v", err)
	}
	if !strings.Contains(err.Error(), "example.com") {
		t.Errorf("error should name the pattern: %v", err)
	}
}

func TestAdminRecrawlDomainRequiresPattern(t *testing.T) {
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "recrawl-domain", []string{"-y"})
	if err == nil {
		t.Fatal("missing pattern should error")
	}
	if !strings.Contains(err.Error(), "pattern") {
		t.Errorf("error should mention pattern: %v", err)
	}
}

func TestAdminRecrawlDomainTooManyPositional(t *testing.T) {
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "recrawl-domain",
		[]string{"-y", "example.com", "other.com"})
	if err == nil {
		t.Fatal("multiple patterns should error")
	}
	if !strings.Contains(err.Error(), "one") {
		t.Errorf("error should mention one-pattern limit: %v", err)
	}
}

func TestAdminRecrawlDomainHappyPath(t *testing.T) {
	resp := server.AdminRecrawlByDomainResponse{
		Domain:  "example.com",
		Matched: 42,
		Queued:  42,
	}
	srv, capture := newRecrawlDomainStub(t, resp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl-domain",
			[]string{"-server", srv.URL, "-token", "secret", "-y", "example.com"}); err != nil {
			t.Fatalf("recrawl-domain: %v", err)
		}
	})
	method, auth, body := capture()
	if method != http.MethodPost {
		t.Errorf("want POST, got %s", method)
	}
	if auth != "Bearer secret" {
		t.Errorf("bearer not propagated: %q", auth)
	}
	if body == nil {
		t.Fatal("server got no body")
	}
	if body.Domain != "example.com" {
		t.Errorf("domain not forwarded: %q", body.Domain)
	}
	if body.DryRun {
		t.Errorf("-y should set dry_run=false; got true")
	}
	if !strings.Contains(out, "Queued 42/42 URL(s)") {
		t.Errorf("missing queued summary: %q", out)
	}
}

func TestAdminRecrawlDomainDryRun(t *testing.T) {
	resp := server.AdminRecrawlByDomainResponse{
		Domain:  "example.com",
		Matched: 142,
		Queued:  0,
		DryRun:  true,
	}
	srv, capture := newRecrawlDomainStub(t, resp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl-domain",
			[]string{"-server", srv.URL, "-dry-run", "example.com"}); err != nil {
			t.Fatalf("recrawl-domain dry-run: %v", err)
		}
	})
	_, _, body := capture()
	if body == nil || !body.DryRun {
		t.Errorf("dry_run not propagated: %+v", body)
	}
	if !strings.Contains(out, "Dry-run: 142 URL(s) match") {
		t.Errorf("missing dry-run summary: %q", out)
	}
	if !strings.Contains(out, "Re-run with -y") {
		t.Errorf("dry-run output should hint at next step: %q", out)
	}
}

func TestAdminRecrawlDomainDryRunListsURLsUnderCap(t *testing.T) {
	// dry-run prints the URL list when server returned it.
	// Small list (3 URLs) under the default -limit-list cap (20) → all printed,
	// no "... more" suffix.
	urls := []string{"https://example.com/a", "https://blog.example.com/b", "https://example.com/c"}
	resp := server.AdminRecrawlByDomainResponse{
		Domain:  "example.com",
		Matched: 3,
		Queued:  0,
		DryRun:  true,
		URLs:    urls,
	}
	srv, _ := newRecrawlDomainStub(t, resp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl-domain",
			[]string{"-server", srv.URL, "-dry-run", "example.com"}); err != nil {
			t.Fatalf("recrawl-domain dry-run: %v", err)
		}
	})
	for _, u := range urls {
		if !strings.Contains(out, u) {
			t.Errorf("missing URL %q in dry-run output: %q", u, out)
		}
	}
	if strings.Contains(out, "more") {
		t.Errorf("3 URLs under cap shouldn't show suffix: %q", out)
	}
	// Existing summary still present.
	if !strings.Contains(out, "Dry-run: 3 URL(s) match") {
		t.Errorf("missing summary line: %q", out)
	}
}

func TestAdminRecrawlDomainDryRunListsURLsOverCap(t *testing.T) {
	// 50 URLs, default cap 20 → first 20 printed + "... (30 more — use -limit-list -1 to see all)".
	urls := make([]string, 50)
	for i := range urls {
		urls[i] = fmt.Sprintf("https://example.com/doc-%02d", i)
	}
	resp := server.AdminRecrawlByDomainResponse{
		Domain: "example.com", Matched: 50, DryRun: true, URLs: urls,
	}
	srv, _ := newRecrawlDomainStub(t, resp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl-domain",
			[]string{"-server", srv.URL, "-dry-run", "example.com"}); err != nil {
			t.Fatalf("recrawl-domain dry-run: %v", err)
		}
	})
	// First 20 URLs printed.
	for i := 0; i < 20; i++ {
		if !strings.Contains(out, urls[i]) {
			t.Errorf("missing URL %q (under cap): %q", urls[i], out)
		}
	}
	// 21st URL NOT printed.
	if strings.Contains(out, urls[20]) {
		t.Errorf("URL %d (at cap+1) should be suppressed: %q", 21, out)
	}
	// "... more" suffix with correct remainder count.
	if !strings.Contains(out, "(30 more — use -limit-list -1 to see all)") {
		t.Errorf("missing suffix with remainder count: %q", out)
	}
}

func TestAdminRecrawlDomainDryRunUnlimitedList(t *testing.T) {
	// -limit-list -1 → print all URLs even when there are 50.
	urls := make([]string, 50)
	for i := range urls {
		urls[i] = fmt.Sprintf("https://example.com/doc-%02d", i)
	}
	resp := server.AdminRecrawlByDomainResponse{
		Domain: "example.com", Matched: 50, DryRun: true, URLs: urls,
	}
	srv, _ := newRecrawlDomainStub(t, resp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl-domain",
			[]string{"-server", srv.URL, "-dry-run", "-limit-list", "-1", "example.com"}); err != nil {
			t.Fatalf("recrawl-domain dry-run -limit-list -1: %v", err)
		}
	})
	// All 50 URLs printed.
	if !strings.Contains(out, urls[49]) {
		t.Errorf("URL 50 missing from unlimited output: %q", out[:200])
	}
	// No "more" suffix when all are shown.
	if strings.Contains(out, "more") {
		t.Errorf("unlimited shouldn't show suffix: %q", out[:200])
	}
}

func TestAdminRecrawlDomainDryRunCountOnly(t *testing.T) {
	// -limit-list 0 → preserve count-only behavior.
	urls := []string{"https://example.com/a", "https://example.com/b"}
	resp := server.AdminRecrawlByDomainResponse{
		Domain: "example.com", Matched: 2, DryRun: true, URLs: urls,
	}
	srv, _ := newRecrawlDomainStub(t, resp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl-domain",
			[]string{"-server", srv.URL, "-dry-run", "-limit-list", "0", "example.com"}); err != nil {
			t.Fatalf("recrawl-domain dry-run -limit-list 0: %v", err)
		}
	})
	// Count line present, but no URLs.
	if !strings.Contains(out, "Dry-run: 2 URL(s)") {
		t.Errorf("count line missing: %q", out)
	}
	for _, u := range urls {
		if strings.Contains(out, u) {
			t.Errorf("URL %q should be suppressed by -limit-list 0: %q", u, out)
		}
	}
}

func TestAdminRecrawlDomainBothFlagsDryRunWins(t *testing.T) {
	// -y AND -dry-run together → dry-run wins (safer-on-conflict).
	resp := server.AdminRecrawlByDomainResponse{
		Domain: "example.com", Matched: 5, Queued: 0, DryRun: true,
	}
	srv, capture := newRecrawlDomainStub(t, resp)
	cfg := config.Default()
	_ = captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl-domain",
			[]string{"-server", srv.URL, "-y", "-dry-run", "example.com"}); err != nil {
			t.Fatalf("recrawl-domain: %v", err)
		}
	})
	_, _, body := capture()
	if body == nil || !body.DryRun {
		t.Errorf("conflicting flags: dry-run should win; body=%+v", body)
	}
}

func TestAdminRecrawlDomainJSONPassthrough(t *testing.T) {
	resp := server.AdminRecrawlByDomainResponse{
		Domain: "example.com", Matched: 3, Queued: 3,
	}
	srv, _ := newRecrawlDomainStub(t, resp)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "recrawl-domain",
			[]string{"-server", srv.URL, "-y", "-json", "example.com"}); err != nil {
			t.Fatalf("recrawl-domain -json: %v", err)
		}
	})
	var decoded server.AdminRecrawlByDomainResponse
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("decode: %v\nout=%q", err, out)
	}
	if decoded.Queued != 3 {
		t.Errorf("roundtrip mismatch: %+v", decoded)
	}
}

func TestAdminRecrawlDomainServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "matched 99999 URLs; max 10000 per call", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "recrawl-domain",
		[]string{"-server", srv.URL, "-y", "example.com"})
	if err == nil {
		t.Fatal("400 should surface")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("status should be in error: %v", err)
	}
}
