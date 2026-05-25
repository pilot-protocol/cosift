package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/calinteodor/cosift/internal/store"
)

func seedRecrawlByDomainStore(t *testing.T) *store.Store {
	t.Helper()
	// Iter 134: OpenMemory.
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	docs := []store.Document{
		{URL: "https://example.com/a", Domain: "example.com", Title: "A", Text: "a", Source: "test", FetchedAt: time.Now()},
		{URL: "https://blog.example.com/b", Domain: "blog.example.com", Title: "B", Text: "b", Source: "test", FetchedAt: time.Now()},
		{URL: "https://evilexample.com/c", Domain: "evilexample.com", Title: "C", Text: "c", Source: "test", FetchedAt: time.Now()},
		{URL: "https://other.org/d", Domain: "other.org", Title: "D", Text: "d", Source: "test", FetchedAt: time.Now()},
	}
	for i := range docs {
		if _, err := s.UpsertDocument(context.Background(), &docs[i]); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	return s
}

func TestAdminRecrawlByDomainHappyPath(t *testing.T) {
	s := seedRecrawlByDomainStore(t)
	srv := New(s).WithAdminToken("secret")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	body := strings.NewReader(`{"domain":"example.com"}`)
	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/recrawl-by-domain", body)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	var got AdminRecrawlByDomainResponse
	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.Domain != "example.com" {
		t.Errorf("domain: got %q", got.Domain)
	}
	// example.com + blog.example.com = 2. evilexample.com must NOT match.
	if got.Matched != 2 {
		t.Errorf("matched: got %d want 2 (%+v)", got.Matched, got)
	}
	if got.Queued != 2 {
		t.Errorf("queued: got %d want 2", got.Queued)
	}
	if len(got.Errors) != 0 {
		t.Errorf("expected no per-URL errors, got %+v", got.Errors)
	}
}

func TestAdminRecrawlByDomainRequiresAuth(t *testing.T) {
	s := seedRecrawlByDomainStore(t)
	srv := New(s).WithAdminToken("secret")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// No Authorization header → 401.
	body := strings.NewReader(`{"domain":"example.com"}`)
	resp, _ := http.Post(httpSrv.URL+"/admin/recrawl-by-domain", "application/json", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		t.Errorf("want 401/403, got %d", resp.StatusCode)
	}
}

func TestAdminRecrawlByDomainBadInput(t *testing.T) {
	s := seedRecrawlByDomainStore(t)
	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	send := func(body string) int {
		req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/recrawl-by-domain", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer k")
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := send(`not json`); got != 400 {
		t.Errorf("bad json: got %d want 400", got)
	}
	if got := send(`{"domain":""}`); got != 400 {
		t.Errorf("empty domain: got %d want 400", got)
	}
	if got := send(`{"domain":"   "}`); got != 400 {
		t.Errorf("whitespace-only domain: got %d want 400", got)
	}
}

func TestAdminRecrawlByDomainDryRun(t *testing.T) {
	// Iter 111: dry_run enumerates but doesn't call RecrawlURL. Matched > 0,
	// Queued == 0, DryRun echoed in response.
	s := seedRecrawlByDomainStore(t)
	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	body := strings.NewReader(`{"domain":"example.com","dry_run":true}`)
	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/recrawl-by-domain", body)
	req.Header.Set("Authorization", "Bearer k")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	var got AdminRecrawlByDomainResponse
	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.Matched != 2 {
		t.Errorf("dry-run should still report matched count; got %d", got.Matched)
	}
	if got.Queued != 0 {
		t.Errorf("dry-run should NOT queue; got queued=%d", got.Queued)
	}
	if !got.DryRun {
		t.Errorf("response should echo dry_run=true; got false")
	}
}

func TestAdminRecrawlByDomainDryRunIncludesURLs(t *testing.T) {
	// Iter 123: dry_run=true returns the matched URLs in the URLs field,
	// not just the count. Operators previewing a pattern need to see WHICH
	// URLs would be queued.
	s := seedRecrawlByDomainStore(t)
	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	body := strings.NewReader(`{"domain":"example.com","dry_run":true}`)
	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/recrawl-by-domain", body)
	req.Header.Set("Authorization", "Bearer k")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	var got AdminRecrawlByDomainResponse
	_ = json.NewDecoder(resp.Body).Decode(&got)

	if !got.DryRun {
		t.Errorf("dry_run not echoed; got %+v", got)
	}
	if len(got.URLs) != 2 {
		t.Fatalf("dry_run should return 2 URLs (example.com + blog.example.com); got %d: %+v", len(got.URLs), got.URLs)
	}
	// URL list should contain both example.com and blog.example.com, NOT evilexample.com.
	hasExact, hasSub := false, false
	for _, u := range got.URLs {
		if strings.Contains(u, "evilexample.com") {
			t.Errorf("evilexample.com leaked into dry-run URL list: %s", u)
		}
		if u == "https://example.com/a" {
			hasExact = true
		}
		if u == "https://blog.example.com/b" {
			hasSub = true
		}
	}
	if !hasExact || !hasSub {
		t.Errorf("dry-run URL list missing expected entries: %+v", got.URLs)
	}
	// Matched count still reported (sanity check; consistent with URLs slice length).
	if got.Matched != len(got.URLs) {
		t.Errorf("matched (%d) and URLs length (%d) should agree", got.Matched, len(got.URLs))
	}
}

func TestAdminRecrawlByDomainNonDryRunOmitsURLs(t *testing.T) {
	// Iter 123: non-dry-run response should NOT include URLs (would be
	// redundant with queued count + errors map). omitempty keeps response shape small.
	s := seedRecrawlByDomainStore(t)
	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	body := strings.NewReader(`{"domain":"example.com"}`) // no dry_run
	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/recrawl-by-domain", body)
	req.Header.Set("Authorization", "Bearer k")
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	rawBody, _ := io.ReadAll(resp.Body)

	// Check the wire format directly: `urls` field should not appear when omitempty
	// and value is nil.
	if strings.Contains(string(rawBody), `"urls"`) {
		t.Errorf("non-dry-run response should omit urls field: %s", rawBody)
	}

	// Decode and confirm the parsed struct's URLs is nil.
	var got AdminRecrawlByDomainResponse
	_ = json.Unmarshal(rawBody, &got)
	if got.URLs != nil {
		t.Errorf("URLs should be nil in non-dry-run; got %+v", got.URLs)
	}
}

func TestAdminRecrawlByDomainUnknownReturns200Empty(t *testing.T) {
	// A pattern that matches nothing should succeed with matched=0 (not 404).
	// Matches the export-filter convention from iter 104 — empty result is
	// a valid response, not an error.
	s := seedRecrawlByDomainStore(t)
	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/recrawl-by-domain",
		strings.NewReader(`{"domain":"nonexistent.io"}`))
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	var got AdminRecrawlByDomainResponse
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if got.Matched != 0 || got.Queued != 0 {
		t.Errorf("expected 0/0, got matched=%d queued=%d", got.Matched, got.Queued)
	}
}
