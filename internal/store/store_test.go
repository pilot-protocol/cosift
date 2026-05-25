package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestGetDocMetas verifies iter-82's batched URL→DocMeta lookup. Mixed dated
// and undated docs, plus a URL not in the store (should be absent from the
// returned map, not produce an error).
func TestGetDocMetas(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	published := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	docs := []struct {
		url, domain string
		pub         time.Time
	}{
		{"https://a.com/p", "a.com", published},
		{"https://b.com/p", "b.com", time.Time{}}, // undated
		{"https://c.com/p", "c.com", published},
	}
	for _, d := range docs {
		_, err := s.UpsertDocument(ctx, &Document{
			URL: d.url, Domain: d.domain, Title: "T", Text: "x",
			Source: "test", FetchedAt: time.Now(), PublishedAt: d.pub,
		})
		if err != nil {
			t.Fatalf("upsert %s: %v", d.url, err)
		}
	}
	// Query: 3 known URLs + 1 unknown.
	urls := []string{"https://a.com/p", "https://b.com/p", "https://missing.example.com/x", "https://c.com/p"}
	out, err := s.GetDocMetas(ctx, urls)
	if err != nil {
		t.Fatalf("GetDocMetas: %v", err)
	}
	if len(out) != 3 {
		t.Errorf("len: got %d want 3 (missing URL should be absent, not error)", len(out))
	}
	if out["https://a.com/p"].Domain != "a.com" {
		t.Errorf("a domain: %v", out["https://a.com/p"])
	}
	if !out["https://a.com/p"].PublishedAt.Equal(published) {
		t.Errorf("a PublishedAt: got %v want %v", out["https://a.com/p"].PublishedAt, published)
	}
	if !out["https://b.com/p"].PublishedAt.IsZero() {
		t.Errorf("b should have zero PublishedAt (undated), got %v", out["https://b.com/p"].PublishedAt)
	}
	if _, exists := out["https://missing.example.com/x"]; exists {
		t.Errorf("missing URL should not be in the map")
	}
}

// TestFailFrontierStoresError verifies iter-85: FailFrontier persists the
// error string in last_error, which ListErroredFrontier surfaces.
func TestFailFrontierStoresError(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_ = s.PushFrontier(ctx, "https://x/fails", 0, 1.0)
	it, _, _ := s.ClaimFrontier(ctx)
	if err := s.FailFrontier(ctx, it.URL, "http 503: upstream gone"); err != nil {
		t.Fatalf("FailFrontier: %v", err)
	}

	errs, err := s.ListErroredFrontier(ctx, 10)
	if err != nil {
		t.Fatalf("ListErroredFrontier: %v", err)
	}
	if len(errs) != 1 {
		t.Fatalf("want 1 errored, got %d", len(errs))
	}
	if errs[0].URL != "https://x/fails" {
		t.Errorf("url: %q", errs[0].URL)
	}
	if errs[0].LastError != "http 503: upstream gone" {
		t.Errorf("last_error: %q", errs[0].LastError)
	}
	if errs[0].Attempts != 1 {
		t.Errorf("attempts: got %d want 1", errs[0].Attempts)
	}
}

// TestFailFrontierTruncatesLongErrors verifies the 500-char cap so an
// unbounded error message can't bloat the frontier row.
func TestFailFrontierTruncatesLongErrors(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.PushFrontier(ctx, "https://x/long-err", 0, 1.0)
	it, _, _ := s.ClaimFrontier(ctx)

	huge := make([]byte, 5000)
	for i := range huge {
		huge[i] = 'x'
	}
	if err := s.FailFrontier(ctx, it.URL, string(huge)); err != nil {
		t.Fatalf("FailFrontier: %v", err)
	}
	errs, _ := s.ListErroredFrontier(ctx, 10)
	if len(errs) != 1 {
		t.Fatalf("want 1")
	}
	if len(errs[0].LastError) > 500 {
		t.Errorf("LastError should be ≤ 500 chars, got %d", len(errs[0].LastError))
	}
	if !strings.HasSuffix(errs[0].LastError, "...") {
		t.Errorf("truncated string should end with ..., got %q", errs[0].LastError[len(errs[0].LastError)-10:])
	}
}

// TestListErroredFrontierLimit verifies the LIMIT clause + ordering (most
// recent first by enqueued_at).
func TestListErroredFrontierLimit(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		url := fmt.Sprintf("https://x/%d", i)
		_ = s.PushFrontier(ctx, url, 0, 1.0)
		it, _, _ := s.ClaimFrontier(ctx)
		_ = s.FailFrontier(ctx, it.URL, fmt.Sprintf("reason %d", i))
	}
	errs, err := s.ListErroredFrontier(ctx, 3)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(errs) != 3 {
		t.Errorf("limit: got %d want 3", len(errs))
	}
}

// TestGetDocMetasExcerptTruncation verifies iter-83's SQL substr truncation.
// A document with 2000+ chars body produces an Excerpt at most 500 chars.
func TestGetDocMetasExcerptTruncation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	long := make([]byte, 2000)
	for i := range long {
		long[i] = 'a' + byte(i%26)
	}
	_, err := s.UpsertDocument(ctx, &Document{
		URL: "https://x/long", Domain: "x", Title: "T",
		Text: string(long), Source: "test", FetchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	out, err := s.GetDocMetas(ctx, []string{"https://x/long"})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	m := out["https://x/long"]
	if len(m.Excerpt) > 500 {
		t.Errorf("excerpt should be ≤ 500 chars, got %d", len(m.Excerpt))
	}
	if len(m.Excerpt) == 0 {
		t.Errorf("excerpt should not be empty")
	}
}

func TestGetDocMetasEmpty(t *testing.T) {
	s := newTestStore(t)
	out, err := s.GetDocMetas(context.Background(), nil)
	if err != nil {
		t.Fatalf("empty input: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("empty input should return empty map, got %d entries", len(out))
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	// Iter 134: migrated to OpenMemory — no on-disk artifacts, faster tests,
	// honors the directive's "keep disk usage low for tests" language. iter-133
	// shipped the API; this iter switches the highest-frequency helper to use it.
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestFrontierPushClaimComplete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.PushFrontier(ctx, "https://example.com/a", 0, 1.0); err != nil {
		t.Fatalf("push a: %v", err)
	}
	if err := s.PushFrontier(ctx, "https://example.com/b", 1, 0.5); err != nil {
		t.Fatalf("push b: %v", err)
	}
	// Idempotent push.
	if err := s.PushFrontier(ctx, "https://example.com/a", 0, 1.0); err != nil {
		t.Fatalf("re-push a: %v", err)
	}

	stats, err := s.GetFrontierStats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Queued != 2 {
		t.Errorf("queued: got %d want 2", stats.Queued)
	}

	// Higher priority first.
	it, ok, err := s.ClaimFrontier(ctx)
	if err != nil || !ok {
		t.Fatalf("claim 1: ok=%v err=%v", ok, err)
	}
	if it.URL != "https://example.com/a" {
		t.Errorf("first claim: got %q want example.com/a", it.URL)
	}

	stats, _ = s.GetFrontierStats(ctx)
	if stats.InFlight != 1 || stats.Queued != 1 {
		t.Errorf("after first claim: queued=%d in_flight=%d", stats.Queued, stats.InFlight)
	}

	if err := s.CompleteFrontier(ctx, it.URL); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// Second claim picks up b.
	it2, ok, err := s.ClaimFrontier(ctx)
	if err != nil || !ok || it2.URL != "https://example.com/b" {
		t.Fatalf("claim 2: got %+v ok=%v err=%v", it2, ok, err)
	}

	// Simulate a crash mid-flight: recover should bring it back to queued.
	if err := s.RecoverInFlight(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	stats, _ = s.GetFrontierStats(ctx)
	if stats.InFlight != 0 || stats.Queued != 1 {
		t.Errorf("after recover: queued=%d in_flight=%d done=%d", stats.Queued, stats.InFlight, stats.Done)
	}

	// Empty queue → ok=false.
	for {
		_, ok, err := s.ClaimFrontier(ctx)
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		if !ok {
			break
		}
	}
}

func TestGCErroredFrontier(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Push 3 URLs, fail each 6 times → all status='error', attempts=6.
	for _, u := range []string{"https://a/1", "https://a/2", "https://a/3"} {
		_ = s.PushFrontier(ctx, u, 0, 1.0)
		it, _, _ := s.ClaimFrontier(ctx)
		for i := 0; i < 6; i++ {
			_ = s.FailFrontier(ctx, it.URL, "test error")
		}
	}

	// Push one "fresh" errored row with only 2 attempts.
	_ = s.PushFrontier(ctx, "https://b/1", 0, 1.0)
	it, _, _ := s.ClaimFrontier(ctx)
	_ = s.FailFrontier(ctx, it.URL, "test error")
	_ = s.FailFrontier(ctx, it.URL, "test error")

	// GC with min_attempts=5 should drop the 3 high-failure rows, keep the new one.
	n, err := s.GCErroredFrontier(ctx, 5)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if n != 3 {
		t.Errorf("dropped rows: got %d want 3", n)
	}
	stats, _ := s.GetFrontierStats(ctx)
	if stats.Errored != 1 {
		t.Errorf("remaining errored: got %d want 1", stats.Errored)
	}
}

func TestRecordOutcomeAndCount(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for i, useful := range []bool{true, true, false, true, false} {
		if err := s.RecordOutcome(ctx, &Outcome{
			Query: "q" + string(rune('a'+i)), URL: "https://x/" + string(rune('a'+i)),
			Score: 0.5, Useful: useful, Source: "test",
		}); err != nil {
			t.Fatalf("record: %v", err)
		}
	}
	total, useful, err := s.CountOutcomes(ctx)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if total != 5 {
		t.Errorf("total: got %d want 5", total)
	}
	if useful != 3 {
		t.Errorf("useful: got %d want 3", useful)
	}
}

func TestFrontierFailureIncrementsAttempts(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.PushFrontier(ctx, "https://x.test/p", 0, 1.0)
	it, ok, _ := s.ClaimFrontier(ctx)
	if !ok {
		t.Fatal("expected claim")
	}
	if err := s.FailFrontier(ctx, it.URL, "test error"); err != nil {
		t.Fatalf("fail: %v", err)
	}

	stats, _ := s.GetFrontierStats(ctx)
	if stats.Errored != 1 {
		t.Errorf("errored: got %d want 1", stats.Errored)
	}
}

// Iter 181: concurrent ClaimFrontier doesn't return SQLITE_BUSY_SNAPSHOT errors.
//
// Pre-fix, ClaimFrontier ran BeginTx → SELECT → UPDATE → COMMIT. Under WAL
// with multiple writers, one worker's SELECT established a read snapshot;
// another worker's UPDATE committed; the first's UPDATE failed with
// SQLITE_BUSY_SNAPSHOT (extended error 517). The fix: single atomic
// UPDATE...RETURNING that acquires the write lock for its duration.
//
// This test reproduces the original contention scenario: on-disk store
// (OpenMemory's SetMaxOpenConns(1) serializes everything through one
// connection and can't surface the race), N concurrent goroutines
// hammering ClaimFrontier. Any "database is locked" error in the goroutine
// returns fails the test.
//
// On-disk because reproducing the WAL multi-writer race REQUIRES multiple
// connections. Uses t.TempDir() so the test cleans up after itself; one of
// the rare disk-using tests in the post-iter-134 store package.
func TestFrontierClaimConcurrencyNoLockErrors(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	// Seed 50 URLs — enough for 8 workers to actually race each other.
	for i := 0; i < 50; i++ {
		url := fmt.Sprintf("https://x.test/%d", i)
		if err := s.PushFrontier(ctx, url, 0, 1.0); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}

	const workers = 8
	var wg sync.WaitGroup
	errCh := make(chan error, workers*100)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each worker claims-and-completes until the queue is empty.
			for {
				it, ok, err := s.ClaimFrontier(ctx)
				if err != nil {
					errCh <- fmt.Errorf("claim: %w", err)
					return
				}
				if !ok {
					return
				}
				if err := s.CompleteFrontier(ctx, it.URL); err != nil {
					errCh <- fmt.Errorf("complete %s: %w", it.URL, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("worker error: %v", err)
	}

	// All 50 should have been processed.
	stats, err := s.GetFrontierStats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Done != 50 {
		t.Errorf("done: got %d want 50", stats.Done)
	}
	if stats.Queued != 0 || stats.InFlight != 0 {
		t.Errorf("queue not drained: queued=%d in_flight=%d", stats.Queued, stats.InFlight)
	}
}

// TestClaimFrontierHostFairness — iter 190. ClaimFrontier should prefer URLs
// whose host has no in-flight row, so the worker pool spreads across hosts
// instead of stacking on a single fanout-heavy domain. Locks in the
// scheduling contract surfaced by the iter-190 v2 crawl analysis where 96%
// of indexed docs came from 2 hosts despite 13 hosts being in the frontier.
func TestClaimFrontierHostFairness(t *testing.T) {
	ctx := context.Background()
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	// Enqueue 10 URLs from hostA (fanout-heavy domain), then 1 each from
	// hostB and hostC. With pure priority+age ordering, all 12 claims would
	// drain hostA first. With host-fair ordering, after claiming the first
	// hostA URL, hostB and hostC must be picked before claiming a SECOND
	// hostA URL.
	for i := 0; i < 10; i++ {
		if err := s.PushFrontier(ctx, fmt.Sprintf("https://a.example.com/%d", i), 0, 0); err != nil {
			t.Fatalf("push hostA %d: %v", i, err)
		}
	}
	if err := s.PushFrontier(ctx, "https://b.example.com/x", 0, 0); err != nil {
		t.Fatalf("push hostB: %v", err)
	}
	if err := s.PushFrontier(ctx, "https://c.example.com/y", 0, 0); err != nil {
		t.Fatalf("push hostC: %v", err)
	}

	// Three claims back-to-back, no Complete between them so all three stay
	// in_flight. The fairness invariant: each host appears AT MOST once in
	// the first three claims.
	seen := map[string]int{}
	for i := 0; i < 3; i++ {
		it, ok, err := s.ClaimFrontier(ctx)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("claim %d: no item returned", i)
		}
		host := extractHost(it.URL)
		seen[host]++
	}
	for host, n := range seen {
		if n > 1 {
			t.Errorf("host %q claimed %d times in first 3 claims; expected ≤1 (host-fair)", host, n)
		}
	}
	if len(seen) != 3 {
		t.Errorf("expected 3 distinct hosts in first 3 claims; got %d: %v", len(seen), seen)
	}

	// Fourth claim must come from hostA (the only host with queued+no-in-flight
	// rows left, since b and c each had only 1 URL and are now in-flight).
	it, ok, err := s.ClaimFrontier(ctx)
	if err != nil || !ok {
		t.Fatalf("4th claim: %v %v", ok, err)
	}
	if h := extractHost(it.URL); h != "a.example.com" {
		t.Errorf("4th claim should be hostA; got %q", h)
	}
}

// TestExtractHost locks in the URL→host parser. Pure-string; avoids url.Parse
// on the hot enqueue path.
func TestExtractHost(t *testing.T) {
	cases := []struct{ url, want string }{
		{"https://en.wikipedia.org/wiki/Goroutine", "en.wikipedia.org"},
		{"http://Example.COM/path", "example.com"},
		{"https://docs.example.com", "docs.example.com"},
		{"https://x.com?q=foo", "x.com"},
		{"https://x.com:8080/", "x.com:8080"},
		{"//no-scheme.com/", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := extractHost(c.url); got != c.want {
			t.Errorf("extractHost(%q) = %q; want %q", c.url, got, c.want)
		}
	}
}

// TestCrawlStatus — iter 193. Verifies the operator-facing crawl snapshot
// returned by Store.CrawlStatus aggregates correctly across frontier statuses,
// host counts, error classes, and rolling-window doc rates.
func TestCrawlStatus(t *testing.T) {
	ctx := context.Background()
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	// Seed the frontier with a mix of statuses + hosts. PushFrontier defaults
	// to 'queued'; we'll bump some to 'done' / 'error' manually.
	urls := []struct{ url, status, errMsg string }{
		{"https://a.example.com/1", "done", ""},
		{"https://a.example.com/2", "done", ""},
		{"https://a.example.com/3", "done", ""},
		{"https://b.example.com/x", "done", ""},
		{"https://b.example.com/y", "queued", ""},
		{"https://c.example.com/p", "error", "http 403"},
		{"https://c.example.com/q", "error", "http 403"},
		{"https://d.example.com/r", "error", "blocked by robots.txt"},
		{"https://e.example.com/s", "in_flight", ""},
	}
	for _, u := range urls {
		if err := s.PushFrontier(ctx, u.url, 0, 0); err != nil {
			t.Fatalf("push: %v", err)
		}
		// Force the row's status / last_error to match the test scenario.
		if _, err := s.db.ExecContext(ctx,
			`UPDATE frontier SET status=?, last_error=? WHERE url=?`, u.status, u.errMsg, u.url); err != nil {
			t.Fatalf("set status: %v", err)
		}
	}
	// Insert one document so r.Documents / r.Terms come back non-zero.
	if _, err := s.UpsertDocument(ctx, &Document{URL: "https://a.example.com/1", Title: "doc1", Text: "hello", FetchedAt: time.Now()}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	r, err := s.CrawlStatus(ctx, 10, 5)
	if err != nil {
		t.Fatalf("CrawlStatus: %v", err)
	}

	if r.Documents != 1 {
		t.Errorf("documents: want 1, got %d", r.Documents)
	}
	if r.UniqueHosts != 2 {
		t.Errorf("unique hosts (a + b have 'done' rows): want 2, got %d", r.UniqueHosts)
	}

	// Frontier breakdown — expect 4 done, 1 queued, 3 error, 1 in_flight.
	got := map[string]int64{}
	for _, fs := range r.FrontierByStatus {
		got[fs.Status] = fs.Count
	}
	want := map[string]int64{"done": 4, "queued": 1, "error": 3, "in_flight": 1}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("frontier[%s]: want %d, got %d", k, v, got[k])
		}
	}

	// Top hosts: a.example.com has 3 done, b has 1.
	if len(r.TopHosts) < 1 || r.TopHosts[0].Host != "a.example.com" || r.TopHosts[0].Count != 3 {
		t.Errorf("top hosts[0]: want {a.example.com 3}, got %+v", r.TopHosts)
	}

	// Error classes: "http 403" (2) tops "blocked by robots.txt" (1).
	if len(r.ErrorClasses) < 2 {
		t.Fatalf("error classes: want ≥2, got %d", len(r.ErrorClasses))
	}
	if r.ErrorClasses[0].LastError != "http 403" || r.ErrorClasses[0].Count != 2 {
		t.Errorf("error[0]: want {http 403, 2}, got %+v", r.ErrorClasses[0])
	}

	// Three rate windows: 5/15/30 min. All should contain our one fresh doc.
	if len(r.RateWindows) != 3 {
		t.Fatalf("rate windows: want 3, got %d", len(r.RateWindows))
	}
	for _, w := range r.RateWindows {
		if w.Count < 1 {
			t.Errorf("rate window %ds: expected ≥1 doc in window, got %d", w.WindowSec, w.Count)
		}
	}
}

// TestFirstIndexedAtStableAcrossReFetches — iter 194. UpsertDocument must set
// first_indexed_at exactly once (on initial INSERT). Subsequent upserts of
// the same URL — what re-crawling produces — must leave first_indexed_at
// unchanged even though fetched_at moves forward.
//
// Without this invariant, crawl-status' rolling-rate windows would count
// every re-fetch as a "new" doc and inflate the rate enormously (observed in
// v3: 5,768 total docs, but the iter-193 fetched_at-based window reported
// 5,769 docs in 30 min — basically the entire index, because the crawler
// re-touched almost every row).
func TestFirstIndexedAtStableAcrossReFetches(t *testing.T) {
	ctx := context.Background()
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer s.Close()

	t1 := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC) // 5 months later

	// First insert.
	if _, err := s.UpsertDocument(ctx, &Document{
		URL: "https://example.com/page", Title: "v1", Text: "body", FetchedAt: t1,
	}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	var firstIndexed1, fetchedAt1 int64
	if err := s.db.QueryRowContext(ctx,
		"SELECT first_indexed_at, fetched_at FROM documents WHERE url=?",
		"https://example.com/page").Scan(&firstIndexed1, &fetchedAt1); err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if firstIndexed1 != t1.Unix() {
		t.Errorf("first_indexed_at on INSERT: want %d, got %d", t1.Unix(), firstIndexed1)
	}

	// Re-fetch (same URL, later timestamp) — UpsertDocument's ON CONFLICT path.
	if _, err := s.UpsertDocument(ctx, &Document{
		URL: "https://example.com/page", Title: "v2", Text: "body v2", FetchedAt: t2,
	}); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	var firstIndexed2, fetchedAt2 int64
	if err := s.db.QueryRowContext(ctx,
		"SELECT first_indexed_at, fetched_at FROM documents WHERE url=?",
		"https://example.com/page").Scan(&firstIndexed2, &fetchedAt2); err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if firstIndexed2 != t1.Unix() {
		t.Errorf("first_indexed_at must NOT bump on re-fetch: want %d (t1), got %d", t1.Unix(), firstIndexed2)
	}
	if fetchedAt2 != t2.Unix() {
		t.Errorf("fetched_at SHOULD bump on re-fetch: want %d (t2), got %d", t2.Unix(), fetchedAt2)
	}
}
