package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// TestBenchCrawlSmall verifies the crawler bench mode runs to
// completion against an in-process website. The benchCrawl function is
// self-contained — spins up the httptest server, runs the crawl, prints
// throughput. We just need to confirm it doesn't error and the printed
// output won't hang.
//
// Uses N=10 to keep test runtime under a few seconds; the crawler's ~1.5s
// terminator grace dominates anyway at this size.
func TestBenchCrawlSmall(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping bench in -short")
	}
	r, err := benchCrawl(context.Background(), 10, 0)
	if err != nil {
		t.Fatalf("benchCrawl: %v", err)
	}
	if r == nil || r.Mode != "crawl" {
		t.Errorf("unexpected result: %+v", r)
	}
	if r.Docs != 10 {
		t.Errorf("docs: got %d want 10", r.Docs)
	}
}

// TestBenchResultJSON verifies the JSON output is parseable + carries
// the expected field names. The format is documented as a CI-ingestion contract;
// this test locks it against accidental rename.
func TestBenchResultJSON(t *testing.T) {
	r := &benchResult{
		Mode: "crawl", N: 100,
		ElapsedMicros: 1500000, Docs: 100, PagesPerSec: 66.5, Terms: 101, PerHostDelayMs: 50,
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	// Confirm snake_case keys (json tags) survived.
	for _, want := range []string{`"mode":"crawl"`, `"n":100`, `"elapsed_us":1500000`, `"docs":100`, `"pages_per_sec":66.5`, `"terms":101`, `"per_host_delay_ms":50`} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %q in: %s", want, s)
		}
	}
	// Round-trip back into a fresh struct.
	var rr benchResult
	if err := json.Unmarshal(b, &rr); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rr.Mode != "crawl" || rr.N != 100 || rr.PerHostDelayMs != 50 {
		t.Errorf("round-trip mismatch: %+v", rr)
	}
}

// TestBenchResultJSONOmitEmpty verifies mode-specific fields don't pollute
// other modes' JSON output (e.g. crawl shouldn't carry dim/p50/p95 fields,
// vector shouldn't carry pages_per_sec). Keeps the CI-ingestion payload tight.
func TestBenchResultJSONOmitEmpty(t *testing.T) {
	r := &benchResult{Mode: "vector", N: 1000, Dim: 384, Queries: 100, P50Micros: 196, P95Micros: 220, P99Micros: 250, QPS: 5000}
	b, _ := json.Marshal(r)
	s := string(b)
	if strings.Contains(s, "elapsed_us") || strings.Contains(s, "pages_per_sec") || strings.Contains(s, "docs") {
		t.Errorf("vector result leaked crawl-mode fields: %s", s)
	}
}
