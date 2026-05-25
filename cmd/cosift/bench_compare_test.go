package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeBenchReport(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestLoadBenchReport(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.json")
	writeBenchReport(t, p,
		`{"mode":"vector","n":1000,"dim":384,"queries":100,"p50_us":190,"p95_us":220,"p99_us":480,"qps":5000}`+"\n"+
			`{"mode":"bm25","n":1000,"queries":100,"p50_us":2100,"p95_us":3200,"p99_us":3400,"qps":450}`+"\n"+
			`{"mode":"crawl","n":1000,"elapsed_us":2000000,"docs":1000,"pages_per_sec":500,"terms":1001}`+"\n")
	r, err := loadBenchReport(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(r) != 3 {
		t.Errorf("got %d modes, want 3", len(r))
	}
	v := r["vector"]
	if v == nil || v.QPS != 5000 || v.Dim != 384 {
		t.Errorf("vector: %+v", v)
	}
	c := r["crawl"]
	if c == nil || c.Docs != 1000 || c.PagesPerSec != 500 {
		t.Errorf("crawl: %+v", c)
	}
}

func TestLoadBenchReportSkipsBlankAndUnparseableLines(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "noisy.json")
	writeBenchReport(t, p,
		"some preamble that's not JSON\n"+
			"\n"+ // blank
			`{"mode":"vector","n":100,"qps":1000}`+"\n"+
			"oh hey another non-json line\n"+
			"{this is not valid json}\n"+ // brace prefix but invalid
			`{"mode":"crawl","n":100,"pages_per_sec":50}`+"\n")
	r, err := loadBenchReport(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(r) != 2 {
		t.Errorf("want 2 valid records, got %d: %+v", len(r), r)
	}
}

func TestLoadBenchReportEmptyError(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "empty.json")
	writeBenchReport(t, p, "")
	_, err := loadBenchReport(p)
	if err == nil {
		t.Errorf("empty file should error")
	}
	if !strings.Contains(err.Error(), "no parseable bench records") {
		t.Errorf("error should mention expected format: %v", err)
	}
}

func TestLoadBenchReportMissingFile(t *testing.T) {
	_, err := loadBenchReport("/nonexistent/path/cosift-bench.json")
	if err == nil {
		t.Errorf("missing file should error")
	}
}

func TestSortedModes(t *testing.T) {
	in := map[string]*benchResult{
		"crawl":  {Mode: "crawl"},
		"vector": {Mode: "vector"},
	}
	got := sortedModes(in)
	if len(got) != 2 || got[0] != "vector" || got[1] != "crawl" {
		t.Errorf("ordering: got %v want [vector crawl]", got)
	}
	// Unknown mode preserved at the end.
	in["custom"] = &benchResult{Mode: "custom"}
	got = sortedModes(in)
	if len(got) != 3 || got[2] != "custom" {
		t.Errorf("unknown mode: got %v", got)
	}
}

func TestUnionPreservesOrder(t *testing.T) {
	a := map[string]*benchResult{"vector": {Mode: "vector"}}
	b := map[string]*benchResult{"vector": {Mode: "vector"}, "crawl": {Mode: "crawl"}}
	got := union(a, b)
	// vector first (in known order, in a), then crawl (in known order, in b only)
	if len(got) != 2 || got[0] != "vector" || got[1] != "crawl" {
		t.Errorf("union: got %v want [vector crawl]", got)
	}
}

// End-to-end: write two reports, run runBenchCompare against them, verify
// the function doesn't error. Output goes to stdout; we don't assert on the
// exact text (covered by smoke runs documented in NOTES).
func TestRunBenchCompareEndToEnd(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.json")
	b := filepath.Join(dir, "b.json")
	writeBenchReport(t, a, `{"mode":"crawl","n":100,"pages_per_sec":50,"docs":100,"elapsed_us":2000000,"terms":101}`+"\n")
	writeBenchReport(t, b, `{"mode":"crawl","n":100,"pages_per_sec":75,"docs":100,"elapsed_us":1500000,"terms":101}`+"\n")
	if err := runBenchCompare([]string{a, b}); err != nil {
		t.Fatalf("runBenchCompare: %v", err)
	}
}

func TestRunBenchCompareUsage(t *testing.T) {
	// Wrong arg count → usage error.
	err := runBenchCompare([]string{"only-one-arg.json"})
	if err == nil {
		t.Errorf("expected usage error")
	}
}
