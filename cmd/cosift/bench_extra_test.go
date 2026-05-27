package main

import (
	"math/rand"
	"strings"
	"testing"
)

func TestFormatHumanVector(t *testing.T) {
	r := &benchResult{Mode: "vector", N: 100, Dim: 768, P50Micros: 500, P95Micros: 1200, P99Micros: 2500, QPS: 4500.7}
	out := r.formatHuman()
	if !strings.Contains(out, "vector") {
		t.Errorf("missing mode: %q", out)
	}
	if !strings.Contains(out, "n=100") {
		t.Errorf("missing n: %q", out)
	}
	if !strings.Contains(out, "dim=768") {
		t.Errorf("missing dim: %q", out)
	}
}

func TestFormatHumanBM25(t *testing.T) {
	r := &benchResult{Mode: "bm25", N: 100, P50Micros: 50, P95Micros: 120, P99Micros: 250, QPS: 12500}
	out := r.formatHuman()
	if !strings.Contains(out, "bm25") {
		t.Errorf("missing mode: %q", out)
	}
	if !strings.Contains(out, "qps=12500") {
		t.Errorf("missing qps: %q", out)
	}
}

func TestFormatHumanCrawl(t *testing.T) {
	r := &benchResult{Mode: "crawl", N: 50, ElapsedMicros: 5_000_000, Docs: 50, PagesPerSec: 10, Terms: 200}
	out := r.formatHuman()
	if !strings.Contains(out, "crawl") {
		t.Errorf("missing mode: %q", out)
	}
	if !strings.Contains(out, "docs=50") {
		t.Errorf("missing docs: %q", out)
	}
	// PerHostDelayMs > 0 appends an extra label.
	r.PerHostDelayMs = 250
	out2 := r.formatHuman()
	if !strings.Contains(out2, "per-host-delay=250ms") {
		t.Errorf("expected per-host-delay annotation: %q", out2)
	}
}

func TestFormatHumanUnknownMode(t *testing.T) {
	r := &benchResult{Mode: "weird-mode", N: 1}
	out := r.formatHuman()
	if !strings.Contains(out, "weird-mode") {
		t.Errorf("expected fallback to include struct dump: %q", out)
	}
}

func TestUnion(t *testing.T) {
	a := map[string]*benchResult{"vector": {}, "extra-a": {}}
	b := map[string]*benchResult{"bm25": {}, "vector": {}, "extra-b": {}}
	out := union(a, b)
	seen := map[string]bool{}
	for _, k := range out {
		seen[k] = true
	}
	for _, k := range []string{"vector", "bm25", "extra-a", "extra-b"} {
		if !seen[k] {
			t.Errorf("missing %s in union: %v", k, out)
		}
	}
}

func TestNeutralVocabForDistractors(t *testing.T) {
	v := neutralVocabForDistractors()
	if len(v) < 30 {
		t.Errorf("vocab too short: %d words", len(v))
	}
	// All entries non-empty.
	for i, w := range v {
		if w == "" {
			t.Errorf("entry %d is empty", i)
		}
	}
}

func TestGenerateDistractorText(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	vocab := []string{"a", "b", "c"}
	got := generateDistractorText(rng, vocab, 5)
	parts := strings.Fields(got)
	if len(parts) != 5 {
		t.Errorf("got %d words want 5", len(parts))
	}
	allowed := map[string]bool{"a": true, "b": true, "c": true}
	for _, p := range parts {
		if !allowed[p] {
			t.Errorf("unexpected word %q", p)
		}
	}
}
