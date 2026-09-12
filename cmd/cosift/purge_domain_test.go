package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// TestRunPurgeDomain verifies the host-suffix sweep soft-deletes exactly the
// matching-TLD docs (dot-boundary) and leaves the rest, and that the default
// dry run deletes nothing.
func TestRunPurgeDomain(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	idx := index.NewPebbleBM25(ps)

	docs := []struct{ url, title, text string }{
		{"https://spam1.cfd/a", "Spam A", "junk body one"},
		{"https://x.spam.cfd/b", "Spam B", "junk body two"},  // subdomain of a .cfd host
		{"https://gamble.sbs/c", "Bet C", "junk body three"}, // .sbs
		{"https://good.com/d", "Good D", "real useful content"},
		{"https://docs.example.org/e", "Docs E", "reference material"},
		{"https://notcfd.com/f", "Edge F", "ends in cfd-ish but is .com"}, // must NOT match
	}
	for _, d := range docs {
		id, err := ps.UpsertDocument(ctx, &store.Document{URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now()})
		if err != nil {
			t.Fatalf("UpsertDocument %s: %v", d.url, err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("IndexDocument %s: %v", d.url, err)
		}
	}
	ps.Close() // release the write lock so runPurgeDomain can open the dir

	// Dry run: must delete nothing.
	if err := runPurgeDomain(ctx, []string{"-dir", dir, "-suffix", "cfd,sbs"}); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	assertDocs(t, dir, map[string]bool{
		"https://spam1.cfd/a": true, "https://gamble.sbs/c": true, "https://good.com/d": true,
	})

	// Apply with a keep-list: the kept host survives even though it matches -suffix/-blocklist.
	bl := writeSuffixFile(t, "# spam TLDs\n\n  SBS  \n")
	if err := runPurgeDomain(ctx, []string{"-dir", dir, "-suffix", "cfd", "-blocklist", bl, "-keep", "spam.cfd", "-apply"}); err != nil {
		t.Fatalf("apply with keep: %v", err)
	}
	assertDocs(t, dir, map[string]bool{
		"https://spam1.cfd/a":  false,
		"https://x.spam.cfd/b": true, // kept (subdomain of the keep entry)
		"https://gamble.sbs/c": false,
	})

	// Apply: purge *.cfd and *.sbs.
	if err := runPurgeDomain(ctx, []string{"-dir", dir, "-suffix", "cfd,sbs", "-apply"}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	assertDocs(t, dir, map[string]bool{
		"https://spam1.cfd/a":        false, // purged
		"https://x.spam.cfd/b":       false, // purged (subdomain)
		"https://gamble.sbs/c":       false, // purged
		"https://good.com/d":         true,  // kept
		"https://docs.example.org/e": true,  // kept
		"https://notcfd.com/f":       true,  // kept (dot-boundary: not a .cfd)
	})
}

// assertDocs opens the store read-only and checks presence/absence per URL.
func assertDocs(t *testing.T, dir string, want map[string]bool) {
	t.Helper()
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ps.Close()
	for u, shouldExist := range want {
		d, _ := ps.GetDocByURL(context.Background(), u)
		if shouldExist && d == nil {
			t.Errorf("%s: expected present, got deleted", u)
		}
		if !shouldExist && d != nil {
			t.Errorf("%s: expected purged, still present", u)
		}
	}
}

func TestRunPurgeDomainRequiresSuffix(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	ps.Close()
	if err := runPurgeDomain(context.Background(), []string{"-dir", dir}); err == nil {
		t.Error("expected error when -suffix is empty")
	}
	empty := writeSuffixFile(t, "# only comments\n\n")
	if err := runPurgeDomain(context.Background(), []string{"-dir", dir, "-blocklist", empty}); err == nil {
		t.Error("expected error when -suffix and -blocklist are both empty")
	}
	if err := runPurgeDomain(context.Background(), []string{"-dir", dir, "-blocklist", filepath.Join(dir, "missing.txt")}); err == nil {
		t.Error("expected error when -blocklist file is missing")
	}
	if err := runPurgeDomain(context.Background(), []string{"-dir", dir, "-suffix", "cfd", "-apply", "-readonly"}); err == nil {
		t.Error("expected error when -apply combined with -readonly")
	}
}

func writeSuffixFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "blocklist.txt")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadSuffixFile(t *testing.T) {
	p := writeSuffixFile(t, "# header comment\n\n  cfd  \nSpam.SBS\n\t\nbbc.co.uk # inline\n   # indented comment\nXYZ\n")
	got, err := loadSuffixFile(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"cfd", "spam.sbs", "bbc.co.uk", "xyz"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if _, err := loadSuffixFile(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Error("expected error for a missing file")
	}
}

func TestSuffixMatcherBucketedEqualsLinear(t *testing.T) {
	suffixes := []string{"cfd", "sbs", "xyz", "bbc.co.uk", "spam.cfd", "localhost", "trail.cfd."}
	keep := []string{"news.bbc.co.uk", "good.spam.cfd", "cfd"}
	for i := 0; i < 600; i++ {
		suffixes = append(suffixes, fmt.Sprintf("junk%d.example%d.tld%d", i, i%7, i%13))
		keep = append(keep, fmt.Sprintf("keep%d.junk%d.example%d.tld%d", i, i, i%7, i%13))
	}
	if len(suffixes) <= suffixBucketThreshold || len(keep) <= suffixBucketThreshold {
		t.Fatalf("fixture must exceed %d entries", suffixBucketThreshold)
	}
	bm, km := newSuffixMatcher(suffixes), newSuffixMatcher(keep)
	if bm.buckets == nil || km.buckets == nil {
		t.Fatal("expected bucketed matchers")
	}
	if small := newSuffixMatcher(suffixes[:3]); small.buckets != nil {
		t.Error("small list must stay linear")
	}

	hosts := []string{
		"x.cfd", "X.CFD", "cfd", "notcfd.com", "a.b.c.sbs", "sbs.com", "xyz",
		"bbc.co.uk", "news.bbc.co.uk", "www.news.bbc.co.uk", "bbc.co.uk.evil.com",
		"spam.cfd", "good.spam.cfd", "deep.good.spam.cfd", "badspam.cfd",
		"localhost", "host.localhost", "trail.cfd.", "x.trail.cfd.", "",
		"junk5.example5.tld5", "sub.junk5.example5.tld5", "keep5.junk5.example5.tld5",
		"junk5.example5.tld6", "example5.tld5", "tld5", "ajunk5.example5.tld5",
	}
	for i := 0; i < 600; i += 37 {
		hosts = append(hosts, fmt.Sprintf("junk%d.example%d.tld%d", i, i%7, i%13), fmt.Sprintf("keep%d.junk%d.example%d.tld%d", i, i, i%7, i%13))
	}
	var hits, kept int
	for _, h := range hosts {
		wantMatch, wantKeep := matchesAnyDomain(h, suffixes), matchesAnyDomain(h, keep)
		if got := bm.matches(h); got != wantMatch {
			t.Errorf("suffix %q: bucketed=%v linear=%v", h, got, wantMatch)
		}
		if got := km.matches(h); got != wantKeep {
			t.Errorf("keep %q: bucketed=%v linear=%v", h, got, wantKeep)
		}
		if wantMatch {
			hits++
		}
		if wantMatch && wantKeep {
			kept++
		}
	}
	if hits == 0 || kept == 0 || hits == len(hosts) {
		t.Fatalf("fixture not mixed: hits=%d kept=%d of %d", hits, kept, len(hosts))
	}
	if !bm.matches("x.cfd") || bm.matches("notcfd.com") {
		t.Error("bare TLD entry must dot-boundary match")
	}
}
