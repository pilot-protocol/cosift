package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

func TestNormalizeURLKey(t *testing.T) {
	cases := []struct{ in, key, host string }{
		{"https://example.com/a", "https://example.com/a", "example.com"},
		{"http://example.com/a", "https://example.com/a", "example.com"},
		{"HTTPS://WWW.Example.COM/a", "https://example.com/a", "example.com"},
		{"https://example.com:443/a", "https://example.com/a", "example.com"},
		{"http://example.com:80/a", "https://example.com/a", "example.com"},
		{"https://example.com:8443/a", "https://example.com:8443/a", "example.com"},
		{"https://example.com/a#section", "https://example.com/a", "example.com"},
		{"https://example.com/a/", "https://example.com/a", "example.com"},
		{"https://example.com/", "https://example.com/", "example.com"},
		{"https://example.com", "https://example.com/", "example.com"},
		{"https://example.com/a?utm_source=x&utm_medium=y", "https://example.com/a", "example.com"},
		{"https://example.com/a?ref=t&fbclid=1&gclid=2&mc_cid=3&mc_eid=4&_ga=5&ref_src=6", "https://example.com/a", "example.com"},
		{"https://example.com/a?b=2&a=1", "https://example.com/a?a=1&b=2", "example.com"},
		{"https://example.com/a?a=1&utm_campaign=z&b=2", "https://example.com/a?a=1&b=2", "example.com"},
		{"https://example.com/a?refresh=1", "https://example.com/a?refresh=1", "example.com"},
		{"ftp://example.com/a", "ftp://example.com/a", "example.com"},
	}
	for _, c := range cases {
		key, host, ok := normalizeURLKey(c.in)
		if !ok {
			t.Errorf("%q: not ok", c.in)
			continue
		}
		if key != c.key || host != c.host {
			t.Errorf("%q: got (%q, %q), want (%q, %q)", c.in, key, host, c.key, c.host)
		}
	}
	for _, bad := range []string{"", "not a url", "/relative/path", "://x"} {
		if _, _, ok := normalizeURLKey(bad); ok {
			t.Errorf("%q: expected not ok", bad)
		}
	}
}

func TestNormalizeLang(t *testing.T) {
	for in, want := range map[string]string{"": "", "en": "en", "EN-US": "en", "pt_BR": "pt", " de ": "de"} {
		if got := normalizeLang(in); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}

func TestNonEnglishFootprint(t *testing.T) {
	hosts := map[string]int64{"a.com": 5, "b.co.jp": 3, "c.de": 2, "d.org": 4, "127.0.0.1": 1, "e.co": 1}
	docs := map[string]int64{}
	for h, n := range hosts {
		tld := tldOfHost(h)
		if tld == "" {
			tld = "(none)"
		}
		docs[tld] += n
	}
	if docs["(none)"] != 0 || docs["1"] != 1 {
		t.Fatalf("ip host bucket: %v", docs)
	}
	ne := nonEnglishFootprint(docs, docs, 16)
	if ne.Docs != 6 || ne.Tokens != 6 {
		t.Fatalf("non-english docs=%d tokens=%d, want 6/6 (jp=3 de=2 co=1)", ne.Docs, ne.Tokens)
	}
	if ne.DocShare != 6.0/16.0 {
		t.Fatalf("doc_share=%v", ne.DocShare)
	}
	if len(ne.TLDs) != len(nonEnglishTLDs) {
		t.Fatalf("tld rows=%d want %d", len(ne.TLDs), len(nonEnglishTLDs))
	}
	got := map[string]int64{}
	for _, b := range ne.TLDs {
		got[b.Key] = b.Docs
	}
	if got["jp"] != 3 || got["de"] != 2 || got["co"] != 1 || got["fr"] != 0 {
		t.Fatalf("per-tld: %v", got)
	}
}

// TestRunCensus builds a ~30-doc store across TLDs/hosts/duplicates and checks
// the JSON report numbers.
func TestRunCensus(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	idx := index.NewPebbleBM25(ps)

	type doc struct{ url, lang, text string }
	var docs []doc
	// 10 distinct .com docs (host a.com), 3 of which have duplicates below.
	for i := 0; i < 10; i++ {
		docs = append(docs, doc{"https://a.com/p" + string(rune('0'+i)), "en", "alpha beta gamma"})
	}
	docs = append(docs,
		doc{"http://www.a.com/p0", "en", "alpha beta gamma"},            // dup of p0 (scheme + www)
		doc{"https://a.com/p0/?utm_source=x", "en", "alpha beta gamma"}, // dup of p0 (slash + utm)
		doc{"https://a.com/p1#frag", "en-GB", "alpha beta"},             // dup of p1
		doc{"https://a.com/p2?fbclid=1", "", "alpha"},                   // dup of p2
	)
	// 5 .de docs on two hosts, one duplicate pair.
	for i := 0; i < 5; i++ {
		host := "x.de"
		if i >= 3 {
			host = "y.de"
		}
		docs = append(docs, doc{"https://" + host + "/s" + string(rune('0'+i)), "de", "ein zwei drei vier"})
	}
	docs = append(docs, doc{"https://x.de:443/s0", "de", "ein zwei drei vier"}) // dup of x.de/s0
	// 4 .jp, 2 .org, 2 .co.uk, 1 IP host, 1 .fr.
	for i := 0; i < 4; i++ {
		docs = append(docs, doc{"https://j.co.jp/" + string(rune('a'+i)), "ja", "one two"})
	}
	for i := 0; i < 2; i++ {
		docs = append(docs, doc{"https://o.org/" + string(rune('a'+i)), "en", "one two three"})
	}
	docs = append(docs,
		doc{"https://u.co.uk/a", "en", "one"},
		doc{"https://u.co.uk/b", "", "one"},
		doc{"http://10.0.0.1/index", "", "one"},
		doc{"https://f.fr/a", "fr", "un deux trois"},
	)
	if len(docs) != 30 {
		t.Fatalf("test corpus has %d docs, want 30", len(docs))
	}
	for _, d := range docs {
		id, err := ps.UpsertDocument(ctx, &store.Document{URL: d.url, Title: "t", Text: d.text, Lang: d.lang, FetchedAt: time.Now()})
		if err != nil {
			t.Fatalf("UpsertDocument %s: %v", d.url, err)
		}
		if err := idx.IndexDocument(ctx, id, "t", d.text); err != nil {
			t.Fatalf("IndexDocument %s: %v", d.url, err)
		}
	}
	sumLen, _, _ := ps.SumDocLengths(ctx)
	ps.Close()

	out := filepath.Join(t.TempDir(), "census.json")
	if err := runCensus(ctx, []string{"-dir", dir, "-out", out, "-dups", "-lang", "-top", "3", "-progress", "7"}); err != nil {
		t.Fatalf("runCensus: %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var rep censusReport
	if err := json.Unmarshal(data, &rep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if rep.Docs != 30 {
		t.Errorf("docs=%d want 30", rep.Docs)
	}
	if rep.Tokens != sumLen || rep.Tokens == 0 {
		t.Errorf("tokens=%d want %d", rep.Tokens, sumLen)
	}
	if rep.TextBytes == 0 || rep.DocsPerSec <= 0 {
		t.Errorf("text_bytes=%d docs/s=%v", rep.TextBytes, rep.DocsPerSec)
	}
	if rep.DistinctTLDs != 7 { // com de jp org uk fr (none)→"1"
		t.Errorf("distinct_tlds=%d want 7", rep.DistinctTLDs)
	}
	if len(rep.TopTLDs) != 3 || rep.TopTLDs[0].Key != "com" || rep.TopTLDs[0].Docs != 14 ||
		rep.TopTLDs[1].Key != "de" || rep.TopTLDs[1].Docs != 6 || rep.TopTLDs[2].Key != "jp" || rep.TopTLDs[2].Docs != 4 {
		t.Errorf("top_tlds=%+v", rep.TopTLDs)
	}
	if rep.TopTLDs[0].Tokens == 0 {
		t.Errorf("top tld tokens should be non-zero: %+v", rep.TopTLDs[0])
	}
	if rep.NonEnglish.Docs != 11 { // de 6 + jp 4 + fr 1
		t.Errorf("non_english.docs=%d want 11", rep.NonEnglish.Docs)
	}
	if rep.NonEnglish.DocShare != 11.0/30.0 {
		t.Errorf("non_english.doc_share=%v", rep.NonEnglish.DocShare)
	}

	if rep.Lang == nil {
		t.Fatal("lang block missing")
	}
	if rep.Lang.English != 16 || rep.Lang.NonEnglish != 11 || rep.Lang.Unknown != 3 || rep.Lang.Distinct != 4 {
		t.Errorf("lang=%+v", *rep.Lang)
	}
	if len(rep.Lang.Top) != 3 || rep.Lang.Top[0].Key != "en" || rep.Lang.Top[0].Docs != 16 {
		t.Errorf("lang.top=%+v", rep.Lang.Top)
	}

	if rep.Dups == nil {
		t.Fatal("dups block missing")
	}
	if rep.Dups.Groups != 4 || rep.Dups.DocsInGroups != 9 || rep.Dups.Removable != 5 || rep.Dups.Unparseable != 0 {
		t.Errorf("dups=%+v", *rep.Dups)
	}
	if len(rep.Dups.TopHosts) != 2 || rep.Dups.TopHosts[0].Key != "a.com" || rep.Dups.TopHosts[0].Docs != 4 ||
		rep.Dups.TopHosts[1].Key != "x.de" || rep.Dups.TopHosts[1].Docs != 1 {
		t.Errorf("dups.top_hosts=%+v", rep.Dups.TopHosts)
	}
	if len(rep.Dups.Samples) != 4 {
		t.Fatalf("samples=%+v", rep.Dups.Samples)
	}
	var p0 *censusDupGroup
	for i := range rep.Dups.Samples {
		if rep.Dups.Samples[i].Count == 3 {
			p0 = &rep.Dups.Samples[i]
		}
	}
	if p0 == nil || len(p0.URLs) != 3 {
		t.Errorf("p0 group=%+v", p0)
	}

	// -limit caps the scan; -lang/-dups off drop their blocks.
	out2 := filepath.Join(t.TempDir(), "census2.json")
	if err := runCensus(ctx, []string{"-dir", dir, "-out", out2, "-limit", "5"}); err != nil {
		t.Fatalf("runCensus limit: %v", err)
	}
	data, _ = os.ReadFile(out2)
	var rep2 censusReport
	if err := json.Unmarshal(data, &rep2); err != nil {
		t.Fatalf("unmarshal 2: %v", err)
	}
	if rep2.Docs != 5 || rep2.Lang != nil || rep2.Dups != nil {
		t.Errorf("limited report: docs=%d lang=%v dups=%v", rep2.Docs, rep2.Lang != nil, rep2.Dups != nil)
	}
}
