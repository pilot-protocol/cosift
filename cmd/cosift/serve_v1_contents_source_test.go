package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

func TestV1ContentsReportsSourceAndVerifier(t *testing.T) {
	f := newV1Fixture(t)
	ctx := context.Background()
	for _, d := range []store.Document{
		{URL: "https://p.example/server", Source: "attested", VerifiedBy: "server"},
		{URL: "https://p.example/quorum", Source: "attested", VerifiedBy: "quorum"},
		{URL: "https://p.example/crawl", Source: "crawl"},
		{URL: "https://p.example/import", Source: "fineweb"},
		{URL: "https://p.example/legacy"},
		{URL: "https://p.example/stray", Source: "crawl", VerifiedBy: "server"},
	} {
		d.Title, d.Text, d.FetchedAt = "T", "body text of the page", time.Now()
		if _, err := f.ps.UpsertDocument(ctx, &d); err != nil {
			t.Fatal(err)
		}
	}
	w := f.v1Call(f.srv.handleV1Contents, `{"urls":["https://p.example/server","https://p.example/quorum","https://p.example/crawl","https://p.example/import","https://p.example/legacy","https://p.example/stray","https://p.example/none"]}`)
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var out struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || len(out.Results) != 7 {
		t.Fatalf("%v %s", err, w.Body)
	}
	for i, want := range []struct{ source, by string }{{"attested", "server"}, {"attested", "quorum"}, {"crawl", ""}, {"crawl", ""}, {"crawl", ""}, {"crawl", ""}} {
		r := out.Results[i]
		by, hasBy := r["verified_by"]
		if r["source"] != want.source || (want.by == "") == hasBy || (hasBy && by != want.by) {
			t.Errorf("%v: source %v verified_by %v", r["url"], r["source"], by)
		}
	}
	if _, ok := out.Results[6]["source"]; ok {
		t.Errorf("a missing url carries a source: %v", out.Results[6])
	}
}
