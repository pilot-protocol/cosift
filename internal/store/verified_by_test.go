package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"
	"time"
)

func TestPebbleKeepsVerifiedBy(t *testing.T) {
	ctx := context.Background()
	p, err := OpenPebble(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for url, by := range map[string]string{"https://a.example/server": "server", "https://a.example/quorum": "quorum", "https://a.example/crawled": ""} {
		if _, err := p.UpsertDocument(ctx, &Document{URL: url, Title: "t", Text: "text", Source: "attested", VerifiedBy: by, FetchedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		got, err := p.GetDocByURL(ctx, url)
		if err != nil || got.VerifiedBy != by {
			t.Errorf("%s: %q %v, want %q", url, got.VerifiedBy, err, by)
		}
	}
	if _, err := p.WriteCrawlResult(ctx, &Document{URL: "https://a.example/batch", Title: "t", Text: "text", VerifiedBy: "server", FetchedAt: time.Now()}, "t", "text", "", func(s string) []string { return []string{s} }, 1); err != nil {
		t.Fatal(err)
	}
	if got, _ := p.GetDocByURL(ctx, "https://a.example/batch"); got == nil || got.VerifiedBy != "server" {
		t.Fatalf("WriteCrawlResult dropped verified_by: %+v", got)
	}
}

func TestPebbleReadsADocumentStoredBeforeVerifiedBy(t *testing.T) {
	p, err := OpenPebble(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	legacy := struct {
		ID         int64
		URL        string
		Title      string
		Text       string
		Source     string
		FetchedAt  time.Time
		ContentSHA []byte
	}{ID: 7, URL: "https://old.example/", Title: "old", Text: "old text", Source: "crawl", FetchedAt: time.Now()}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(legacy); err != nil {
		t.Fatal(err)
	}
	idBuf := []byte{0, 0, 0, 0, 0, 0, 0, 7}
	if err := p.db.Set(docKey(7), buf.Bytes(), nil); err != nil {
		t.Fatal(err)
	}
	if err := p.db.Set(urlKey(legacy.URL), idBuf, nil); err != nil {
		t.Fatal(err)
	}
	got, err := p.GetDocByURL(context.Background(), legacy.URL)
	if err != nil || got.Title != "old" || got.VerifiedBy != "" {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestSQLiteKeepsVerifiedByAndMigratesIt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertDocument(ctx, &Document{URL: "https://a.example/x", Title: "t", Text: "text", Source: "attested", VerifiedBy: "quorum", FetchedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetDocByURL(ctx, "https://a.example/x"); err != nil || got.VerifiedBy != "quorum" {
		t.Fatalf("%+v %v", got, err)
	}
	if docs, err := s.ListDocuments(ctx, 0); err != nil || len(docs) != 1 || docs[0].VerifiedBy != "quorum" {
		t.Fatalf("%+v %v", docs, err)
	}
	if _, err := s.UpsertDocument(ctx, &Document{URL: "https://a.example/x", Title: "t", Text: "text", Source: "attested", VerifiedBy: "server", FetchedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetDocByURL(ctx, "https://a.example/x"); err != nil || got.VerifiedBy != "server" {
		t.Fatalf("an upsert over the row must update it: %+v %v", got, err)
	}
	if _, err := s.DB().ExecContext(ctx, `ALTER TABLE documents DROP COLUMN verified_by;`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen on a database without the column: %v", err)
	}
	defer s.Close()
	got, err := s.GetDocByURL(ctx, "https://a.example/x")
	if err != nil || got.Title != "t" || got.VerifiedBy != "" {
		t.Fatalf("%+v %v", got, err)
	}
}
