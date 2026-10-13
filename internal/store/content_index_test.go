package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func shaOf(text string) []byte {
	sum := sha256.Sum256([]byte(text))
	return sum[:]
}

func contentDoc(url, text string) *Document {
	return &Document{URL: url, Domain: "example.com", Title: url, Text: text, FetchedAt: time.Now(), ContentSHA: shaOf(text)}
}

func mustLive(t *testing.T, p *PebbleStore, text string, exclude int64, want bool) {
	t.Helper()
	got, err := p.HasLiveContent(context.Background(), shaOf(text), exclude)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("HasLiveContent(%q, exclude=%d) = %v, want %v", text, exclude, got, want)
	}
}

func TestContentIndexCoversEveryDocumentWritePath(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	if _, err := p.UpsertDocument(ctx, contentDoc("https://example.com/upsert", "text via upsert")); err != nil {
		t.Fatal(err)
	}
	crawl := contentDoc("https://example.com/crawl", "text via crawl result")
	if _, err := p.WriteCrawlResult(ctx, crawl, crawl.Title, crawl.Text, "", func(s string) []string { return strings.Fields(s) }, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.UpsertDocumentBatch(ctx, []*Document{contentDoc("https://example.com/batch", "text via batch")}); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"text via upsert", "text via crawl result", "text via batch"} {
		mustLive(t, p, text, 0, true)
	}
	mustLive(t, p, "text nobody wrote", 0, false)
}

func TestContentIndexIsValidatedOnRead(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	id, err := p.UpsertDocument(ctx, contentDoc("https://example.com/a", "original"))
	if err != nil {
		t.Fatal(err)
	}
	mustLive(t, p, "original", 0, true)
	mustLive(t, p, "original", id, false)

	if _, err := p.UpsertDocument(ctx, contentDoc("https://example.com/a", "rewritten")); err != nil {
		t.Fatal(err)
	}
	mustLive(t, p, "original", 0, false)
	mustLive(t, p, "rewritten", 0, true)

	if ok, err := p.SoftDeleteDocument(ctx, id, "https://example.com/a"); err != nil || !ok {
		t.Fatalf("soft delete: %v %v", ok, err)
	}
	mustLive(t, p, "rewritten", 0, false)
}

func TestContentIndexSkipsStaleEntriesButFindsLiveOne(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	gone, _ := p.UpsertDocument(ctx, contentDoc("https://example.com/gone", "shared text"))
	if _, err := p.UpsertDocument(ctx, contentDoc("https://example.com/kept", "shared text")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SoftDeleteDocument(ctx, gone, "https://example.com/gone"); err != nil {
		t.Fatal(err)
	}
	mustLive(t, p, "shared text", 0, true)
}

func TestContentIndexIgnoresMalformedHashes(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	d := contentDoc("https://example.com/short", "short hash doc")
	d.ContentSHA = []byte("too short")
	if _, err := p.UpsertDocument(ctx, d); err != nil {
		t.Fatal(err)
	}
	if got, err := p.HasLiveContent(ctx, d.ContentSHA, 0); err != nil || got {
		t.Fatalf("malformed hash reported live: %v %v", got, err)
	}
	it, _ := p.db.NewIter(&pebble.IterOptions{LowerBound: []byte{famContentSHA}, UpperBound: []byte{famContentSHA + 1}})
	defer it.Close()
	if it.First() {
		t.Fatal("malformed hash was indexed")
	}
}

func TestContentIndexKeyLayout(t *testing.T) {
	sha := shaOf("layout")
	k := contentSHAKey(sha, 7)
	if k[0] != 'C' || !bytes.Equal(k[1:33], sha) || len(k) != 41 {
		t.Fatalf("unexpected key layout %x", k)
	}
}
