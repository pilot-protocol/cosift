package store

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestHasUncomparableVariantRejectsAnUnparseableURL(t *testing.T) {
	p := newPebbleStore(t)
	if got, err := p.HasUncomparableVariant(context.Background(), "http://[::1", 0); err == nil || got {
		t.Fatalf("unparseable URL: %v %v", got, err)
	}
}

func TestHasUncomparableVariantSurfacesAMalformedURLIndexValue(t *testing.T) {
	p := newPebbleStore(t)
	if err := p.db.Set(urlKey("https://news.example/guide"), []byte{1, 2, 3}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	got, err := p.HasUncomparableVariant(context.Background(), "https://news.example/guide?ref=1", 0)
	if err == nil || !strings.Contains(err.Error(), "malformed URL") || got {
		t.Fatalf("malformed index value: %v %v", got, err)
	}
}

func TestHasUncomparableVariantSurfacesACorruptDocument(t *testing.T) {
	p := newPebbleStore(t)
	id, err := p.UpsertDocument(context.Background(), variantDoc("https://news.example/guide", "wet-import"))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.db.Set(docKey(id), []byte("not a document"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	got, err := p.HasUncomparableVariant(context.Background(), "https://news.example/guide?ref=1", 0)
	if err == nil || !strings.Contains(err.Error(), "decode doc") || got {
		t.Fatalf("corrupt document: %v %v", got, err)
	}
}

func TestHasUncomparableVariantSkipsMalformedPrefixEntries(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	if err := p.db.Set(urlKey("https://news.example/guide?broken"), []byte{1}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if got, err := p.HasUncomparableVariant(ctx, "https://news.example/guide", 0); err != nil || got {
		t.Fatalf("malformed prefix entry: %v %v", got, err)
	}
	if _, err := p.UpsertDocument(ctx, variantDoc("https://news.example/guide?real", "wet-import")); err != nil {
		t.Fatal(err)
	}
	if got, err := p.HasUncomparableVariant(ctx, "https://news.example/guide", 0); err != nil || !got {
		t.Fatalf("a real entry behind a malformed one was missed: %v %v", got, err)
	}
}
