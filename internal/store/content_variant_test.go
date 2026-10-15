package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
)

func variantDoc(url, source string) *Document {
	d := contentDoc(url, "text of "+url)
	d.Source = source
	return d
}

func TestHasUncomparableVariant(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	ids := map[string]int64{}
	for url, source := range map[string]string{
		"https://news.example/guide":              "wet-import",
		"http://www.blog.example/post/":           "wet-import",
		"https://shop.example/item?id=7":          "wet-import",
		"https://crawled.example/page":            "crawl",
		"https://crawled.example/page?ref=1":      "crawl",
		"https://news.example/guide-two":          "wet-import",
		"https://news.example/guide/child":        "wet-import",
		"https://other.example/solo?x=1":          "wet-import",
		"https://corpus.example/doc":              "",
		"https://news.example/archive?page=2":     "wet-import",
		"https://news.example/archive/extra?page": "wet-import",
		"https://ported.example:443/page":         "wet-import",
		"http://plain.example:80/page":            "wet-import",
		"https://[2001:db8::1]/page":              "wet-import",
	} {
		id, err := p.UpsertDocument(ctx, variantDoc(url, source))
		if err != nil {
			t.Fatal(err)
		}
		ids[url] = id
	}
	for _, tc := range []struct {
		url     string
		exclude int64
		want    bool
	}{
		{"https://news.example/guide?ref=1", 0, true},
		{"https://news.example/guide/", 0, true},
		{"http://news.example/guide", 0, true},
		{"https://www.news.example/guide?a=1&b=2", 0, true},
		{"https://blog.example/post", 0, true},
		{"https://blog.example/post?utm=x", 0, true},
		{"https://shop.example/item?id=8", 0, true},
		{"https://shop.example/item", 0, true},
		{"https://corpus.example/doc?ref=1", 0, true},
		{"https://news.example/archive", 0, true},
		{"https://news.example:443/guide", 0, true},
		{"http://news.example:80/guide", 0, true},
		{"https://www.news.example:443/guide?ref=1", 0, true},
		{"https://News.Example./guide", 0, true},
		{"https://news.example.:443/guide/", 0, true},
		{"https://news.example/./guide", 0, true},
		{"https://news.example/x/../guide", 0, true},
		{"https://news.example/../guide", 0, true},
		{"https://news.example/guide/.", 0, true},
		{"https://news.example/x/%2e%2E/guide", 0, true},
		{"https://news.example/%2E/guide", 0, true},
		{"https://news.example/x/.%2e/guide", 0, true},
		{"https://news.example/guide/./child", 0, true},
		{"https://news.example/a/b/../../guide?x=1", 0, true},
		{"https://ported.example/page?x=1", 0, true},
		{"http://plain.example/page", 0, true},
		{"https://[2001:db8::1]:443/page?x=1", 0, true},
		{"https://[2001:db8::2]:443/page", 0, false},
		{"https://news.example:8443/guide", 0, false},
		{"https://news.example:80/guide", 0, false},
		{"https://news.example/gui/../other", 0, false},
		{"https://news.example/guide/..", 0, false},
		{"https://news.example/./guide-three", 0, false},
		{"https://crawled.example:443/page?ref=3", 0, false},
		{"https://crawled.example/x/../page/", 0, false},
		{"https://news.example/guide-three", 0, false},
		{"https://news.example/gui", 0, false},
		{"https://news.example/other", 0, false},
		{"https://elsewhere.example/guide", 0, false},
		{"https://news.example/", 0, false},
		{"https://crawled.example/page?ref=2", 0, false},
		{"https://crawled.example/page/", 0, false},
		{"https://other.example/solo?x=1", ids["https://other.example/solo?x=1"], false},
		{"https://other.example/solo?x=2", ids["https://other.example/solo?x=1"], false},
		{"https://other.example/solo?x=2", 0, true},
	} {
		got, err := p.HasUncomparableVariant(ctx, tc.url, tc.exclude)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("HasUncomparableVariant(%s, exclude=%d) = %v, want %v", tc.url, tc.exclude, got, tc.want)
		}
	}
}

func TestHasUncomparableVariantIgnoresDeletedDocuments(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	id, err := p.UpsertDocument(ctx, variantDoc("https://news.example/guide", "wet-import"))
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := p.SoftDeleteDocument(ctx, id, "https://news.example/guide"); err != nil || !ok {
		t.Fatalf("soft delete: %v %v", ok, err)
	}
	if got, err := p.HasUncomparableVariant(ctx, "https://news.example/guide?ref=1", 0); err != nil || got {
		t.Fatalf("a deleted document still blocks: %v %v", got, err)
	}
}

func TestHasUncomparableVariantSkipsURLKeysWithoutADocument(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	id, err := p.UpsertDocument(ctx, variantDoc("https://news.example/guide", "wet-import"))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.db.Delete(docKey(id), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if got, err := p.HasUncomparableVariant(ctx, "https://news.example/guide?ref=1", 0); err != nil || got {
		t.Fatalf("a URL key without a document still blocks: %v %v", got, err)
	}
}

func TestHasUncomparableVariantFailsClosedOnLargeFamilies(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	for i := 0; i <= variantScanCap; i++ {
		if _, err := p.UpsertDocument(ctx, variantDoc(fmt.Sprintf("https://forum.example/view?t=%d", i), "crawl")); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := p.HasUncomparableVariant(ctx, "https://forum.example/view?t=99999", 0); err != nil || !got {
		t.Fatalf("a family too large to examine must not read as clear: %v %v", got, err)
	}
}
