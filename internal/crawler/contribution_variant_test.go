package crawler

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

func TestContributionVariantOfWETImportedPageIsNotNovel(t *testing.T) {
	page := pagePlain("shared article text about raft consensus")
	wetBody := []byte("Page\nNavigation Home About\nshared article text about raft consensus\nFooter links\n")
	for name, tc := range map[string]struct {
		url  string
		html string
		want bool
	}{
		"query variant":      {"https://news.example/guide?ref=1", page, false},
		"trailing slash":     {"https://news.example/guide/", page, false},
		"other scheme":       {"http://news.example/guide", page, false},
		"www host":           {"https://www.news.example/guide", page, false},
		"default https port": {"https://news.example:443/guide", page, false},
		"default http port":  {"http://news.example:80/guide", page, false},
		"port www query":     {"https://www.news.example:443/guide?ref=1", page, false},
		"trailing dot host":  {"https://news.example.:443/guide", page, false},
		"dot segment":        {"https://news.example/./guide", page, false},
		"parent segment":     {"https://news.example/x/../guide", page, false},
		"other port":         {"https://news.example:8443/guide", pagePlain("same path on another port"), true},
		"sibling path":       {"https://news.example/guide-two", pagePlain("another article entirely, about gossip protocols"), true},
		"sub path":           {"https://news.example/guide/part", pagePlain("a sub page about leader election"), true},
		"other host":         {"https://other.example/guide", pagePlain("same path, another site"), true},
		"unrelated new page": {"https://news.example/fresh", pagePlain("a brand new article"), true},
	} {
		t.Run(name, func(t *testing.T) {
			route, _, _ := strings.Cut(tc.url, "?")
			c, ps, _ := pebbleContributor(t, map[string]string{route: tc.html})
			if err := c.indexWetRecord(context.Background(), &WetRecord{URL: "https://news.example/guide", Body: wetBody}, true); err != nil {
				t.Fatal(err)
			}
			completeContentIndex(t, ps)
			if r := contribute(t, c, tc.url, tc.html); !r.Indexed || r.Novel != tc.want {
				t.Fatalf("novel = %v, want %v: %+v", r.Novel, tc.want, r)
			}
		})
	}
}

func TestContributionOfDifferentContentBesideACrawledQueryVariantStaysNovel(t *testing.T) {
	c, ps, _ := pebbleContributor(t, map[string]string{"https://forum.example/view": pagePlain("thread two about restores")})
	text := "thread one about backups"
	sum := sha256.Sum256([]byte(text))
	if _, err := ps.UpsertDocument(context.Background(), &store.Document{URL: "https://forum.example/view?t=1", Domain: "forum.example", Title: "Page", Text: text, Source: "crawl", FetchedAt: time.Now(), ContentSHA: sum[:]}); err != nil {
		t.Fatal(err)
	}
	completeContentIndex(t, ps)
	if r := contribute(t, c, "https://forum.example/view?t=2", pagePlain("thread two about restores")); !r.Novel {
		t.Fatalf("new content beside a crawled query variant must be novel: %+v", r)
	}
}
