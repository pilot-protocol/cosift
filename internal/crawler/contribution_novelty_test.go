package crawler

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

func pagePlain(text string) string {
	return "<!doctype html><html><head><title>Page</title></head><body><main><p>" + text + "</p></main></body></html>"
}

func pebbleContributor(t *testing.T, routes map[string]string) (*Crawler, *store.PebbleStore, *siteRT) {
	t.Helper()
	ps, err := store.OpenPebble(filepath.Join(t.TempDir(), "pebble"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ps.Close() })
	cfg := testCrawlerCfg()
	cfg.RespectRobots = false
	cfg.PerHostDelayMs = 0
	cfg.DisableLinkFollowing = true
	c := NewWithBackend(cfg, ps, index.NewPebbleBM25(ps))
	rt := &siteRT{hits: map[string]int{}, routes: routes}
	c.http.Transport = rt
	c.contributionOnce.Do(func() { c.contributionCrawler = c; c.contributionSlots = make(chan struct{}, 2) })
	return c, ps, rt
}

func contribute(t *testing.T, c *Crawler, raw, html string) ContributionReceipt {
	t.Helper()
	receipt, err := c.FetchContribution(context.Background(), raw, nil, approvedForHTML(t, raw, html))
	if err != nil {
		t.Fatalf("contribute %s: %v", raw, err)
	}
	return receipt
}

func TestContributionOfAlreadyIndexedContentIsNotNovel(t *testing.T) {
	page := pagePlain("shared article text about raft consensus")
	routes := map[string]string{
		"https://news.example/guide":         page,
		"https://news.example/guide?ref=1":   page,
		"https://news.example/guide/":        page,
		"https://mirror.example/copy":        page,
		"https://news.example/other-article": pagePlain("an entirely different article about gossip protocols"),
	}
	c, ps, _ := pebbleContributor(t, routes)
	if err := c.FetchAndIndexNow(context.Background(), "https://news.example/guide"); err != nil {
		t.Fatal(err)
	}
	completeContentIndex(t, ps)
	for _, variant := range []string{"https://news.example/guide?ref=1", "https://news.example/guide/", "https://mirror.example/copy"} {
		if r := contribute(t, c, variant, page); !r.Indexed || r.Novel {
			t.Errorf("%s: already-indexed content reported novel: %+v", variant, r)
		}
	}
	other := contribute(t, c, "https://news.example/other-article", routes["https://news.example/other-article"])
	if !other.Novel {
		t.Fatalf("new content on a complete index must be novel: %+v", other)
	}
}

func TestContributionNoveltyIgnoresStaleIndexEntries(t *testing.T) {
	x, y := pagePlain("original text of the page"), pagePlain("rewritten text of the page")
	c, ps, rt := pebbleContributor(t, map[string]string{
		"https://news.example/a":   x,
		"https://news.example/b":   x,
		"https://news.example/c":   y,
		"https://news.example/d":   y,
		"https://news.example/e":   y,
		"https://news.example/new": pagePlain("unrelated"),
	})
	ctx := context.Background()
	if err := c.FetchAndIndexNow(ctx, "https://news.example/a"); err != nil {
		t.Fatal(err)
	}
	rt.setRoute("https://news.example/a", y)
	if err := c.FetchAndIndexNow(ctx, "https://news.example/a"); err != nil {
		t.Fatal(err)
	}
	completeContentIndex(t, ps)
	if r := contribute(t, c, "https://news.example/b", x); !r.Novel {
		t.Fatalf("content that a recrawl replaced still blocks novelty: %+v", r)
	}
	doc, err := ps.GetDocByURL(ctx, "https://news.example/a")
	if err != nil {
		t.Fatal(err)
	}
	if r := contribute(t, c, "https://news.example/c", y); r.Novel {
		t.Fatalf("live content reported novel: %+v", r)
	}
	if _, err := ps.SoftDeleteDocument(ctx, doc.ID, doc.URL); err != nil {
		t.Fatal(err)
	}
	cDoc, _ := ps.GetDocByURL(ctx, "https://news.example/c")
	if _, err := ps.SoftDeleteDocument(ctx, cDoc.ID, cDoc.URL); err != nil {
		t.Fatal(err)
	}
	if r := contribute(t, c, "https://news.example/d", y); !r.Novel {
		t.Fatalf("content of deleted documents still blocks novelty: %+v", r)
	}
}

func TestContributionOfExistingURLIsNotNovelEvenWithNewContent(t *testing.T) {
	routes := map[string]string{
		"https://news.example/live":  pagePlain("first version"),
		"https://news.example/moved": "redirect:https://news.example/live",
	}
	c, ps, rt := pebbleContributor(t, routes)
	ctx := context.Background()
	if err := c.FetchAndIndexNow(ctx, "https://news.example/live"); err != nil {
		t.Fatal(err)
	}
	completeContentIndex(t, ps)
	changed := pagePlain("second version with different words")
	rt.setRoute("https://news.example/live", changed)
	if r := contribute(t, c, "https://news.example/live", changed); !r.Indexed || r.Novel {
		t.Fatalf("existing URL reported novel: %+v", r)
	}
	rt.setRoute("https://news.example/live", pagePlain("third version, again different"))
	if r := contribute(t, c, "https://news.example/moved", pagePlain("third version, again different")); !r.Indexed || r.Novel {
		t.Fatalf("redirect onto an indexed URL reported novel: %+v", r)
	}
}

func TestContributionResubmissionIsNotNovel(t *testing.T) {
	page := pagePlain("resubmitted text")
	c, ps, _ := pebbleContributor(t, map[string]string{"https://news.example/r": page})
	completeContentIndex(t, ps)
	if first := contribute(t, c, "https://news.example/r", page); !first.Novel {
		t.Fatalf("first submission: %+v", first)
	}
	if again := contribute(t, c, "https://news.example/r", page); again.Novel {
		t.Fatalf("resubmission reported novel: %+v", again)
	}
}
