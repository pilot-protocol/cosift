package crawler

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

type flakyContentIndex struct {
	*store.PebbleStore
	completeErr  error
	completeFrom int
	calls        int
}

func (f *flakyContentIndex) ContentIndexComplete(ctx context.Context) (bool, error) {
	f.calls++
	if f.completeErr != nil && f.calls >= f.completeFrom {
		return false, f.completeErr
	}
	return f.PebbleStore.ContentIndexComplete(ctx)
}

func flakyContributor(t *testing.T, routes map[string]string, flaky *flakyContentIndex) *Crawler {
	t.Helper()
	ps, err := store.OpenPebble(filepath.Join(t.TempDir(), "pebble"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ps.Close() })
	flaky.PebbleStore = ps
	completeContentIndex(t, ps)
	cfg := testCrawlerCfg()
	cfg.RespectRobots = false
	cfg.PerHostDelayMs = 0
	cfg.DisableLinkFollowing = true
	c := NewWithBackend(cfg, flaky, index.NewPebbleBM25(ps))
	c.http.Transport = &siteRT{hits: map[string]int{}, routes: routes}
	c.contributionOnce.Do(func() { c.contributionCrawler = c; c.contributionSlots = make(chan struct{}, 2) })
	return c
}

func TestContributionFailsBeforeWritingWhenTheContentIndexCannotBeRead(t *testing.T) {
	page := pagePlain("text the index cannot vouch for")
	boom := errors.New("index unreadable")
	flaky := &flakyContentIndex{completeErr: boom, completeFrom: 1}
	c := flakyContributor(t, map[string]string{"https://news.example/a": page}, flaky)
	r, err := c.FetchContribution(context.Background(), "https://news.example/a", nil, approvedForHTML(t, "https://news.example/a", page))
	if !errors.Is(err, boom) || r.Indexed || r.Novel {
		t.Fatalf("receipt %+v err %v", r, err)
	}
	if doc, err := flaky.GetDocByURL(context.Background(), "https://news.example/a"); err == nil && doc != nil {
		t.Fatal("document written although novelty could not be decided")
	}
}

func TestContributionReceiptFailsWhenTheIndexStateCannotBeReadAfterTheWrite(t *testing.T) {
	page := pagePlain("text whose receipt cannot report the index state")
	boom := errors.New("index unreadable")
	flaky := &flakyContentIndex{completeErr: boom, completeFrom: 2}
	c := flakyContributor(t, map[string]string{"https://news.example/b": page}, flaky)
	r, err := c.FetchContribution(context.Background(), "https://news.example/b", nil, approvedForHTML(t, "https://news.example/b", page))
	if !errors.Is(err, boom) || r.Indexed || r.Novel || r.ContentIndexComplete {
		t.Fatalf("a receipt without a known index state was returned: %+v %v", r, err)
	}
}

func TestRobotsCacheUsesTheDefaultBoundsForNonPositiveLimits(t *testing.T) {
	r := NewRobotsWithLimits(nil, "ua", 0, -5)
	if r.maxEntries != DefaultRobotsMaxEntries || r.maxBytes != DefaultRobotsMaxBytes {
		t.Fatalf("bounds %d entries, %d bytes", r.maxEntries, r.maxBytes)
	}
	if r := NewRobotsWithLimits(nil, "ua", 7, 1234); r.maxEntries != 7 || r.maxBytes != 1234 {
		t.Fatalf("explicit bounds not kept: %d, %d", r.maxEntries, r.maxBytes)
	}
}
