package crawler

import (
	"context"
	"errors"
	"testing"

	"github.com/pilot-protocol/cosift/internal/store"
)

var errInjectedRead = errors.New("injected store read failure")

type failingReadStore struct {
	*store.PebbleStore
	failURL string
}

func (f failingReadStore) GetDocByURL(ctx context.Context, url string) (*store.Document, error) {
	if url == f.failURL {
		return nil, errInjectedRead
	}
	return f.PebbleStore.GetDocByURL(ctx, url)
}

func TestContributionFailsWhenTheStoreCannotBeRead(t *testing.T) {
	live := "https://news.example/live"
	for name, submitted := range map[string]string{
		"submitted URL": live,
		"final URL":     "https://news.example/moved",
	} {
		t.Run(name, func(t *testing.T) {
			first, changed := pagePlain("first version"), pagePlain("second version with different words")
			c, ps, rt := pebbleContributor(t, map[string]string{
				live:                         first,
				"https://news.example/moved": "redirect:" + live,
			})
			ctx := context.Background()
			if err := c.FetchAndIndexNow(ctx, live); err != nil {
				t.Fatal(err)
			}
			completeContentIndex(t, ps)
			rt.setRoute(live, changed)
			c.store = failingReadStore{PebbleStore: ps, failURL: live}
			_, err := c.FetchContribution(ctx, submitted, nil, approvedForHTML(t, live, changed))
			if !errors.Is(err, errInjectedRead) {
				t.Fatalf("want the store read error, got %v", err)
			}
			if doc, derr := ps.GetDocByURL(ctx, live); derr != nil || doc.Text != "first version" {
				t.Fatalf("the stored document was rewritten: %+v %v", doc, derr)
			}
		})
	}
}
