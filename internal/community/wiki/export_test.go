package wiki

import "time"

// Hooks for the external tests.

func SetPageSizes(w *Wiki, hub, sitemap int) { w.hubPage, w.sitemapPage = hub, sitemap }

func SetPoll(w *Wiki, d time.Duration) { w.poll = d }

// EntryStatus is the test-only cache inspector: the slug's cached status and whether it is live.
func EntryStatus(w *Wiki, slug string) (int, bool) {
	e, live := inspect(w, slug)
	return e.status, live
}

func Known(w *Wiki, slug string) bool { return known(w, slug) }

func LimiterSize(w *Wiki) int { return w.ips.size() }
