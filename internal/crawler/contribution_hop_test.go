package crawler

import (
	"context"
	"errors"
	"testing"
)

func TestContributionReceiptNamesFinalURL(t *testing.T) {
	routes := map[string]string{
		"http://site.example/moved":       "redirect:https://www.site.example/guide/",
		"https://www.site.example/guide/": sampleHTML,
		"http://site.example/plain":       sampleHTML,
	}
	c, _, _ := redirectFixture(t, routes, nil)
	ctx := context.Background()
	moved, err := c.FetchContribution(ctx, "http://site.example/moved", nil, approvedFor(t, "https://www.site.example/guide/"))
	if err != nil || moved.FinalURL != "https://www.site.example/guide/" {
		t.Fatalf("redirected receipt %+v %v", moved, err)
	}
	plain, err := c.FetchContribution(ctx, "http://site.example/plain", nil, approvedFor(t, "http://site.example/plain"))
	if err != nil || plain.FinalURL != "http://site.example/plain" {
		t.Fatalf("plain receipt %+v %v", plain, err)
	}
}

func TestContributionHopPolicy(t *testing.T) {
	c := newBare(testCrawlerCfg())
	c.cfg.ExcludeDomains = []string{"banned.example"}
	tests := map[string]bool{
		"https://ok.example/page":            true,
		"http://ok.example:8080/page":        true,
		"https://banned.example/page":        false,
		"https://cdn.banned.example/page":    false,
		"https://user:pw@ok.example/page":    false,
		"https://93.184.216.34/page":         false,
		"http://[2606:4700:4700::1111]/page": false,
		"ftp://ok.example/file":              false,
		"https:///nohost":                    false,
	}
	for raw, want := range tests {
		if got := c.contributionHopAllowed(raw) == nil; got != want {
			t.Errorf("contributionHopAllowed(%s) = %v, want %v", raw, got, want)
		}
	}
	if err := c.contributionHopAllowed("https://banned.example/x"); !errors.Is(err, ErrContributionRejected) {
		t.Fatalf("rejection is not terminal: %v", err)
	}
}

func TestContributionRedirectToUnicodeSpellingOfExcludedHostIsRejected(t *testing.T) {
	const fullwidth = "http://\uff53\uff50\uff41\uff4d.example.com/page"
	routes := map[string]string{
		"http://good.example.com/r": "redirect:" + fullwidth,
		fullwidth:                   sampleHTML,
	}
	c, rt, _ := redirectFixture(t, routes, func(c *Crawler) {
		c.cfg.IncludeDomains = []string{"example.com"}
		c.cfg.ExcludeDomains = []string{"spam.example.com"}
	})
	_, err := c.FetchContribution(context.Background(), "http://good.example.com/r", nil, approvedFor(t, fullwidth))
	if !errors.Is(err, ErrContributionRejected) {
		t.Fatalf("want a terminal rejection, got %v", err)
	}
	if rt.hitsFor(fullwidth) != 0 {
		t.Fatal("the excluded host was requested")
	}
}
