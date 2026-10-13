package crawler

import (
	"context"
	"testing"
)

func TestContributionRobotsRedirectIsNotSubjectToHopPolicy(t *testing.T) {
	const page = "http://front.example/page"
	for name, tc := range map[string]struct {
		target string
		mod    func(*Crawler)
	}{
		"host outside the include list": {"http://cdn.other.example/robots.txt", func(c *Crawler) { c.cfg.IncludeDomains = []string{"front.example"} }},
		"excluded host":                 {"http://cdn.other.example/robots.txt", func(c *Crawler) { c.cfg.ExcludeDomains = []string{"other.example"} }},
		"IP literal":                    {"http://93.184.216.34/robots.txt", nil},
	} {
		t.Run(name, func(t *testing.T) {
			routes := map[string]string{
				"http://front.example/robots.txt": "redirect:" + tc.target,
				tc.target:                         "User-agent: *\nDisallow: /\n",
				page:                              sampleHTML,
			}
			c, rt, _ := redirectFixture(t, routes, func(c *Crawler) {
				c.cfg.RespectRobots = true
				c.robots = NewRobots(c.http, c.cfg.UserAgent)
				if tc.mod != nil {
					tc.mod(c)
				}
			})
			_, err := c.FetchContribution(context.Background(), page, nil, approvedFor(t, page))
			if err == nil {
				t.Fatal("contribution completed although the redirected robots.txt disallows it")
			}
			if rt.hitsFor(page) != 0 {
				t.Fatal("the page was fetched")
			}
			if rt.hitsFor(tc.target) != 1 {
				t.Fatalf("the redirected robots.txt was fetched %d times, want 1", rt.hitsFor(tc.target))
			}
			if doc, derr := c.store.GetDocByURL(context.Background(), page); derr == nil && doc != nil {
				t.Fatal("the disallowed page was indexed")
			}
		})
	}
}
