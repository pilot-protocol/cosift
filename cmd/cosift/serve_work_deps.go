package main

import (
	"context"
	"errors"
	"log"
	"strings"
	"unicode/utf8"

	"github.com/pilot-protocol/cosift/internal/adultfilter"
	"github.com/pilot-protocol/cosift/internal/community"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/store"
	"github.com/pilot-protocol/cosift/internal/work"
)

const (
	workModerationText = 32000
	workModerationJoin = "\n[…]\n"
	workModerationHead = (workModerationText - len(workModerationJoin)) / 2
	workModerationTail = workModerationText - len(workModerationJoin) - workModerationHead
)

var errWorkCrawlerDown = errors.New("crawler not running")

// workCanonical is the pool's canonical form of a public web URL: the intake's rules decide whether
// the URL is acceptable, the crawler's canonical form is what is stored and leased.
func workCanonical(raw string) (string, error) {
	if _, err := community.NormalizeURL(raw); err != nil {
		return "", err
	}
	return crawler.ContributionURL(raw)
}

// initWork builds the work service over the server's store. The crawler arrives later through attach;
// until then the server fetch and the indexing report the crawler as down.
func (s *pebbleHTTP) initWork(ps *store.PebbleStore) error {
	h := s.work
	crawlerOrNil := func() *crawler.Crawler { return h.crawler.Load() }
	svc, err := work.New(work.Options{
		DB: ps.DB(), Config: h.settings, Frontier: h.frontier, Logf: log.Printf,
		Deps: work.Deps{
			Canonical: workCanonical,
			Allowed: func(u string) bool {
				c := crawlerOrNil()
				return c != nil && c.AllowedURL(u)
			},
			InCorpus: func(ctx context.Context, u string) (bool, error) {
				_, err := ps.GetDocByURL(ctx, u)
				if errors.Is(err, store.ErrNotFound) {
					return false, nil
				}
				return err == nil, err
			},
			Frontier: workFrontier{ps},
			Fetch: func(ctx context.Context, u string) (work.Page, error) {
				c := crawlerOrNil()
				if c == nil {
					return work.Page{}, errWorkCrawlerDown
				}
				p, err := c.FetchCopy(ctx, u)
				if err != nil {
					return work.Page{}, err
				}
				return work.Page{FinalURL: p.FinalURL, Title: p.Title, Text: p.Text, Lang: p.Lang}, nil
			},
			BotCheck: workUnusableCopy,
			Moderate: s.workModerate,
			Index: func(ctx context.Context, d crawler.AttestedDoc) (bool, bool, error) {
				c := crawlerOrNil()
				if c == nil {
					return false, false, errWorkCrawlerDown
				}
				indexed, novel, err := c.IndexAttested(ctx, d)
				if err != nil {
					return false, false, err
				}
				if err := ps.CompleteFrontier(ctx, d.LeaseURL); err != nil && !errors.Is(err, store.ErrNotFound) {
					return false, false, err
				}
				return indexed, novel, nil
			},
			Purge: func(ctx context.Context, u string, apply bool) (int, int, error) {
				doc, err := ps.GetDocByURL(ctx, u)
				if errors.Is(err, store.ErrNotFound) {
					return 0, 0, nil
				}
				if err != nil {
					return 0, 0, err
				}
				if doc.Source != "attested" {
					return 0, 1, nil
				}
				if apply {
					if _, err := ps.SoftDeleteDocument(ctx, doc.ID, u); err != nil {
						return 0, 0, err
					}
					if g := s.hnsw(); g != nil {
						g.MarkURLPassagesInvalid(u)
					}
				}
				return 1, 0, nil
			},
		},
	})
	if err != nil {
		return err
	}
	h.svc = svc
	log.Printf("pebble-serve: fetch work enabled=%v frontier=%v", h.enabled, h.frontier)
	return nil
}

// workUnusableCopy reports a server copy that is an error, login, bot-check or placeholder page: it counts as a failed fetch.
func workUnusableCopy(title, text string) bool {
	status, _ := community.ObviousQualityProblem(community.ModerationDocument{Title: title, Text: text})
	return status != ""
}

// workFrontier reads frontier rows for the feeders.
type workFrontier struct{ ps *store.PebbleStore }

func (f workFrontier) scan(ctx context.Context, status store.FrontierStatus, after string, n int) ([]work.FrontierRow, string, bool, error) {
	rows, next, end, err := f.ps.ScanFrontier(ctx, status, after, n)
	out := make([]work.FrontierRow, len(rows))
	for i, r := range rows {
		out[i] = work.FrontierRow{URL: r.URL, LastError: r.LastError}
	}
	return out, next, end, err
}

func (f workFrontier) Errored(ctx context.Context, after string, n int) ([]work.FrontierRow, string, bool, error) {
	return f.scan(ctx, store.FrontierStatusError, after, n)
}

func (f workFrontier) Queued(ctx context.Context, after string, n int) ([]work.FrontierRow, string, bool, error) {
	return f.scan(ctx, store.FrontierStatusQueued, after, n)
}

// workModerate decides a page the work service verified. The adult filter and the quality prefilter see
// the full text; the model sees at most its head and tail, because the shared check refuses a larger document.
func (s *pebbleHTTP) workModerate(ctx context.Context, doc work.ModerationDoc, reserve func() error) (work.Verdict, error) {
	if adultfilter.IsAdult("", "", doc.URL) || adultfilter.IsAdult(doc.Title, doc.Text, doc.URL) {
		return work.Verdict{Decision: "reject", Category: "adult"}, nil
	}
	title, _ := cutUTF8(doc.Title, 1000)
	full := community.ModerationDocument{URL: doc.URL, Title: title, Text: doc.Text}
	if status, _ := community.ObviousQualityProblem(full); status != "" {
		if status == "unverified" {
			return work.Verdict{Decision: "uncertain", Category: "unverified"}, nil
		}
		return work.Verdict{Decision: "reject", Category: "low_quality"}, nil
	}
	full.Text = headAndTail(doc.Text)
	v, err := s.moderateDocument(ctx, full, reserve)
	return work.Verdict{Decision: v.Decision, Category: v.Category}, err
}

// headAndTail keeps the head and tail of a text longer than the model may see, joined by a marker, in 32,000 bytes
// at most and each cut on a character boundary.
func headAndTail(text string) string {
	if len(text) <= workModerationText {
		return text
	}
	head, _ := cutUTF8(text, workModerationHead)
	start := len(text) - workModerationTail
	for start < len(text) && !utf8.RuneStart(text[start]) {
		start++
	}
	return head + workModerationJoin + strings.Clone(text[start:])
}
