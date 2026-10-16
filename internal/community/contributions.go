package community

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const (
	contributionsShown   = 50
	earnedShown          = 50
	contributionsTimeout = 8 * time.Second
	slugTimeout          = 5 * time.Second
	slugTTL              = 5 * time.Minute
	slugGoneTTL          = time.Minute
	slugCacheMax         = 2000
	titleRunes           = 200
	urlRunes             = 2048
)

var (
	statusPattern  = regexp.MustCompile(`^[a-z_]{1,20}$`)
	codePattern    = regexp.MustCompile(`^[a-z0-9_]{1,40}$`)
	fieldPattern   = regexp.MustCompile(`^[A-Za-z0-9_.\[\]]{1,80}$`)
	handleGrammar  = regexp.MustCompile(`^[a-z0-9_-]{3,24}$`)
	articleSlugRe  = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	earnedReasons  = []any{"contribution_award", "royalty", "clawback", "fetch_verified", "verified_contribution"}
	awardLedgerKey = "award:"
)

type articleRef struct {
	ID      string `json:"id"`
	URL     string `json:"url,omitempty"`
	Version int64  `json:"version"`
}

type refusalItem struct {
	Code  string `json:"code"`
	Field string `json:"field,omitempty"`
}

type contributionItem struct {
	ID            string        `json:"contribution_id"`
	Kind          string        `json:"kind"`
	Status        string        `json:"status"`
	StatusAt      string        `json:"status_at"`
	Title         string        `json:"title,omitempty"`
	Article       *articleRef   `json:"article,omitempty"`
	Credits       *int64        `json:"credits,omitempty"`
	Refusals      []refusalItem `json:"refusals,omitempty"`
	LostReason    string        `json:"lost_reason,omitempty"`
	URLs          []string      `json:"urls,omitempty"`
	RecheckAt     string        `json:"recheck_at,omitempty"`
	SourcesPushed *bool         `json:"sources_pushed,omitempty"`
}

type earnedItem struct {
	Reason  string `json:"reason"`
	Delta   int64  `json:"delta"`
	At      string `json:"at"`
	AwardID string `json:"award_id,omitempty"`
}

type slugEntry struct {
	slug  string
	until time.Time
}

// slugCache maps article ids to slugs for a few minutes; an article the engine no longer returns is remembered shorter, with no slug.
type slugCache struct {
	mu sync.Mutex
	m  map[string]slugEntry
}

func (c *slugCache) get(id string, now time.Time) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[id]
	if !ok || !e.until.After(now) {
		return "", false
	}
	return e.slug, true
}

func (c *slugCache) put(id, slug string, ttl time.Duration, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]slugEntry{}
	}
	if _, ok := c.m[id]; !ok && len(c.m) >= slugCacheMax {
		for k, e := range c.m {
			if !e.until.After(now) {
				delete(c.m, k)
			}
		}
		for k := range c.m {
			if len(c.m) < slugCacheMax {
				break
			}
			delete(c.m, k)
		}
	}
	c.m[id] = slugEntry{slug, now.Add(ttl)}
}

// articleURL is the wiki address of an article the engine still serves, else empty.
func (s *Server) articleURL(ctx context.Context, id string) string {
	if s.articles == nil {
		return ""
	}
	now := time.Now()
	slug, ok := s.slugs.get(id, now)
	if !ok {
		rec, err := s.articles.ArticleByID(ctx, id)
		switch {
		case err == nil && articleSlugRe.MatchString(rec.Slug) && len(rec.Slug) <= 80:
			slug = rec.Slug
			s.slugs.put(id, slug, slugTTL, now)
		case err == nil || errors.Is(err, ErrArticleGone):
			s.slugs.put(id, "", slugGoneTTL, now)
		}
	}
	if slug == "" {
		return ""
	}
	return s.cfg.PublicURL + "/wiki/" + slug
}

// plain drops control, format and line-break characters and collapses spaces; a contributor's text is data, never markup.
func plain(s string, max int) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case unicode.IsSpace(r):
			return ' '
		case unicode.In(r, unicode.Cc, unicode.Cf, unicode.Zl, unicode.Zp):
			return -1
		}
		return r
	}, s)
	out := []rune(strings.Join(strings.Fields(s), " "))
	if len(out) > max {
		out = out[:max]
	}
	return string(out)
}

func (s *Server) contributions(w http.ResponseWriter, r *http.Request, u User) {
	w.Header().Set("Cache-Control", "private, no-store")
	if !s.allowRoute(w, "contributions", u.ID, contributionsPerMinute) {
		return
	}
	src, _ := s.cfg.Shared.(sharedaccount.ContributionSource)
	var uid string
	if src != nil {
		_ = s.db.QueryRowContext(r.Context(), `SELECT uid FROM shared_identities WHERE user_id=?`, u.ID).Scan(&uid)
	}
	if uid == "" {
		respond(w, http.StatusOK, map[string]any{"joined": false})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), contributionsTimeout)
	defer cancel()
	member, err := src.Member(ctx, uid)
	if errors.Is(err, sharedaccount.ErrNotFound) {
		respond(w, http.StatusOK, map[string]any{"joined": false})
		return
	}
	var records []sharedaccount.ContributionRecord
	if err == nil {
		records, err = src.Contributions(ctx, member.CID, contributionsShown+1)
	}
	if err != nil {
		problemCode(w, http.StatusServiceUnavailable, "contributions are unavailable", "contributions_unavailable", nil)
		return
	}
	earned, err := s.earnedRows(ctx, u.ID)
	if err != nil {
		problemCode(w, http.StatusInternalServerError, "contributions are unavailable", "contributions_unavailable", nil)
		return
	}
	out := map[string]any{"joined": true, "contributions": s.contributionItems(r.Context(), records), "more": len(records) > contributionsShown, "fetch": fetchCounts(member.Fetch), "earned": earned}
	if handleGrammar.MatchString(member.Handle) {
		out["handle"] = member.Handle
	}
	respond(w, http.StatusOK, out)
}

func fetchCounts(in map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for _, name := range []string{"claimed", "submitted", "verified", "mismatch", "rejected", "credits"} {
		out[name] = max(0, in[name])
	}
	return out
}

func (s *Server) contributionItems(ctx context.Context, records []sharedaccount.ContributionRecord) []contributionItem {
	ctx, cancel := context.WithTimeout(ctx, slugTimeout)
	defer cancel()
	items := []contributionItem{}
	for i, rec := range records {
		if i == contributionsShown {
			break
		}
		if !ulidPattern.MatchString(rec.ID) || !statusPattern.MatchString(rec.Status) || (rec.Kind != "new" && rec.Kind != "revision") || rec.StatusAt.IsZero() {
			continue
		}
		it := contributionItem{ID: rec.ID, Kind: rec.Kind, Status: rec.Status, StatusAt: rec.StatusAt.UTC().Format(time.RFC3339), Title: plain(rec.Title, titleRunes)}
		switch rec.Status {
		case "published", "won":
			credits := max(0, rec.Credits)
			it.Credits = &credits
			if ulidPattern.MatchString(rec.ArticleID) {
				it.Article = &articleRef{ID: rec.ArticleID, URL: s.articleURL(ctx, rec.ArticleID), Version: max(0, rec.Version)}
			}
		case "refused":
			for _, c := range rec.Refusals {
				if codePattern.MatchString(c.Code) {
					ref := refusalItem{Code: c.Code}
					if fieldPattern.MatchString(c.Field) {
						ref.Field = c.Field
					}
					it.Refusals = append(it.Refusals, ref)
				}
			}
		case "lost":
			if codePattern.MatchString(rec.LostReason) {
				it.LostReason = rec.LostReason
			}
		case "needs_sources":
			for _, raw := range rec.URLs {
				if u := plain(raw, urlRunes); strings.HasPrefix(u, "https://") || strings.HasPrefix(u, "http://") {
					it.URLs = append(it.URLs, u)
				}
			}
			if !rec.RecheckAt.IsZero() {
				it.RecheckAt = rec.RecheckAt.UTC().Format(time.RFC3339)
			}
			pushed := rec.SourcesPushed
			it.SourcesPushed = &pushed
		}
		items = append(items, it)
	}
	return items
}

// earnedRows are the newest ledger rows that pay or reclaim contribution credit; the award id is the ledger id without its prefix.
func (s *Server) earnedRows(ctx context.Context, userID string) ([]earnedItem, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,delta,reason,created_at FROM credit_ledger WHERE user_id=? AND reason IN (?,?,?,?,?) ORDER BY created_at DESC,id DESC LIMIT ?`,
		append(append([]any{userID}, earnedReasons...), earnedShown)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []earnedItem{}
	for rows.Next() {
		var id, reason string
		var delta, at int64
		if err := rows.Scan(&id, &delta, &reason, &at); err != nil {
			return nil, err
		}
		item := earnedItem{Reason: reason, Delta: delta, At: time.Unix(at, 0).UTC().Format(time.RFC3339)}
		if after, ok := strings.CutPrefix(id, awardLedgerKey); ok {
			item.AwardID = after
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
