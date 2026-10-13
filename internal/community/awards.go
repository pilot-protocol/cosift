package community

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/community/wiki"
	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const (
	awardPageSize       = 50
	awardsPerCIDPerTick = 5
	awardRetryFloor     = 30 * time.Second
	awardRetryCeil      = time.Hour
	awardTerminalAge    = 7 * 24 * time.Hour
	awardInvalidLogGap  = time.Hour
	awardMemory         = 10000
)

var awardPollEvery = 30 * time.Second

var (
	articleAwardID  = regexp.MustCompile(`^(art|rev):([0-9A-HJKMNP-TV-Z]{26})$`)
	royaltyAwardID  = regexp.MustCompile(`^roy:([0-9]{4}-[0-9]{2}-[0-9]{2}):([0-9A-HJKMNP-TV-Z]{26}):([0-9a-f]{16})$`)
	clawbackAwardID = regexp.MustCompile(`^claw:([0-9A-HJKMNP-TV-Z]{26}_[0-9A-HJKMNP-TV-Z]{26}):([0-9a-f]{16})$`)
	awardCID        = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

type awardTries struct {
	failures int
	next     time.Time
	last     time.Time
}

// awardPoller applies pending credit awards to the SQLite ledger; one goroutine owns it.
type awardPoller struct {
	src      sharedaccount.AwardSource
	articles ArticleReader
	now      func() time.Time
	// hook lets a test stop the flow between steps, as a crash would.
	hook func(step string) error

	mu     sync.Mutex
	pos    sharedaccount.AwardPosition
	tries  map[string]*awardTries
	logged map[string]time.Time
}

func newAwardPoller(src sharedaccount.AwardSource, articles ArticleReader) *awardPoller {
	return &awardPoller{src: src, articles: articles, now: time.Now, tries: map[string]*awardTries{}, logged: map[string]time.Time{}}
}

type awardOutcome struct {
	status string // applied, skipped, invalid or retry
	detail string // skip reason, failed rule or failure class
}

func quoted(id string) string { return fmt.Sprintf("%.128q", id) }

// validateAward checks an award against itself; any identity that can write the outbox could write a forged one.
func validateAward(a sharedaccount.Award) string {
	switch {
	case !a.Valid:
		return "malformed"
	case a.Status != "pending" || a.Applied:
		return "state"
	case !sharedaccount.UIDPattern.MatchString(a.UID) || !awardCID.MatchString(a.CID):
		return "identity"
	}
	switch a.Reason {
	case "article", "revision":
		m := articleAwardID.FindStringSubmatch(a.ID)
		want := map[string]string{"article": "art", "revision": "rev"}[a.Reason]
		switch {
		case m == nil || m[1] != want || a.Ref.ContributionID != m[2]:
			return "id_form"
		case !ulidPattern.MatchString(a.Ref.ArticleID) || a.Ref.Version < 1:
			return "ref"
		case a.Amount < 1 || a.Amount > 600000:
			return "amount"
		}
	case "royalty":
		m := royaltyAwardID.FindStringSubmatch(a.ID)
		if m == nil || a.Ref.Day != m[1] || a.Ref.ArticleID != m[2] || a.CID != m[3] {
			return "id_form"
		}
		if _, err := time.Parse("2006-01-02", m[1]); err != nil {
			return "id_form"
		}
		if a.Amount < 1 || a.Amount > 100 {
			return "amount"
		}
	case "clawback":
		m := clawbackAwardID.FindStringSubmatch(a.ID)
		if m == nil || a.Ref.RequestID != m[1] || a.CID != m[2] || !ulidPattern.MatchString(a.Ref.ArticleID) {
			return "id_form"
		}
		if a.Amount >= 0 || a.Amount < -(1<<31) {
			return "amount"
		}
	default:
		return "reason"
	}
	return ""
}

func ledgerReason(award string) string {
	switch award {
	case "royalty":
		return "royalty"
	case "clawback":
		return "clawback"
	}
	return "contribution_award"
}

func (p *awardPoller) step(name string) error {
	if p.hook == nil {
		return nil
	}
	return p.hook(name)
}

func (s *Server) pollAwards(ctx context.Context) error {
	p := s.awards
	p.mu.Lock()
	pos := p.pos
	p.prune()
	p.mu.Unlock()
	page, err := p.src.PendingAwards(ctx, pos, awardPageSize)
	if err != nil {
		return err
	}
	perCID := map[string]int{}
	for _, a := range page {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.mu.Lock()
		p.pos = sharedaccount.AwardPosition{CreatedAt: a.CreatedAt, ID: a.ID}
		due := p.due(a.ID)
		p.mu.Unlock()
		if !due || perCID[a.CID] >= awardsPerCIDPerTick {
			continue
		}
		perCID[a.CID]++
		s.settleAward(ctx, a)
	}
	if len(page) < awardPageSize {
		p.mu.Lock()
		p.pos = sharedaccount.AwardPosition{}
		p.mu.Unlock()
	}
	return nil
}

func (p *awardPoller) due(id string) bool {
	t := p.tries[id]
	return t == nil || !p.now().Before(t.next)
}

func (p *awardPoller) prune() {
	for id, t := range p.tries {
		if p.now().Sub(t.last) > 2*awardRetryCeil {
			delete(p.tries, id)
		}
	}
}

func (p *awardPoller) fail(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	t := p.tries[id]
	if t == nil {
		if len(p.tries) >= awardMemory {
			for k := range p.tries {
				delete(p.tries, k)
				break
			}
		}
		t = &awardTries{}
		p.tries[id] = t
	}
	t.failures++
	delay := awardRetryFloor << min(t.failures-1, 7)
	if delay > awardRetryCeil {
		delay = awardRetryCeil
	}
	t.last = p.now()
	t.next = t.last.Add(delay)
	return t.failures
}

func (p *awardPoller) forget(id string) {
	p.mu.Lock()
	delete(p.tries, id)
	delete(p.logged, id)
	p.mu.Unlock()
}

func (s *Server) settleAward(ctx context.Context, a sharedaccount.Award) {
	p := s.awards
	out := s.applyAward(ctx, a)
	switch out.status {
	case "applied", "skipped":
		p.forget(a.ID)
	case "invalid":
		p.fail(a.ID)
		p.mu.Lock()
		last, seen := p.logged[a.ID]
		if !seen || p.now().Sub(last) >= awardInvalidLogGap {
			p.logged[a.ID] = p.now()
			log.Printf("community: award_invalid award=%s rule=%s", quoted(a.ID), out.detail)
		}
		p.mu.Unlock()
	default:
		failures := p.fail(a.ID)
		log.Printf("community: award_unresolved award=%s class=%s", quoted(a.ID), out.detail)
		if reason := terminalReason(out.detail); reason != "" && failures >= 2 && p.now().Sub(a.CreatedAt) >= awardTerminalAge {
			s.expireAward(ctx, a, reason)
		}
	}
}

func terminalReason(class string) string {
	switch class {
	case "account", "link":
		return "account_unresolvable"
	case "article":
		return "article_unavailable"
	}
	return ""
}

// expireAward gives an award that stayed unresolvable for a week its terminal state.
func (s *Server) expireAward(ctx context.Context, a sharedaccount.Award, reason string) {
	result, err := s.endingResult(ctx, a, reason)
	if err != nil {
		return
	}
	if err := s.awards.src.MarkApplied(ctx, a.ID, result); err == nil || errors.Is(err, sharedaccount.ErrNotPending) {
		s.awards.forget(a.ID)
	}
}

// endingResult skips an award for reason unless its ledger row exists, which wins.
func (s *Server) endingResult(ctx context.Context, a sharedaccount.Award, reason string) (sharedaccount.AwardResult, error) {
	delta, found, err := s.awardLedgerDelta(ctx, "award:"+a.ID)
	switch {
	case err != nil:
		return sharedaccount.AwardResult{}, err
	case found:
		return sharedaccount.AwardResult{Status: "applied", AppliedDelta: delta, LedgerID: "award:" + a.ID}, nil
	}
	return sharedaccount.AwardResult{Status: "skipped", SkipReason: reason}, nil
}

func (s *Server) skipAward(ctx context.Context, a sharedaccount.Award, reason string) awardOutcome {
	result, err := s.endingResult(ctx, a, reason)
	if err != nil {
		return awardOutcome{"retry", "ledger"}
	}
	return s.finishAward(ctx, a, result)
}

func (s *Server) awardLedgerDelta(ctx context.Context, ledgerID string) (delta int64, found bool, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT delta FROM credit_ledger WHERE id=?`, ledgerID).Scan(&delta)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return delta, err == nil, err
}

func (s *Server) applyAward(ctx context.Context, a sharedaccount.Award) awardOutcome {
	p := s.awards
	retry := func(class string) awardOutcome { return awardOutcome{"retry", class} }
	if rule := validateAward(a); rule != "" {
		return awardOutcome{"invalid", rule}
	}
	if err := p.step("contributor"); err != nil {
		return retry("firestore")
	}
	contributor, err := p.src.Contributor(ctx, a.CID)
	switch {
	case errors.Is(err, sharedaccount.ErrNotFound):
		return awardOutcome{"invalid", "contributor"}
	case err != nil:
		return retry("firestore")
	case contributor.UID != a.UID:
		return awardOutcome{"invalid", "contributor"}
	}
	if a.Reason == "article" || a.Reason == "revision" {
		if err := p.step("article"); err != nil {
			return retry("article")
		}
		record, err := p.articles.ArticleByID(ctx, a.Ref.ArticleID)
		switch {
		case errors.Is(err, ErrArticleGone):
			return s.skipAward(ctx, a, "article_unavailable")
		case err != nil:
			return retry("article")
		case record.Origin != "community" || !slices.Contains(record.Contributors, a.CID) || record.Version < a.Ref.Version:
			return awardOutcome{"invalid", "article_mismatch"}
		}
	}
	if err := p.step("account"); err != nil {
		return retry("account")
	}
	identity, err := p.src.Account(ctx, a.UID)
	switch {
	case errors.Is(err, sharedaccount.ErrUnauthorized):
		return s.skipAward(ctx, a, "unknown_account")
	case err != nil || identity.UID != a.UID:
		return retry("account")
	}
	if err := p.step("link"); err != nil {
		return retry("link")
	}
	userID, err := s.awardUser(ctx, identity)
	if err != nil {
		return retry("link")
	}
	if err := p.step("insert"); err != nil {
		return retry("ledger")
	}
	delta, err := s.writeAward(ctx, a, userID)
	if err != nil {
		return retry("ledger")
	}
	if err := p.step("mark"); err != nil {
		return retry("mark")
	}
	return s.finishAward(ctx, a, sharedaccount.AwardResult{Status: "applied", AppliedDelta: delta, LedgerID: "award:" + a.ID})
}

func (s *Server) finishAward(ctx context.Context, a sharedaccount.Award, result sharedaccount.AwardResult) awardOutcome {
	err := s.awards.src.MarkApplied(ctx, a.ID, result)
	if err != nil && !errors.Is(err, sharedaccount.ErrNotPending) {
		return awardOutcome{"retry", "mark"}
	}
	return awardOutcome{result.Status, result.SkipReason}
}

// awardUser links the account to a community user in a transaction over SQLite alone.
func (s *Server) awardUser(ctx context.Context, identity sharedaccount.Identity) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	id, err := linkSharedUser(ctx, tx, identity.UID, identity.Email)
	if err != nil {
		return "", err
	}
	return id, tx.Commit()
}

// writeAward is the one statement that credits or claws back; the clamp is computed inside it, and the ledger id makes a replay a no-op.
func (s *Server) writeAward(ctx context.Context, a sharedaccount.Award, userID string) (int64, error) {
	id, now := "award:"+a.ID, s.awards.now().Unix()
	var err error
	if a.Reason == "clawback" {
		_, err = s.db.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at)
SELECT ?,?,-MIN(?,MAX(0,(SELECT COALESCE(sum(delta),0) FROM credit_ledger WHERE user_id=?))),'clawback',? WHERE true
ON CONFLICT(id) DO NOTHING`, id, userID, -a.Amount, userID, now)
	} else {
		_, err = s.db.ExecContext(ctx, `INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING`, id, userID, a.Amount, ledgerReason(a.Reason), now)
	}
	if err != nil {
		return 0, err
	}
	delta, found, err := s.awardLedgerDelta(ctx, id)
	if err != nil || !found {
		return 0, errors.Join(err, errors.New("ledger row missing"))
	}
	return delta, nil
}

func (s *Server) runAwards(ctx context.Context) {
	tick := time.NewTicker(awardPollEvery)
	defer tick.Stop()
	for {
		if err := s.pollAwards(ctx); err != nil && ctx.Err() == nil {
			log.Printf("community: award poll failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func newConfiguredPoller(cfg Config) (*awardPoller, error) {
	if cfg.BrokerKey != "" && cfg.BrokerKey == cfg.Wiki.EngineKey {
		return nil, fmt.Errorf("COSIFT_COMMUNITY_BROKER_KEY and COSIFT_WIKI_ENGINE_KEY must be different keys")
	}
	if !cfg.Awards {
		return nil, nil
	}
	src, ok := cfg.Shared.(sharedaccount.AwardSource)
	if !ok {
		return nil, fmt.Errorf("COSIFT_COMMUNITY_AWARDS requires COSIFT_AUTH_MODE=shared")
	}
	articles := cfg.Articles
	if articles == nil {
		if !brokerKeyPattern.MatchString(cfg.Wiki.EngineKey) {
			return nil, fmt.Errorf("COSIFT_COMMUNITY_AWARDS requires COSIFT_WIKI_ENGINE_KEY for the article cross-check")
		}
		client, err := newEngineClient(cmp.Or(cfg.Wiki.EngineURL, wiki.DefaultEngineURL), cfg.Wiki.EngineKey)
		if err != nil {
			return nil, fmt.Errorf("COSIFT_WIKI_ENGINE_URL must be a loopback http origin")
		}
		articles = engineArticles{client}
	}
	return newAwardPoller(src, articles), nil
}
