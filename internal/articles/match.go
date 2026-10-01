package articles

import (
	"math"
	"net/http"
	"unicode"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

type matchBody struct {
	Q        *string   `json:"q"`
	TopicID  *string   `json:"topic_id"`
	TopicIDs *[]string `json:"topic_ids"`
	Self     *string   `json:"self"`
	Reader   *string   `json:"reader"`
	Purpose  *string   `json:"purpose"`
}

type claimView struct {
	TopicID   string `json:"topic_id"`
	ArticleID string `json:"article_id,omitempty"`
	Status    string `json:"status,omitempty"`
	OtherEnv  bool   `json:"other_env,omitempty"`
}

type refView struct {
	ID     string `json:"id"`
	Slug   string `json:"slug,omitempty"`
	Status string `json:"status,omitempty"`
	Title  string `json:"title,omitempty"`
}

type serveResult struct {
	Verdict string   `json:"verdict"`
	Match   string   `json:"match,omitempty"`
	Score   *float64 `json:"score"`
	Article any      `json:"article,omitempty"`
}

type buildResult struct {
	Verdict            string      `json:"verdict"`
	Article            *refView    `json:"article,omitempty"`
	Claims             []claimView `json:"claims"`
	Writes             string      `json:"writes"`
	ThresholdsMeasured bool        `json:"thresholds_measured"`
}

func validQuery(q string) bool {
	if len(q) < 1 || len(q) > 512 {
		return false
	}
	for _, r := range q {
		if unicode.In(r, unicode.Cc, unicode.Cf) {
			return false
		}
	}
	return true
}

func (s *Store) handleMatch(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	var b matchBody
	if !v1.Decode(w, r, &b, limitMatch) {
		return
	}
	build := false
	if b.Purpose != nil {
		switch *b.Purpose {
		case "serve":
		case "build":
			build = true
		default:
			v1.WriteError(w, v1.InvalidField("purpose", "enum"))
			return
		}
	}
	if !allowed(w, r, v1.ScopeArticlesRead) {
		return
	}
	if build && !allowed(w, r, v1.ScopeArticlesWrite, v1.ScopeArticlesStub) {
		return
	}
	if s.refuseRestorePending(w) {
		return
	}
	if e := validateMatch(&b, build); e != nil {
		v1.WriteError(w, *e)
		return
	}
	vecs, err := s.embed(r.Context(), s.matchTimeout, []string{*b.Q})
	if err != nil {
		v1.WriteError(w, errEmbedderUnavailable())
		return
	}
	if build {
		v1.WriteJSON(w, http.StatusOK, s.matchBuild(p, &b, vecs[0]))
		return
	}
	v1.WriteJSON(w, http.StatusOK, s.matchServe(p, &b, vecs[0]))
}

func validateMatch(b *matchBody, build bool) *v1.Error {
	switch {
	case b.Q == nil:
		return fail(v1.InvalidField("q", "required"))
	case !validQuery(*b.Q):
		return fail(v1.InvalidField("q", "invalid"))
	case b.TopicID != nil && (build || !topicPattern.MatchString(*b.TopicID)):
		return fail(v1.InvalidField("topic_id", ruleFor(build, "pattern")))
	case b.TopicIDs != nil && !build:
		return fail(v1.InvalidField("topic_ids", "build_only"))
	case b.TopicIDs != nil && len(*b.TopicIDs) == 0:
		return fail(v1.InvalidField("topic_ids", "empty"))
	case b.Self != nil && (!build || !validID(*b.Self)):
		return fail(v1.InvalidField("self", ruleFor(!build, "pattern")))
	case b.Reader != nil && !readerPattern.MatchString(*b.Reader):
		return fail(v1.InvalidField("reader", "pattern"))
	}
	if b.TopicIDs != nil {
		return checkTopics("topic_ids", *b.TopicIDs, 256)
	}
	return nil
}

func ruleFor(wrongMode bool, rule string) string {
	if wrongMode {
		return "wrong_purpose"
	}
	return rule
}

// eligible is what articles:read may ever see in serve mode (§3.3).
func eligible(p v1.Principal, r *Record) bool {
	return r != nil && r.Status == StatusPublished && !r.Residue && (p.Env == v1.EnvStaging || !r.Prelive)
}

func bestScore(rows []row, q []float32) float32 {
	best := float32(math.Inf(-1))
	for _, rw := range rows {
		if len(rw.vec) == len(q) {
			best = max(best, dot(rw.vec, q))
		}
	}
	return best
}

// pick keeps the best-scoring record, ties to the smaller id.
type pick struct {
	r     *Record
	score float32
}

func (pk *pick) offer(r *Record, sc float32) {
	if math.IsInf(float64(sc), -1) {
		return
	}
	if pk.r == nil || sc > pk.score || (sc == pk.score && r.ID < pk.r.ID) {
		pk.r, pk.score = r, sc
	}
}

func (pk *pick) atLeast(t float64) bool { return pk.r != nil && float64(pk.score) >= t }

func rounded(f float32) *float64 {
	v := math.Round(float64(f)*1e6) / 1e6
	return &v
}

func (s *Store) matchServe(p v1.Principal, b *matchBody, q []float32) serveResult {
	th := s.th()
	var res serveResult
	s.view(func(ix *index) {
		var covered *Record
		res, covered = serveVerdict(ix, p, b, q, th)
		// Counted under the same read lock, so a tombstone cannot drop the set first.
		if covered != nil && p.CountsReads && b.Reader != nil {
			s.counters.add(covered.ID, *b.Reader, s.now())
		}
	})
	return res
}

func serveVerdict(ix *index, p v1.Principal, b *matchBody, q []float32, th Thresholds) (serveResult, *Record) {
	var top pick
	scores := map[string]float32{}
	for id, rows := range ix.rows {
		if !eligible(p, ix.recs[id]) {
			continue
		}
		sc := bestScore(rows, q)
		scores[id] = sc
		top.offer(ix.recs[id], sc)
	}
	if b.TopicID != nil {
		if owner := ix.claims[*b.TopicID]; owner != "" {
			if sc, ok := scores[owner]; ok && !math.IsInf(float64(sc), -1) && float64(sc) >= th.Related {
				covered := ix.recs[owner]
				return serveResult{Verdict: "covered", Match: "alias", Score: rounded(sc), Article: covered.public(promotedAt(covered, th.PromotionMinTier))}, covered
			}
		}
	}
	rec := top.r
	switch {
	case rec == nil:
		return serveResult{Verdict: "none"}, nil
	case top.atLeast(th.Covered):
		return serveResult{Verdict: "covered", Match: "embedding", Score: rounded(top.score), Article: rec.public(promotedAt(rec, th.PromotionMinTier))}, rec
	case top.atLeast(th.Related):
		return serveResult{Verdict: "related", Match: "embedding", Score: rounded(top.score), Article: refView{ID: rec.ID, Slug: rec.Slug, Title: rec.Title}}, nil
	}
	return serveResult{Verdict: "none", Score: rounded(top.score)}, nil
}

func (s *Store) writesState(p v1.Principal) string {
	switch {
	case s.policy.WritesFrozen():
		return "frozen"
	case s.policy.WritesExhausted(p.ID):
		return "budget_exhausted"
	}
	return "open"
}

// matchBuild is the build-purpose match: own environment only, never a score or content.
func (s *Store) matchBuild(p v1.Principal, b *matchBody, q []float32) buildResult {
	th := s.th()
	res := buildResult{Claims: []claimView{}, Writes: s.writesState(p), ThresholdsMeasured: th.Measured}
	self := ""
	if b.Self != nil {
		self = *b.Self
	}
	var topics []string
	if b.TopicIDs != nil {
		topics = *b.TopicIDs
	}
	qBase, qFull := Slugify(*b.Q), fullSlug(*b.Q)
	s.view(func(ix *index) {
		own := func(r *Record) bool { return r != nil && p.SameEnv(r.Prelive) }
		heldSelf := func(r *Record) bool { return r.ID == self && r.Status == StatusHeld }
		blockedBy := func(r *Record) {
			if res.Verdict == "" {
				res.Verdict, res.Article = "blocked", &refView{ID: r.ID, Status: r.Status}
			}
		}
		held := map[string]int{}
		seen := map[string]bool{}
		for _, t := range topics {
			if seen[t] {
				continue
			}
			seen[t] = true
			owner := ix.recs[ix.claims[t]]
			if owner == nil {
				continue
			}
			if !own(owner) {
				res.Claims = append(res.Claims, claimView{TopicID: t, OtherEnv: true})
				continue
			}
			res.Claims = append(res.Claims, claimView{TopicID: t, ArticleID: owner.ID, Status: owner.Status})
			held[owner.ID]++
			if owner.blocksTitle() && !heldSelf(owner) {
				blockedBy(owner)
			}
		}
		var hr, fr pick
		for id, rows := range ix.rows {
			r := ix.recs[id]
			if own(r) && (r.Status == StatusHeld || r.Status == StatusRejected) && !heldSelf(r) {
				hr.offer(r, bestScore(rows, q))
			}
		}
		if hr.atLeast(th.Covered) {
			blockedBy(hr.r)
		}
		for id, v := range ix.fps {
			if r := ix.recs[id]; own(r) && len(v) == len(q) {
				fr.offer(r, dot(v, q))
			}
		}
		if fr.atLeast(th.Related) {
			blockedBy(fr.r)
		}
		var sr *Record
		for _, r := range ix.recs {
			if own(r) && r.blocksTitle() && r.BaseSlug == qBase && !heldSelf(r) && (sr == nil || r.ID < sr.ID) {
				sr = r
			}
		}
		if sr != nil {
			blockedBy(sr)
		}
		if res.Verdict != "" {
			return
		}
		var claimant *Record
		better := func(r *Record) bool {
			if claimant == nil {
				return true
			}
			if held[r.ID] != held[claimant.ID] {
				return held[r.ID] > held[claimant.ID]
			}
			if r.CreatedAt != claimant.CreatedAt {
				return r.CreatedAt < claimant.CreatedAt
			}
			return r.ID < claimant.ID
		}
		for id, r := range ix.recs {
			if !own(r) || id == self || (r.Status != StatusPublished && r.Status != StatusPending) {
				continue
			}
			if (held[id] > 0 || ix.fullSlugs[id] == qFull) && better(r) {
				claimant = r
			}
		}
		if claimant != nil {
			res.Verdict = "claimed"
			res.Article = &refView{ID: claimant.ID, Slug: claimant.Slug, Status: claimant.Status, Title: claimant.Title}
			return
		}
		var top pick
		for id, rows := range ix.rows {
			if r := ix.recs[id]; own(r) && id != self && r.Status == StatusPublished && !r.Residue {
				top.offer(r, bestScore(rows, q))
			}
		}
		switch t := top.r; {
		case top.atLeast(th.Covered):
			res.Verdict = "covered"
			res.Article = &refView{ID: t.ID, Slug: t.Slug, Status: t.Status, Title: t.Title}
		case top.atLeast(th.Related):
			res.Verdict = "related"
			res.Article = &refView{ID: t.ID, Slug: t.Slug, Title: t.Title}
		default:
			res.Verdict = "none"
		}
	})
	return res
}
