package articles

import (
	"errors"
	"net/http"
	"regexp"
	"slices"

	"github.com/cockroachdb/pebble"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type revertBody struct {
	ToVersion  *int    `json:"to_version"`
	ReasonCode *string `json:"reason_code"`
	By         string  `json:"by"`
	RequestID  *string `json:"request_id"`
}

type revertResult struct {
	Result               string    `json:"result"`
	Article              writeView `json:"article"`
	ToVersion            int       `json:"to_version"`
	RevertedVersions     []int     `json:"reverted_versions"`
	RevertedContributors []string  `json:"reverted_contributors"`
	Replayed             bool      `json:"replayed,omitempty"`
}

func validateRevert(b *revertBody) *v1.Error {
	switch {
	case b.ToVersion == nil:
		return fail(v1.InvalidField("to_version", "required"))
	case *b.ToVersion < 1:
		return fail(v1.InvalidField("to_version", "range"))
	case b.ReasonCode == nil:
		return fail(v1.InvalidField("reason_code", "required"))
	case !reasonCodes[*b.ReasonCode]:
		return fail(v1.InvalidField("reason_code", "enum"))
	}
	if e := checkText("by", b.By, 1, 64); e != nil {
		return e
	}
	if id := b.RequestID; id != nil {
		switch {
		case len(*id) < 1 || len(*id) > 64:
			return fail(v1.InvalidField("request_id", "length"))
		case !requestIDPattern.MatchString(*id):
			return fail(v1.InvalidField("request_id", "pattern"))
		}
	}
	return nil
}

func (s *Store) versionRecord(id string, n int) (*Record, error) {
	val, closer, err := s.db.Get(versionKey(id, n))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return decodeRecord(val)
}

func (s *Store) handleRevert(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	id := r.PathValue("id")
	if !validID(id) {
		v1.WriteError(w, v1.NotFound())
		return
	}
	if !allowed(w, r, v1.ScopeArticlesModerate) {
		return
	}
	var b revertBody
	if !v1.Decode(w, r, &b, limitStatus) {
		return
	}
	if e := validateRevert(&b); e != nil {
		v1.WriteError(w, *e)
		return
	}
	vecs := map[string][]float32{}
	if e := s.embedTargetLead(r, id, *b.ToVersion, vecs); e != nil {
		v1.WriteError(w, *e)
		return
	}
	for attempt := 0; attempt < 3; attempt++ {
		res, status, need, e := s.revertLocked(p, id, &b, vecs)
		if e != nil {
			v1.WriteError(w, *e)
			return
		}
		if len(need) == 0 {
			v1.WriteJSON(w, status, res)
			return
		}
		got, err := s.embed(r.Context(), s.putTimeout, need)
		if err != nil {
			v1.WriteError(w, errEmbedderUnavailable())
			return
		}
		for i, t := range need {
			vecs[t] = got[i]
		}
	}
	v1.WriteError(w, errEmbedderUnavailable())
}

// embedTargetLead embeds the target's lead before the write lock, only when it differs from the stored one.
func (s *Store) embedTargetLead(r *http.Request, id string, to int, vecs map[string][]float32) *v1.Error {
	rec := s.record(id)
	if rec == nil || rec.Status != StatusPublished || to >= rec.Version {
		return nil
	}
	tgt, err := s.versionRecord(id, to)
	if err != nil || tgt.Lead == rec.Lead {
		return nil
	}
	got, err := s.embed(r.Context(), s.putTimeout, []string{tgt.Lead})
	if err != nil {
		return fail(errEmbedderUnavailable())
	}
	vecs[tgt.Lead] = got[0]
	return nil
}

// revertLocked decides and commits a revert under the write lock; need lists texts whose vectors are missing.
func (s *Store) revertLocked(p v1.Principal, id string, b *revertBody, vecs map[string][]float32) (revertResult, int, []string, *v1.Error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	a, e := s.activeLocked(p, v1.ScopeArticlesModerate, false)
	if e != nil {
		return revertResult{}, 0, nil, e
	}
	stored := s.idx.recs[id]
	switch {
	case stored == nil:
		return revertResult{}, 0, nil, fail(v1.NotFound())
	case !a.SameEnv(stored.Prelive):
		return revertResult{}, 0, nil, fail(v1.EnvMismatch())
	}
	to := *b.ToVersion
	if m := stored.Moderation; m != nil && m.Revert != nil && b.RequestID != nil && m.RequestID == *b.RequestID {
		return revertResult{Result: "reverted", Article: stored.written(), ToVersion: m.ToVersion,
			RevertedVersions: nonNil(m.RevertedVersions), RevertedContributors: nonNil(m.RevertedContributors), Replayed: true}, http.StatusOK, nil, nil
	}
	if stored.Status != StatusPublished {
		return revertResult{}, 0, nil, fail(errIllegalTransition(stored.Status))
	}
	if to >= stored.Version {
		return revertResult{}, 0, nil, fail(v1.InvalidField("to_version", "range"))
	}
	tgt, err := s.versionRecord(id, to)
	switch {
	case errors.Is(err, pebble.ErrNotFound):
		return revertResult{}, 0, nil, fail(v1.NotFound())
	case err != nil:
		s.logf("articles: revert target read failed: %v", err)
		return revertResult{}, 0, nil, fail(v1.InternalError())
	case tgt.Status != StatusPublished:
		return revertResult{}, 0, nil, fail(v1.InvalidField("to_version", "must_be_published"))
	case tgt.Sensitivity.Class != "none":
		return revertResult{}, 0, nil, fail(v1.InvalidField("to_version", "must_be_none"))
	}
	cand := stored.clone()
	cand.Lead, cand.BodyMD, cand.Citations = tgt.Lead, tgt.BodyMD, slices.Clone(tgt.Citations)
	cand.QualityTier, cand.Vertical, cand.Sensitivity = tgt.QualityTier, tgt.Vertical, tgt.Sensitivity
	cand.Models, cand.Build = tgt.Models, tgt.Build
	cand.ContentSHA256 = contentHash(cand)
	if cand.ContentSHA256 == stored.ContentSHA256 {
		return revertResult{Result: "unchanged", Article: stored.written(), ToVersion: to,
			RevertedVersions: []int{}, RevertedContributors: []string{}}, http.StatusOK, nil, nil
	}
	versions, contributors, err := s.revertedBy(stored, to)
	if err != nil {
		s.logf("articles: revert history read failed: %v", err)
		return revertResult{}, 0, nil, fail(v1.InternalError())
	}
	storedVecs := storedVectors(stored, s.idx.rows[id])
	texts := vectorTexts(cand)
	all := make([][]float32, len(texts))
	var need []string
	for i, t := range texts {
		v, ok := vecs[t]
		if !ok {
			v, ok = storedVecs[t]
		}
		if !ok {
			need = append(need, t)
			continue
		}
		all[i] = v
	}
	if len(need) > 0 {
		return revertResult{}, 0, need, nil
	}
	now := s.nowString()
	cand.Version, cand.UpdatedAt = stored.Version+1, now
	if tgt.VersionContributor != nil {
		c := *tgt.VersionContributor
		cand.VersionContributor = &c
	} else {
		cand.VersionContributor = nil
	}
	cand.RevertsTo = to
	rv := &Revert{ToVersion: to, RevertedVersions: nonNil(versions), RevertedContributors: nonNil(contributors)}
	if b.RequestID != nil {
		rv.RequestID = *b.RequestID
	}
	cand.Moderation = &Moderation{Action: "revert", By: b.By, ReasonCode: *b.ReasonCode, At: now, Revert: rv}
	rows := rowsFor(cand, all)
	bt := s.db.NewBatch()
	candJSON, _ := cand.MarshalJSON()
	prevJSON, _ := stored.MarshalJSON()
	_ = bt.Set(recordKey(id), candJSON, nil)
	_ = bt.Set(versionKey(id, stored.Version), prevJSON, nil)
	model, dim := s.embedderShape()
	_ = bt.Set(embeddingKey(id), encodeEmbeddings(model, dim, rows), nil)
	if err := s.pruneVersions(bt, id, stored.Version, keepFor(cand)); err != nil {
		bt.Close()
		return revertResult{}, 0, nil, fail(v1.InternalError())
	}
	if err := s.commit(bt); err != nil {
		s.logf("articles: revert commit failed: %v", err)
		return revertResult{}, 0, nil, fail(v1.InternalError())
	}
	s.update(func(ix *index) {
		ix.putRecord(cand)
		ix.rows[id] = rows
	})
	return revertResult{Result: "reverted", Article: cand.written(), ToVersion: to,
		RevertedVersions: nonNil(versions), RevertedContributors: nonNil(contributors)}, http.StatusOK, nil, nil
}

// revertedBy lists the versions after to that a revert to it takes out, and the cids that wrote them.
func (s *Store) revertedBy(cur *Record, to int) ([]int, []string, error) {
	revertsTo := map[int]int{}
	contributor := map[int]*string{}
	for v := to + 1; v <= cur.Version; v++ {
		rec := cur
		if v < cur.Version {
			var err error
			if rec, err = s.versionRecord(cur.ID, v); err != nil {
				return nil, nil, err
			}
		}
		revertsTo[v], contributor[v] = rec.RevertsTo, rec.VersionContributor
	}
	var versions []int
	var cids []string
	for v := to + 1; v <= cur.Version; v++ {
		if revertsTo[v] != 0 {
			continue
		}
		takenOut := false
		for r := v + 1; r <= cur.Version; r++ {
			if revertsTo[r] != 0 && revertsTo[r] < v {
				takenOut = true
				break
			}
		}
		if takenOut {
			continue
		}
		versions = append(versions, v)
		if c := contributor[v]; c != nil && !slices.Contains(cids, *c) {
			cids = append(cids, *c)
		}
	}
	return versions, cids, nil
}
