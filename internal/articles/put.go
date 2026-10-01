package articles

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/cockroachdb/pebble"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const keepVersions = 5

type dropped struct {
	Aliases    int `json:"aliases"`
	TopicIDs   int `json:"topic_ids"`
	ClusterIDs int `json:"cluster_ids"`
}

type writeResult struct {
	Result  string    `json:"result"`
	Article writeView `json:"article"`
	Dropped dropped   `json:"dropped"`
}

// plan is a decided write: the record after it, and what to commit.
type plan struct {
	result  string
	rec     *Record
	prev    *Record
	write   bool
	content bool
	create  bool
	claims  []string
	dropped dropped
}

func (pl *plan) httpStatus() int {
	if pl.result == "created" {
		return http.StatusCreated
	}
	return http.StatusOK
}

// mergeCapped unions req into stored: stored entries stay, new ones are taken
// in request order until limit, and the rest are counted as dropped.
func mergeCapped(stored, req []string, limit int, skip func(string) bool) (out, added []string, dropped int) {
	out = slices.Clone(stored)
	have := set(stored...)
	for _, v := range req {
		if have[v] || (skip != nil && skip(v)) {
			continue
		}
		have[v] = true
		if len(out) >= limit {
			dropped++
			continue
		}
		out = append(out, v)
		added = append(added, v)
	}
	return out, added, dropped
}

// claimConflict returns the first topic in topics already claimed by another article.
func claimConflict(ix *index, id string, topics []string) *Record {
	for _, t := range topics {
		if owner := ix.claims[t]; owner != "" && owner != id {
			if r := ix.recs[owner]; r != nil {
				return r
			}
		}
	}
	return nil
}

// slugBlocker is a moderated record, in any environment, with this base slug.
func slugBlocker(ix *index, id, base string) *Record {
	var found *Record
	for _, r := range ix.recs {
		if r.ID != id && r.blocksTitle() && r.BaseSlug == base && (found == nil || r.ID < found.ID) {
			found = r
		}
	}
	return found
}

// fingerprintBlocker is a residue whose fingerprint is within theta_related of v.
func fingerprintBlocker(ix *index, v []float32, related float64) *Record {
	var best pick
	for id, fp := range ix.fps {
		if r := ix.recs[id]; r != nil && len(fp) == len(v) {
			best.offer(r, dot(fp, v))
		}
	}
	if best.atLeast(related) {
		return best.r
	}
	return nil
}

func (s *Store) handlePut(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	id := r.PathValue("id")
	if !validID(id) {
		v1.WriteError(w, v1.NotFound())
		return
	}
	cr := &countingReader{ReadCloser: r.Body}
	r.Body = cr
	var raw json.RawMessage
	if !v1.Decode(w, r, &raw, limitPut) {
		return
	}
	var head struct {
		Status *string `json:"status"`
	}
	if json.Unmarshal(raw, &head) != nil {
		v1.WriteError(w, v1.InvalidBody())
		return
	}
	if head.Status != nil && *head.Status == StatusPending {
		s.putStub(w, r, p, id, raw, cr.n)
		return
	}
	var b putBody
	if e := decodeStrict(raw, &b); e != nil {
		v1.WriteError(w, *e)
		return
	}
	s.putArticle(w, r, p, id, &b)
}

// envFirst refuses, before any validation, a staging write after go-live and
// a write to another environment's record.
func (s *Store) envFirst(w http.ResponseWriter, p v1.Principal, id string) bool {
	var e *v1.Error
	s.view(func(ix *index) {
		switch rec := ix.recs[id]; {
		case ix.golive != nil && p.Env == v1.EnvStaging:
			e = fail(v1.EnvGolive())
		case rec != nil && !p.SameEnv(rec.Prelive):
			e = fail(v1.EnvMismatch())
		}
	})
	if e != nil {
		v1.WriteError(w, *e)
		return false
	}
	return true
}

func (s *Store) putArticle(w http.ResponseWriter, r *http.Request, p v1.Principal, id string, b *putBody) {
	if !allowed(w, r, v1.ScopeArticlesWrite) || s.refuseRestorePending(w) || !s.chargeWrite(w, p) || !s.envFirst(w, p, id) {
		return
	}
	checked := false
	var verr *v1.Error
	check := func() *v1.Error {
		if !checked {
			checked = true
			if verr = validateArticle(b); verr == nil {
				e, err := s.checkCorpus(r.Context(), b.Citations)
				if err != nil {
					s.logf("articles: corpus lookup failed: %v", err)
					e = fail(v1.InternalError())
				}
				verr = e
			}
		}
		return verr
	}
	vecs := map[string][]float32{}
	for attempt := 0; attempt < 3; attempt++ {
		var pl *plan
		var perr *v1.Error
		s.view(func(ix *index) { pl, perr = s.planArticle(ix, p, id, b, check) })
		if perr != nil {
			v1.WriteError(w, *perr)
			return
		}
		if pl.content {
			var missing []string
			for _, t := range vectorTexts(pl.rec) {
				if _, ok := vecs[t]; !ok && !slices.Contains(missing, t) {
					missing = append(missing, t)
				}
			}
			if len(missing) > 0 {
				got, err := s.embed(r.Context(), s.putTimeout, missing)
				if err != nil {
					v1.WriteError(w, errEmbedderUnavailable())
					return
				}
				for i, t := range missing {
					vecs[t] = got[i]
				}
			}
		}
		res, e, retry := s.commitArticle(p, id, b, vecs, check)
		if e != nil {
			v1.WriteError(w, *e)
			return
		}
		if !retry {
			v1.WriteJSON(w, res.httpStatus(), writeResult{res.result, res.rec.written(), res.dropped})
			return
		}
	}
	v1.WriteError(w, errEmbedderUnavailable())
}

// commitArticle re-plans inside the write lock and commits; retry means the
// record changed underneath and needs vectors that were not computed.
func (s *Store) commitArticle(p v1.Principal, id string, b *putBody, vecs map[string][]float32, check func() *v1.Error) (*plan, *v1.Error, bool) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	a, e := s.activeLocked(p, v1.ScopeArticlesWrite, true)
	if e != nil {
		return nil, e, false
	}
	pl, e := s.planArticle(s.idx, a, id, b, check)
	if e != nil {
		return nil, e, false
	}
	if !pl.write {
		return pl, nil, false
	}
	var rows []row
	if pl.content {
		stored := storedVectors(pl.prev, s.idx.rows[id])
		texts := vectorTexts(pl.rec)
		all := make([][]float32, len(texts))
		for i, t := range texts {
			v, ok := vecs[t]
			if !ok {
				v, ok = stored[t]
			}
			if !ok {
				return nil, nil, true
			}
			all[i] = v
		}
		rows = rowsFor(pl.rec, all)
		if pl.prev == nil || pl.prev.Status == StatusPending {
			if blocker := fingerprintBlocker(s.idx, all[0], s.th().Related); blocker != nil {
				return nil, fail(errBlocked(a, blocker)), false
			}
		}
	}
	if pl.create {
		pl.rec.Slug = assignSlug(pl.rec.BaseSlug, func(sl string) bool { _, ok := s.idx.slugs[sl]; return ok })
	}
	bt := s.db.NewBatch()
	recJSON, _ := pl.rec.MarshalJSON()
	_ = bt.Set(recordKey(id), recJSON, nil)
	if pl.create {
		v, _ := json.Marshal(slugEntry{ID: id, Canonical: true})
		_ = bt.Set(slugKey(pl.rec.Slug), v, nil)
	}
	for _, t := range pl.claims {
		_ = bt.Set(topicKey(t), []byte(id), nil)
	}
	if pl.content {
		model, dim := s.embedderShape()
		_ = bt.Set(embeddingKey(id), encodeEmbeddings(model, dim, rows), nil)
	}
	if pl.result == "updated" && pl.content {
		prevJSON, _ := pl.prev.MarshalJSON()
		_ = bt.Set(versionKey(id, pl.prev.Version), prevJSON, nil)
		if err := s.pruneVersions(bt, id, pl.prev.Version); err != nil {
			bt.Close()
			return nil, fail(v1.InternalError()), false
		}
	}
	if err := s.commit(bt); err != nil {
		s.logf("articles: commit failed: %v", err)
		return nil, fail(v1.InternalError()), false
	}
	s.update(func(ix *index) {
		ix.putRecord(pl.rec)
		if pl.create {
			ix.slugs[pl.rec.Slug] = slugEntry{ID: id, Canonical: true}
		}
		for _, t := range pl.claims {
			ix.claims[t] = id
		}
		if pl.content {
			ix.rows[id] = rows
		}
	})
	return pl, nil, false
}

// storedVectors maps the texts of r's current rows to their vectors.
func storedVectors(r *Record, rows []row) map[string][]float32 {
	out := map[string][]float32{}
	if r == nil {
		return out
	}
	for _, rw := range rows {
		switch {
		case rw.kind == kindTitle:
			out[r.Title] = rw.vec
		case rw.kind == kindLead:
			out[r.Lead] = rw.vec
		case rw.kind == kindAlias && rw.alias < len(r.Aliases):
			out[r.Aliases[rw.alias]] = rw.vec
		}
	}
	return out
}

// pruneVersions deletes prior versions of id beyond the newest keepVersions,
// counting newest as one of them.
func (s *Store) pruneVersions(bt *pebble.Batch, id string, newest int) error {
	lower, upper := versionPrefix(id)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer it.Close()
	versions := []int{newest}
	for ok := it.First(); ok; ok = it.Next() {
		if pk, valid := parseKey(it.Key()); valid && pk.version != newest {
			versions = append(versions, pk.version)
		}
	}
	slices.Sort(versions)
	for len(versions) > keepVersions {
		_ = bt.Delete(versionKey(id, versions[0]), nil)
		versions = versions[1:]
	}
	return it.Error()
}

// planArticle decides an article PUT against ix; check validates the body after a same-hash unchanged.
func (s *Store) planArticle(ix *index, p v1.Principal, id string, b *putBody, check func() *v1.Error) (*plan, *v1.Error) {
	if ix.golive != nil && p.Env == v1.EnvStaging {
		return nil, fail(v1.EnvGolive())
	}
	now := s.nowString()
	prev := ix.recs[id]
	if prev != nil {
		if !p.SameEnv(prev.Prelive) {
			return nil, fail(v1.EnvMismatch())
		}
		if prev.Status == StatusRejected || prev.Status == StatusTombstoned {
			return nil, fail(errModerationLocked(prev.Status))
		}
		if b.Title != prev.Title {
			return nil, fail(v1.InvalidField("title", "title_immutable"))
		}
	}
	rec := &Record{Schema: 1, ID: id, Title: b.Title, AIGenerated: true}
	if prev != nil {
		rec = prev.clone()
		rec.AIGenerated = true
	}
	pl := &plan{rec: rec, prev: prev}
	var stored struct{ aliases, topics, clusters []string }
	if prev != nil {
		stored.aliases, stored.topics, stored.clusters = prev.Aliases, prev.TopicIDs, prev.ClusterIDs
	}
	var newTopics []string
	rec.Aliases, _, pl.dropped.Aliases = mergeCapped(stored.aliases, b.Aliases, 32, func(a string) bool { return a == rec.Title })
	rec.TopicIDs, newTopics, pl.dropped.TopicIDs = mergeCapped(stored.topics, b.TopicIDs, 256, nil)
	rec.ClusterIDs, _, pl.dropped.ClusterIDs = mergeCapped(stored.clusters, b.ClusterIDs, 64, nil)
	rec.Lead, rec.BodyMD, rec.Citations = b.Lead, b.BodyMD, slices.Clone(b.Citations)
	rec.QualityTier, rec.Vertical, rec.Sensitivity = b.QualityTier, b.Vertical, Sensitivity{}
	if b.Sensitivity != nil {
		rec.Sensitivity = *b.Sensitivity
	}
	rec.ContentSHA256 = contentHash(rec)
	if prev != nil && prev.Status != StatusPending && rec.ContentSHA256 == prev.ContentSHA256 {
		pl.result, pl.rec = "unchanged", prev
		if prev.Status == StatusPublished && b.Status != nil && *b.Status == StatusHeld {
			t := prev.clone()
			t.setStatus(StatusHeld, now)
			pl.rec, pl.write = t, true
		}
		return pl, nil
	}
	if e := check(); e != nil {
		return nil, e
	}
	rec.Models, rec.Build = *b.Models, *b.Build
	requested := *b.Status

	switch {
	case prev != nil && prev.Status != StatusPending:
		if b.ExpectedVersion != nil && *b.ExpectedVersion != prev.Version {
			v := prev.Version
			return nil, fail(errVersionConflict(&v))
		}
		status := requested
		if prev.Status == StatusHeld {
			status = StatusHeld
		}
		rec.Version = prev.Version + 1
		rec.UpdatedAt = now
		if status != prev.Status {
			rec.setStatus(status, now)
		} else {
			rec.setStatus(status, prev.StatusChangedAt)
		}
		pl.result = "updated"
	case prev != nil:
		if b.ExpectedVersion != nil && *b.ExpectedVersion != 0 {
			v := 0
			return nil, fail(errVersionConflict(&v))
		}
		rec.Version, rec.UpdatedAt = 1, now
		rec.setStatus(requested, now)
		pl.result = "filled"
	default:
		if b.ExpectedVersion != nil {
			return nil, fail(errVersionConflict(nil))
		}
		rec.BaseSlug = Slugify(rec.Title)
		rec.Prelive = p.Prelive()
		rec.Version, rec.CreatedAt, rec.UpdatedAt = 1, now, now
		rec.setStatus(requested, now)
		pl.result, pl.create = "created", true
	}
	if claimant := claimConflict(ix, id, newTopics); claimant != nil {
		return nil, fail(errTopicClaimed(p, claimant))
	}
	if pl.result != "updated" {
		if blocker := slugBlocker(ix, id, rec.BaseSlug); blocker != nil {
			return nil, fail(errBlocked(p, blocker))
		}
	}
	pl.claims = newTopics
	pl.write, pl.content = true, true
	return pl, nil
}

func (s *Store) putStub(w http.ResponseWriter, r *http.Request, p v1.Principal, id string, raw []byte, size int64) {
	if size > limitStub {
		v1.WriteError(w, v1.BodyTooLarge())
		return
	}
	var b stubBody
	if e := decodeStrict(raw, &b); e != nil {
		v1.WriteError(w, *e)
		return
	}
	if !allowed(w, r, v1.ScopeArticlesStub) || s.refuseRestorePending(w) || !s.chargeWrite(w, p) || !s.envFirst(w, p, id) {
		return
	}
	if e := validateStub(&b); e != nil {
		v1.WriteError(w, *e)
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	a, e := s.activeLocked(p, v1.ScopeArticlesStub, true)
	if e != nil {
		v1.WriteError(w, *e)
		return
	}
	pl, e := s.planStub(s.idx, a, id, &b)
	if e != nil {
		v1.WriteError(w, *e)
		return
	}
	if pl.write {
		bt := s.db.NewBatch()
		recJSON, _ := pl.rec.MarshalJSON()
		_ = bt.Set(recordKey(id), recJSON, nil)
		if pl.create {
			v, _ := json.Marshal(slugEntry{ID: id, Canonical: true})
			_ = bt.Set(slugKey(pl.rec.Slug), v, nil)
		}
		for _, t := range pl.claims {
			_ = bt.Set(topicKey(t), []byte(id), nil)
		}
		if err := s.commit(bt); err != nil {
			s.logf("articles: commit failed: %v", err)
			v1.WriteError(w, v1.InternalError())
			return
		}
		s.update(func(ix *index) {
			ix.putRecord(pl.rec)
			if pl.create {
				ix.slugs[pl.rec.Slug] = slugEntry{ID: id, Canonical: true}
			}
			for _, t := range pl.claims {
				ix.claims[t] = id
			}
		})
	}
	v1.WriteJSON(w, pl.httpStatus(), writeResult{pl.result, pl.rec.written(), pl.dropped})
}

func (s *Store) planStub(ix *index, p v1.Principal, id string, b *stubBody) (*plan, *v1.Error) {
	if ix.golive != nil && p.Env == v1.EnvStaging {
		return nil, fail(v1.EnvGolive())
	}
	prev := ix.recs[id]
	pl := &plan{prev: prev}
	if prev != nil {
		if !p.SameEnv(prev.Prelive) {
			return nil, fail(v1.EnvMismatch())
		}
		if prev.Status != StatusPending || prev.Title != b.Title || prev.Vertical != b.Vertical {
			return nil, fail(errExists(prev.Status))
		}
		rec := prev.clone()
		var added []string
		rec.TopicIDs, added, pl.dropped.TopicIDs = mergeCapped(prev.TopicIDs, *b.TopicIDs, 256, nil)
		if claimant := claimConflict(ix, id, added); claimant != nil {
			return nil, fail(errTopicClaimed(p, claimant))
		}
		rec.ContentSHA256 = contentHash(rec)
		pl.result, pl.rec, pl.claims, pl.write = "unchanged", rec, added, len(added) > 0
		if !pl.write {
			pl.rec = prev
		}
		return pl, nil
	}
	now := s.nowString()
	rec := &Record{Schema: 1, ID: id, Title: b.Title, Vertical: b.Vertical, BaseSlug: Slugify(b.Title),
		Prelive: p.Prelive(), CreatedAt: now, UpdatedAt: now}
	rec.setStatus(StatusPending, now)
	var added []string
	rec.TopicIDs, added, pl.dropped.TopicIDs = mergeCapped(nil, *b.TopicIDs, 256, nil)
	if claimant := claimConflict(ix, id, added); claimant != nil {
		return nil, fail(errTopicClaimed(p, claimant))
	}
	if blocker := slugBlocker(ix, id, rec.BaseSlug); blocker != nil {
		return nil, fail(errBlocked(p, blocker))
	}
	rec.Slug = assignSlug(rec.BaseSlug, func(sl string) bool { _, ok := ix.slugs[sl]; return ok })
	rec.ContentSHA256 = contentHash(rec)
	pl.result, pl.rec, pl.claims, pl.write, pl.create = "created", rec, added, true, true
	return pl, nil
}

func (s *Store) handleDeleteStub(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	id := r.PathValue("id")
	if !validID(id) {
		v1.WriteError(w, v1.NotFound())
		return
	}
	if !allowed(w, r, v1.ScopeArticlesStub) || s.refuseRestorePending(w) || !s.chargeWrite(w, p) {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	a, e := s.activeLocked(p, v1.ScopeArticlesStub, true)
	if e != nil {
		v1.WriteError(w, *e)
		return
	}
	prev := s.idx.recs[id]
	switch {
	case prev == nil:
		v1.WriteError(w, v1.NotFound())
		return
	case !a.SameEnv(prev.Prelive):
		v1.WriteError(w, v1.EnvMismatch())
		return
	case prev.Status != StatusPending:
		v1.WriteError(w, errNotPending(prev.Status))
		return
	}
	if err := s.deleteRecordLocked(id); err != nil {
		s.logf("articles: commit failed: %v", err)
		v1.WriteError(w, v1.InternalError())
		return
	}
	v1.WriteJSON(w, http.StatusOK, map[string]string{"result": "deleted"})
}

// deleteRecordLocked removes every key of id: its record, versions, slugs,
// claims, vectors, fingerprint and counters.
func (s *Store) deleteRecordLocked(id string) error {
	bt := s.db.NewBatch()
	s.deleteKeysInto(bt, s.idx, id)
	if err := s.commit(bt); err != nil {
		return err
	}
	s.update(func(ix *index) { ix.dropRecord(id) })
	s.counters.drop(id)
	return nil
}

func (s *Store) deleteKeysInto(bt *pebble.Batch, ix *index, id string) {
	_ = bt.Delete(recordKey(id), nil)
	lower, upper := versionPrefix(id)
	_ = bt.DeleteRange(lower, upper, nil)
	for sl, e := range ix.slugs {
		if e.ID == id {
			_ = bt.Delete(slugKey(sl), nil)
		}
	}
	for t, owner := range ix.claims {
		if owner == id {
			_ = bt.Delete(topicKey(t), nil)
		}
	}
	_ = bt.Delete(embeddingKey(id), nil)
	_ = bt.Delete(fingerprintKey(id), nil)
	_ = bt.Delete(countersKey(id), nil)
}
