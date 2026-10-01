package articles

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/cockroachdb/pebble"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

var (
	actions     = set("approve", "hold", "reject", "tombstone", "erase_fingerprint", "delete_stub")
	reasonCodes = set("privacy", "legal", "quality", "duplicate", "other")
)

type statusBody struct {
	Action     string  `json:"action"`
	ReasonCode *string `json:"reason_code"`
	By         string  `json:"by"`
}

type statusView struct {
	ID              string  `json:"id"`
	Slug            string  `json:"slug"`
	Status          *string `json:"status"`
	StatusChangedAt string  `json:"status_changed_at"`
	HasFingerprint  bool    `json:"has_fingerprint"`
}

func validateStatus(b *statusBody) *v1.Error {
	if !actions[b.Action] {
		return fail(v1.InvalidField("action", "enum"))
	}
	needsReason := b.Action != "approve" && b.Action != "hold"
	switch {
	case b.ReasonCode == nil && needsReason:
		return fail(v1.InvalidField("reason_code", "required"))
	case b.ReasonCode != nil && !reasonCodes[*b.ReasonCode]:
		return fail(v1.InvalidField("reason_code", "enum"))
	}
	if e := checkText("by", b.By, 1, 64); e != nil {
		return e
	}
	return nil
}

// legal reports whether action applies to r (§4.7).
func legal(action string, r *Record, hasFP bool) bool {
	switch action {
	case "approve":
		return r.Status == StatusHeld
	case "hold":
		return r.Status == StatusPublished || (r.Status == StatusRejected && !r.Residue)
	case "reject":
		return r.Status == StatusPublished || r.Status == StatusHeld
	case "tombstone":
		return r.Status != StatusTombstoned
	case "erase_fingerprint":
		return r.Residue && hasFP
	case "delete_stub":
		return r.Status == StatusPending
	}
	return false
}

func (s *Store) handleStatus(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	id := r.PathValue("id")
	if !validID(id) {
		v1.WriteError(w, v1.NotFound())
		return
	}
	var b statusBody
	if !v1.Decode(w, r, &b, limitStatus) {
		return
	}
	if !allowed(w, r, v1.ScopeArticlesModerate) {
		return
	}
	if e := validateStatus(&b); e != nil {
		v1.WriteError(w, *e)
		return
	}
	var fresh []float32
	if b.Action == "tombstone" {
		if rec := s.record(id); rec != nil && p.SameEnv(rec.Prelive) {
			fresh = s.takedownVectors(r.Context(), []*Record{rec})[rec.ID]
		}
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	a, e := s.activeLocked(p, v1.ScopeArticlesModerate, false)
	if e != nil {
		v1.WriteError(w, *e)
		return
	}
	rec := s.idx.recs[id]
	switch {
	case rec == nil:
		v1.WriteError(w, v1.NotFound())
		return
	case !a.SameEnv(rec.Prelive):
		v1.WriteError(w, v1.EnvMismatch())
		return
	case !legal(b.Action, rec, s.idx.anyFingerprint(id)):
		v1.WriteError(w, errIllegalTransition(rec.Status))
		return
	}
	now := s.nowString()
	reason := ""
	if b.ReasonCode != nil {
		reason = *b.ReasonCode
	}
	var err error
	switch b.Action {
	case "approve", "hold", "reject":
		to := map[string]string{"approve": StatusPublished, "hold": StatusHeld, "reject": StatusRejected}[b.Action]
		next := rec.clone()
		next.setStatus(to, now)
		next.Moderation = &Moderation{Action: b.Action, By: b.By, ReasonCode: reason, At: now}
		err = s.putRecordLocked(next)
	case "tombstone":
		err = s.tombstoneLocked(rec, reason, now, fresh)
	case "erase_fingerprint":
		err = s.eraseFingerprintLocked(id)
	case "delete_stub":
		err = s.deleteRecordLocked(id)
	}
	if err != nil {
		s.logf("articles: moderation commit failed: %v", err)
		v1.WriteError(w, v1.InternalError())
		return
	}
	view := statusView{ID: rec.ID, Slug: rec.Slug, StatusChangedAt: now}
	if cur := s.idx.recs[id]; cur != nil {
		view.Status, view.StatusChangedAt, view.HasFingerprint = &cur.Status, cur.StatusChangedAt, s.idx.hasFingerprint(id)
	}
	v1.WriteJSON(w, http.StatusOK, map[string]any{"result": "ok", "article": view})
}

func (s *Store) putRecordLocked(r *Record) error {
	b, _ := r.MarshalJSON()
	if err := s.db.Set(recordKey(r.ID), b, pebble.Sync); err != nil {
		return err
	}
	s.update(func(ix *index) { ix.putRecord(r) })
	return nil
}

// storedTitleVector reads the title row of id's R e, in whatever model wrote it.
func (s *Store) storedTitleVector(id string) (model string, dim int, v []float32, err error) {
	val, closer, err := s.db.Get(embeddingKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", 0, nil, nil
	}
	if err != nil {
		return "", 0, nil, err
	}
	defer closer.Close()
	model, dim, rows, err := decodeEmbeddings(val)
	if err != nil {
		return "", 0, nil, err
	}
	for _, r := range rows {
		if r.kind == kindTitle {
			return model, dim, r.vec, nil
		}
	}
	return "", 0, nil, nil
}

// erased is a record reduced to a residue, with the fingerprint written for it.
type erased struct {
	res     *Record
	fpModel string
	fp      []float32
}

// takedownVectors embeds, within 5 s, titles whose stored row is missing or of another model.
func (s *Store) takedownVectors(ctx context.Context, recs []*Record) map[string][]float32 {
	model, dim := s.embedderShape()
	var need []*Record
	for _, r := range recs {
		if r.Residue || r.Title == "" || s.emb == nil {
			continue
		}
		if m, d, v, err := s.storedTitleVector(r.ID); err != nil || v == nil || m != model || d != dim {
			need = append(need, r)
		}
	}
	out := map[string][]float32{}
	if len(need) == 0 {
		return out
	}
	texts := make([]string, len(need))
	for i, r := range need {
		texts[i] = r.Title
	}
	vecs, err := s.embed(ctx, 5*time.Second, texts)
	if err != nil {
		s.logf("articles: takedown fingerprint embed failed (embedder_unavailable); %d records keep no current-model fingerprint", len(need))
		return out
	}
	for i, r := range need {
		out[r.ID] = vecs[i]
	}
	return out
}

// eraseInto adds r's erasure and fingerprint (fresh: its title in the running model) to bt.
func (s *Store) eraseInto(bt *pebble.Batch, r *Record, status, reason, at string, fresh []float32) (erased, error) {
	out := erased{res: r.toResidue(status, reason, at)}
	if !s.idx.anyFingerprint(r.ID) {
		model, dim, v, err := s.storedTitleVector(r.ID)
		if err != nil {
			return out, err
		}
		if fresh != nil {
			model, dim = s.embedderShape()
			v = fresh
		}
		if v != nil {
			_ = bt.Set(fingerprintKey(r.ID), encodeFingerprint(model, dim, v), nil)
			out.fpModel, out.fp = model, v
		}
	}
	lower, upper := versionPrefix(r.ID)
	_ = bt.DeleteRange(lower, upper, nil)
	_ = bt.Delete(embeddingKey(r.ID), nil)
	_ = bt.Delete(countersKey(r.ID), nil)
	return out, nil
}

func (s *Store) applyErased(ix *index, e erased) {
	id := e.res.ID
	ix.putRecord(e.res)
	delete(ix.rows, id)
	if e.fp != nil {
		if model, dim := s.embedderShape(); s.emb == nil || (e.fpModel == model && len(e.fp) == dim) {
			ix.fps[id] = e.fp
		} else {
			ix.fpOther[id] = true
		}
	}
}

func (s *Store) tombstoneLocked(r *Record, reason, at string, fresh []float32) error {
	bt := s.db.NewBatch()
	e, err := s.eraseInto(bt, r, StatusTombstoned, reason, at, fresh)
	if err != nil {
		bt.Close()
		return err
	}
	b, _ := e.res.MarshalJSON()
	_ = bt.Set(recordKey(r.ID), b, nil)
	if err := s.commit(bt); err != nil {
		return err
	}
	s.update(func(ix *index) { s.applyErased(ix, e) })
	s.counters.drop(r.ID)
	s.scheduleCompact()
	return nil
}

func (s *Store) eraseFingerprintLocked(id string) error {
	if err := s.db.Delete(fingerprintKey(id), pebble.Sync); err != nil {
		return err
	}
	s.update(func(ix *index) {
		delete(ix.fps, id)
		delete(ix.fpOther, id)
	})
	s.scheduleCompact()
	return nil
}

type purgeBody struct {
	Apply         *bool `json:"apply"`
	ExpectRecords *int  `json:"expect_records"`
}

type purgeCounts struct {
	Apply   bool `json:"apply"`
	Records int  `json:"records"`
	Delete  struct {
		Published int `json:"published"`
		Held      int `json:"held"`
		Pending   int `json:"pending"`
	} `json:"delete"`
	Convert struct {
		Tombstoned int `json:"tombstoned"`
		Rejected   int `json:"rejected"`
	} `json:"convert"`
	Keys struct {
		A int `json:"a"`
		V int `json:"v"`
		S int `json:"s"`
		T int `json:"t"`
		E int `json:"e"`
		C int `json:"c"`
	} `json:"keys"`
	MatrixRows     int      `json:"matrix_rows"`
	StagingWriters []string `json:"staging_writers"`
	GoliveAt       string   `json:"golive_at,omitempty"`
}

// converts reports whether the purge keeps a prelive record as a residue.
func converts(r *Record) bool { return r.Status == StatusTombstoned || r.Status == StatusRejected }

// purgeCounts counts what the purge would remove, from one snapshot of the 'R' keys.
func (s *Store) purgeCounts() (purgeCounts, error) {
	var c purgeCounts
	var ids []string
	err := s.scanSnapshot(func(pk parsedKey, _ []byte, r *Record) {
		if r == nil || !r.Prelive || pk.sub == subMeta {
			return
		}
		del := !converts(r)
		switch pk.sub {
		case subRecord:
			c.Records++
			ids = append(ids, r.ID)
			switch r.Status {
			case StatusPublished:
				c.Delete.Published++
			case StatusHeld:
				c.Delete.Held++
			case StatusPending:
				c.Delete.Pending++
			case StatusTombstoned:
				c.Convert.Tombstoned++
			case StatusRejected:
				c.Convert.Rejected++
			}
			if del {
				c.Keys.A++
			}
		case subSlug:
			if del {
				c.Keys.S++
			}
		case subTopic:
			if del {
				c.Keys.T++
			}
		case subVersion:
			c.Keys.V++
		case subEmbedding:
			c.Keys.E++
		case subCounters:
			c.Keys.C++
		}
	})
	s.view(func(ix *index) {
		for _, id := range ids {
			c.MatrixRows += len(ix.rows[id])
		}
	})
	c.StagingWriters = s.policy.StagingWriters()
	if c.StagingWriters == nil {
		c.StagingWriters = []string{}
	}
	return c, err
}

// scanSnapshot visits one snapshot's 'R' keys with their record (nil: orphan or meta); records sort first.
func (s *Store) scanSnapshot(fn func(pk parsedKey, val []byte, owner *Record)) error {
	snap := s.db.NewSnapshot()
	defer snap.Close()
	lower, upper := familyBounds()
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	recs := map[string]*Record{}
	for ok := it.First(); ok; ok = it.Next() {
		pk, valid := parseKey(it.Key())
		if !valid {
			continue
		}
		val := it.Value()
		owner := pk.suffix
		switch pk.sub {
		case subRecord:
			if r, err := decodeRecord(val); err == nil && r.ID == pk.suffix {
				recs[pk.suffix] = r
			}
		case subSlug:
			var e slugEntry
			owner = ""
			if json.Unmarshal(val, &e) == nil {
				owner = e.ID
			}
		case subTopic:
			owner = string(val)
		case subMeta:
			owner = ""
		}
		fn(pk, val, recs[owner])
	}
	return it.Close()
}

func (s *Store) handlePurge(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	if !allowed(w, r, v1.ScopeArticlesAdmin) {
		return
	}
	if s.golive() {
		v1.WriteError(w, errGolive())
		return
	}
	var b purgeBody
	if !v1.Decode(w, r, &b, limitPurge) {
		return
	}
	if b.Apply == nil || !*b.Apply {
		var c purgeCounts
		var err error
		c, err = s.purgeCounts()
		if err != nil {
			v1.WriteError(w, v1.InternalError())
			return
		}
		v1.WriteJSON(w, http.StatusOK, c)
		return
	}
	if b.ExpectRecords == nil {
		v1.WriteError(w, v1.InvalidField("expect_records", "required"))
		return
	}
	var rejected []*Record
	s.view(func(ix *index) {
		for _, rec := range ix.recs {
			if rec.Prelive && converts(rec) && !rec.Residue {
				rejected = append(rejected, rec)
			}
		}
	})
	fresh := s.takedownVectors(r.Context(), rejected)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	a, e := s.activeLocked(p, v1.ScopeArticlesAdmin, false)
	if e != nil {
		v1.WriteError(w, *e)
		return
	}
	if s.idx.golive != nil {
		v1.WriteError(w, errGolive())
		return
	}
	c, err := s.purgeCounts()
	if err != nil {
		v1.WriteError(w, v1.InternalError())
		return
	}
	if len(c.StagingWriters) > 0 {
		v1.WriteError(w, errStagingWriters(c.StagingWriters))
		return
	}
	if c.Records != *b.ExpectRecords {
		v1.WriteError(w, errCountChanged(c.Records))
		return
	}
	res, err := s.applyPurgeLocked(a, c.Records, fresh)
	if err != nil {
		s.logf("articles: purge commit failed: %v", err)
		v1.WriteError(w, v1.InternalError())
		return
	}
	c.Apply, c.GoliveAt = true, res.At
	v1.WriteJSON(w, http.StatusOK, c)
}

func (s *Store) applyPurgeLocked(p v1.Principal, records int, fresh map[string][]float32) (goliveMeta, error) {
	now := s.nowString()
	bt := s.db.NewBatch()
	var deleted []string
	var conv []erased
	for id, r := range s.idx.recs {
		if !r.Prelive {
			continue
		}
		if !converts(r) {
			s.deleteKeysInto(bt, s.idx, id)
			deleted = append(deleted, id)
			continue
		}
		keep := r.clone()
		keep.Prelive = false
		e, err := s.eraseInto(bt, keep, r.Status, r.ReasonCode, now, fresh[id])
		if err != nil {
			bt.Close()
			return goliveMeta{}, err
		}
		if r.Residue {
			e.res.TombstonedAt, e.res.StatusChangedAt = r.TombstonedAt, r.StatusChangedAt
		} else if r.Moderation != nil {
			e.res.ReasonCode = r.Moderation.ReasonCode
		}
		b, _ := e.res.MarshalJSON()
		_ = bt.Set(recordKey(id), b, nil)
		conv = append(conv, e)
	}
	g := goliveMeta{At: now, By: p.ID, Records: records}
	gv, _ := json.Marshal(g)
	_ = bt.Set(metaKey("golive"), gv, nil)
	if err := s.commit(bt); err != nil {
		return goliveMeta{}, err
	}
	s.update(func(ix *index) {
		for _, id := range deleted {
			ix.dropRecord(id)
		}
		for _, e := range conv {
			s.applyErased(ix, e)
		}
		ix.golive = &g
	})
	for _, id := range deleted {
		s.counters.drop(id)
	}
	for _, e := range conv {
		s.counters.drop(e.res.ID)
	}
	s.scheduleCompact()
	return g, nil
}
