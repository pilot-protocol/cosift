package articles

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

// readScope picks the projection: read_all sees every record, read only what §3.3 allows.
func (s *Store) readScope(w http.ResponseWriter, r *http.Request, p v1.Principal) (all, ok bool) {
	if !allowed(w, r, v1.ScopeArticlesRead, v1.ScopeArticlesReadAll) {
		return false, false
	}
	if p.Has(v1.ScopeArticlesReadAll) {
		return true, true
	}
	return false, !s.refuseRestorePending(w)
}

// visible is §3.3 for a pending or tombstoned record reached by slug.
func visible(p v1.Principal, r *Record) bool { return p.Env == v1.EnvStaging || !r.Prelive }

func (s *Store) fullView(ix *index, r *Record) json.RawMessage {
	now := s.now()
	return full(r, s.counters.readers7d(r.ID, now), s.counters.days(r.ID, now), ix.hasFingerprint(r.ID))
}

func (s *Store) handleGet(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	id := r.PathValue("id")
	if !validID(id) {
		v1.WriteError(w, v1.NotFound())
		return
	}
	all, ok := s.readScope(w, r, p)
	if !ok {
		return
	}
	var out any
	s.view(func(ix *index) {
		rec := ix.recs[id]
		switch {
		case rec == nil:
		case all:
			out = s.fullView(ix, rec)
		case eligible(p, rec):
			out = rec.public()
		}
	})
	if out == nil {
		v1.WriteError(w, v1.NotFound())
		return
	}
	v1.WriteJSON(w, http.StatusOK, out)
}

func (s *Store) handleBySlug(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	slug := r.PathValue("slug")
	if !validSlug(slug) {
		v1.WriteError(w, v1.NotFound())
		return
	}
	all, ok := s.readScope(w, r, p)
	if !ok {
		return
	}
	var out any
	var gone bool
	s.view(func(ix *index) {
		e, found := ix.slugs[slug]
		rec := ix.recs[e.ID]
		if !found || rec == nil {
			return
		}
		if !e.Canonical {
			if all || eligible(p, rec) || ((rec.Status == StatusTombstoned || rec.Status == StatusPending) && visible(p, rec)) {
				out = map[string]string{"redirect_slug": rec.Slug}
			}
			return
		}
		switch {
		case all:
			out = s.fullView(ix, rec)
		case eligible(p, rec):
			out = rec.public()
		case rec.Status == StatusPending && visible(p, rec):
			out = rec.stub()
		case rec.Status == StatusTombstoned && visible(p, rec):
			gone = true
		}
	})
	switch {
	case gone:
		v1.WriteError(w, errGone())
	case out == nil:
		v1.WriteError(w, v1.NotFound())
	default:
		v1.WriteJSON(w, http.StatusOK, out)
	}
}

type listItem struct {
	ID          string `json:"id"`
	Slug        string `json:"slug"`
	Title       string `json:"title,omitempty"`
	Vertical    string `json:"vertical,omitempty"`
	QualityTier string `json:"quality_tier,omitempty"`
	Promoted    bool   `json:"promoted"`
	Status      string `json:"status"`
	Version     int    `json:"version"`
	UpdatedAt   string `json:"updated_at,omitempty"`
	Readers7d   int    `json:"readers_7d"`
	Prelive     *bool  `json:"prelive,omitempty"`
}

type listQuery struct {
	order        string
	limit        int
	promoted     *bool
	vertical     string
	updatedSince time.Time
	statuses     map[string]bool
	prelive      *bool
	all          bool
}

type cursor struct {
	H string `json:"h"`
	S string `json:"s"`
	U string `json:"u,omitempty"`
	I string `json:"i,omitempty"`
	R int    `json:"r,omitempty"`
}

var listParams = set("cursor", "status", "promoted", "vertical", "updated_since", "prelive", "order", "limit")

func parseBool(v string) (*bool, bool) {
	switch v {
	case "true":
		t := true
		return &t, true
	case "false":
		f := false
		return &f, true
	}
	return nil, false
}

func parseList(r *http.Request, all bool) (listQuery, string, *v1.Error) {
	q := listQuery{order: "slug", limit: 100, all: all}
	vals := r.URL.Query()
	for k, v := range vals {
		if !listParams[k] || len(v) != 1 {
			return q, "", fail(errInvalidParam(k))
		}
	}
	if v := vals.Get("order"); v != "" {
		if v != "slug" && v != "updated_at" && v != "readers_7d" {
			return q, "", fail(errInvalidParam("order"))
		}
		q.order = v
	}
	if v := vals.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			return q, "", fail(errInvalidParam("limit"))
		}
		q.limit = n
	}
	if vals.Has("promoted") {
		b, ok := parseBool(vals.Get("promoted"))
		if !ok {
			return q, "", fail(errInvalidParam("promoted"))
		}
		q.promoted = b
	}
	if vals.Has("vertical") {
		if q.vertical = vals.Get("vertical"); !verticals[q.vertical] {
			return q, "", fail(errInvalidParam("vertical"))
		}
	}
	if vals.Has("updated_since") {
		t, err := time.Parse(time.RFC3339, vals.Get("updated_since"))
		if err != nil {
			return q, "", fail(errInvalidParam("updated_since"))
		}
		q.updatedSince = t
	}
	if vals.Has("status") {
		q.statuses = map[string]bool{}
		for _, st := range strings.Split(vals.Get("status"), ",") {
			switch st {
			case StatusPending, StatusPublished, StatusHeld, StatusRejected, StatusTombstoned:
			default:
				return q, "", fail(errInvalidParam("status"))
			}
			if !all && st != StatusPublished {
				return q, "", fail(errInvalidParam("status"))
			}
			q.statuses[st] = true
		}
	}
	if vals.Has("prelive") {
		b, ok := parseBool(vals.Get("prelive"))
		if !ok || !all {
			return q, "", fail(errInvalidParam("prelive"))
		}
		q.prelive = b
	}
	h := sha256.Sum256([]byte(strings.Join([]string{
		strconv.FormatBool(all), q.order, vals.Get("promoted"), q.vertical, vals.Get("updated_since"),
		vals.Get("status"), vals.Get("prelive"),
	}, "\x00")))
	return q, hex.EncodeToString(h[:8]), nil
}

func (q *listQuery) match(p v1.Principal, r *Record) bool {
	if q.all {
		if q.statuses != nil && !q.statuses[r.Status] {
			return false
		}
		if q.prelive != nil && r.Prelive != *q.prelive {
			return false
		}
	} else if !eligible(p, r) {
		return false
	}
	if q.promoted != nil && r.Promoted != *q.promoted {
		return false
	}
	if q.vertical != "" && r.Vertical != q.vertical {
		return false
	}
	if !q.updatedSince.IsZero() {
		t, err := time.Parse(time.RFC3339, r.UpdatedAt)
		if err != nil || t.Before(q.updatedSince) {
			return false
		}
	}
	return true
}

func (s *Store) handleList(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	all, ok := s.readScope(w, r, p)
	if !ok {
		return
	}
	q, hash, e := parseList(r, all)
	if e != nil {
		v1.WriteError(w, *e)
		return
	}
	var after *cursor
	if c := r.URL.Query().Get("cursor"); c != "" {
		b, err := base64.RawURLEncoding.DecodeString(c)
		var cur cursor
		if err != nil || json.Unmarshal(b, &cur) != nil || cur.H != hash {
			v1.WriteError(w, errInvalidCursor())
			return
		}
		after = &cur
	}
	now := s.now()
	type entry struct {
		rec     *Record
		readers int
	}
	var entries []entry
	s.view(func(ix *index) {
		for _, rec := range ix.recs {
			if q.match(p, rec) {
				entries = append(entries, entry{rec, s.counters.readers7d(rec.ID, now)})
			}
		}
	})
	less := func(a, b entry) int {
		switch q.order {
		case "updated_at":
			if c := strings.Compare(b.rec.UpdatedAt, a.rec.UpdatedAt); c != 0 {
				return c
			}
			return strings.Compare(a.rec.ID, b.rec.ID)
		case "readers_7d":
			if a.readers != b.readers {
				return b.readers - a.readers
			}
		}
		return strings.Compare(a.rec.Slug, b.rec.Slug)
	}
	slices.SortFunc(entries, less)
	start := 0
	if after != nil {
		probe := entry{rec: &Record{Slug: after.S, UpdatedAt: after.U, ID: after.I}, readers: after.R}
		start, _ = slices.BinarySearchFunc(entries, probe, less)
		for start < len(entries) && less(entries[start], probe) == 0 {
			start++
		}
	}
	end := min(start+q.limit, len(entries))
	items := make([]listItem, 0, end-start)
	for _, e := range entries[start:end] {
		it := listItem{ID: e.rec.ID, Slug: e.rec.Slug, Title: e.rec.Title, Vertical: e.rec.Vertical,
			QualityTier: e.rec.QualityTier, Promoted: e.rec.Promoted, Status: e.rec.Status, Version: e.rec.Version,
			UpdatedAt: e.rec.UpdatedAt, Readers7d: e.readers}
		if all {
			pl := e.rec.Prelive
			it.Prelive = &pl
		}
		items = append(items, it)
	}
	var next *string
	if end < len(entries) {
		last := entries[end-1]
		b, _ := json.Marshal(cursor{H: hash, S: last.rec.Slug, U: last.rec.UpdatedAt, I: last.rec.ID, R: last.readers})
		c := base64.RawURLEncoding.EncodeToString(b)
		next = &c
	}
	v1.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

type statsBody struct {
	Records struct {
		Pending    int `json:"pending"`
		Published  int `json:"published"`
		Held       int `json:"held"`
		Rejected   int `json:"rejected"`
		Tombstoned int `json:"tombstoned"`
	} `json:"records"`
	Residues struct {
		Tombstoned int `json:"tombstoned"`
		Rejected   int `json:"rejected"`
	} `json:"residues"`
	Fingerprints struct {
		Active     int `json:"active"`
		OtherModel int `json:"other_model"`
	} `json:"fingerprints"`
	PreliveRecords int            `json:"prelive_records"`
	PreliveKeys    int            `json:"prelive_keys"`
	OrphanKeys     int            `json:"orphan_keys"`
	MatrixRows     int            `json:"matrix_rows"`
	Golive         *goliveStats   `json:"golive"`
	Embedder       *embedderStats `json:"embedder"`
	Thresholds     struct {
		Covered float64 `json:"covered"`
		Related float64 `json:"related"`
	} `json:"thresholds"`
}

type goliveStats struct {
	At string `json:"at"`
}

type embedderStats struct {
	Model string `json:"model"`
	Dim   int    `json:"dim"`
}

func (s *Store) handleStats(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	if !allowed(w, r, v1.ScopeArticlesReadAll) {
		return
	}
	var st statsBody
	model, dim := s.embedderShape()
	err := s.scanSnapshot(func(pk parsedKey, val []byte, rec *Record) {
		switch pk.sub {
		case subMeta:
			var g goliveMeta
			if pk.suffix == "golive" && json.Unmarshal(val, &g) == nil {
				st.Golive = &goliveStats{g.At}
			}
			return
		case subRecord:
			if rec == nil {
				return
			}
			switch rec.Status {
			case StatusPending:
				st.Records.Pending++
			case StatusPublished:
				st.Records.Published++
			case StatusHeld:
				st.Records.Held++
			case StatusRejected:
				st.Records.Rejected++
			case StatusTombstoned:
				st.Records.Tombstoned++
			}
			if rec.Residue && rec.Status == StatusTombstoned {
				st.Residues.Tombstoned++
			} else if rec.Residue {
				st.Residues.Rejected++
			}
			if rec.Prelive {
				st.PreliveRecords++
			}
		case subFingerprint:
			if m, d, _, err := decodeFingerprint(val); rec != nil && err == nil && (s.emb == nil || (m == model && d == dim)) {
				st.Fingerprints.Active++
			} else if rec != nil {
				st.Fingerprints.OtherModel++
			}
		}
		switch {
		case rec == nil:
			st.OrphanKeys++
		case rec.Prelive:
			st.PreliveKeys++
		}
	})
	s.view(func(ix *index) { st.MatrixRows = ix.rowCount() })
	if err != nil {
		v1.WriteError(w, v1.InternalError())
		return
	}
	if s.emb != nil {
		st.Embedder = &embedderStats{s.emb.Model(), s.emb.Dim()}
	}
	th := s.th()
	st.Thresholds.Covered, st.Thresholds.Related = th.Covered, th.Related
	v1.WriteJSON(w, http.StatusOK, st)
}

type versionSummary struct {
	Version       int    `json:"version"`
	UpdatedAt     string `json:"updated_at"`
	QualityTier   string `json:"quality_tier"`
	ContentSHA256 string `json:"content_sha256"`
	Models        Models `json:"models"`
	Build         Build  `json:"build"`
}

func (s *Store) handleVersions(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	id := r.PathValue("id")
	if !validID(id) {
		v1.WriteError(w, v1.NotFound())
		return
	}
	if !allowed(w, r, v1.ScopeArticlesReadAll) {
		return
	}
	rec := s.record(id)
	if rec == nil {
		v1.WriteError(w, v1.NotFound())
		return
	}
	lower, upper := versionPrefix(id)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		v1.WriteError(w, v1.InternalError())
		return
	}
	defer it.Close()
	out := []versionSummary{}
	for ok := it.Last(); ok; ok = it.Prev() {
		v, err := decodeRecord(it.Value())
		if err != nil {
			continue
		}
		if len(v.Build.RetrievalParams) == 0 {
			v.Build.RetrievalParams = json.RawMessage("{}")
		}
		out = append(out, versionSummary{v.Version, v.UpdatedAt, v.QualityTier, v.ContentSHA256, v.Models, v.Build})
	}
	v1.WriteJSON(w, http.StatusOK, map[string]any{"current": rec.Version, "versions": out})
}

func (s *Store) handleVersion(w http.ResponseWriter, r *http.Request, p v1.Principal) {
	id := r.PathValue("id")
	n, err := strconv.Atoi(r.PathValue("version"))
	if !validID(id) || err != nil || n < 1 || strconv.Itoa(n) != r.PathValue("version") {
		v1.WriteError(w, v1.NotFound())
		return
	}
	if !allowed(w, r, v1.ScopeArticlesReadAll) {
		return
	}
	val, closer, err := s.db.Get(versionKey(id, n))
	if errors.Is(err, pebble.ErrNotFound) {
		v1.WriteError(w, v1.NotFound())
		return
	}
	if err != nil {
		v1.WriteError(w, v1.InternalError())
		return
	}
	defer closer.Close()
	v, err := decodeRecord(val)
	if err != nil {
		v1.WriteError(w, v1.InternalError())
		return
	}
	var out json.RawMessage
	s.view(func(ix *index) { out = s.fullView(ix, v) })
	v1.WriteJSON(w, http.StatusOK, out)
}
