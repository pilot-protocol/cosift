// Package articles is the engine's article store: the 'R' key family and the
// /v1 article routes. Its contract is ARTICLES.md in the private
// cosift-articles repository.
package articles

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/store"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

// Corpus answers whether a URL is a document in the search corpus.
type Corpus interface {
	HasDocument(ctx context.Context, url string) (bool, error)
}

// StoreCorpus adapts the document store to Corpus.
type StoreCorpus struct{ Store *store.PebbleStore }

func (c StoreCorpus) HasDocument(ctx context.Context, url string) (bool, error) {
	_, err := c.Store.GetDocByURL(ctx, url)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

type Options struct {
	DB *pebble.DB
	// Embedder must be the uncached client, never embed.CachedEmbedder.
	Embedder       embed.Embedder
	Policy         v1.Policy
	Corpus         Corpus
	ThresholdsPath string
	Logf           func(format string, args ...any)
}

// Store serves the article family. Mutations serialise on writeMu; reads use
// the in-memory index under mu, which only writers holding writeMu change.
type Store struct {
	db             *pebble.DB
	emb            embed.Embedder
	policy         v1.Policy
	corpus         Corpus
	thresholdsPath string
	thresholds     atomic.Pointer[Thresholds]
	rd             v1.Readiness
	logf           func(format string, args ...any)

	writeMu sync.Mutex
	closed  bool

	mu  sync.RWMutex
	idx *index

	counters *counters

	jobs       sync.WaitGroup
	stop       chan struct{}
	closeOnce  sync.Once
	bgCtx      context.Context
	bgCancel   context.CancelFunc
	compacting bool
	compacted  atomic.Int64
	recompact  bool

	now          func() time.Time
	flushEvery   time.Duration
	matchTimeout time.Duration
	putTimeout   time.Duration
	rebuildHook  func()
	reembedBase  time.Duration
}

type slugEntry struct {
	ID        string `json:"article_id"`
	Canonical bool   `json:"canonical"`
}

type goliveMeta struct {
	At      string `json:"at"`
	By      string `json:"by"`
	Records int    `json:"records"`
}

type index struct {
	recs      map[string]*Record
	slugs     map[string]slugEntry
	claims    map[string]string
	rows      map[string][]row
	fps       map[string][]float32
	fpOther   map[string]bool
	fullSlugs map[string]string
	golive    *goliveMeta
}

func newIndex() *index {
	return &index{
		recs:      map[string]*Record{},
		slugs:     map[string]slugEntry{},
		claims:    map[string]string{},
		rows:      map[string][]row{},
		fps:       map[string][]float32{},
		fpOther:   map[string]bool{},
		fullSlugs: map[string]string{},
	}
}

func (ix *index) putRecord(r *Record) {
	ix.recs[r.ID] = r
	if r.Title != "" {
		ix.fullSlugs[r.ID] = fullSlug(r.Title)
	} else {
		delete(ix.fullSlugs, r.ID)
	}
}

func (ix *index) dropRecord(id string) {
	delete(ix.recs, id)
	delete(ix.rows, id)
	delete(ix.fps, id)
	delete(ix.fpOther, id)
	delete(ix.fullSlugs, id)
	for s, e := range ix.slugs {
		if e.ID == id {
			delete(ix.slugs, s)
		}
	}
	for t, owner := range ix.claims {
		if owner == id {
			delete(ix.claims, t)
		}
	}
}

func (ix *index) rowCount() int {
	n := 0
	for _, rs := range ix.rows {
		n += len(rs)
	}
	return n
}

// hasFingerprint reports a fingerprint of the running model, the only kind compared.
func (ix *index) hasFingerprint(id string) bool {
	_, ok := ix.fps[id]
	return ok
}

// anyFingerprint also counts a fingerprint of another model.
func (ix *index) anyFingerprint(id string) bool { return ix.hasFingerprint(id) || ix.fpOther[id] }

// Open prepares a store over db. Call Rebuild before the routes can serve.
func Open(opts Options) (*Store, error) {
	if opts.DB == nil || opts.Policy == nil {
		return nil, errors.New("articles: DB and Policy are required")
	}
	if _, cached := opts.Embedder.(*embed.CachedEmbedder); cached {
		return nil, errors.New("articles: the embedder must be uncached")
	}
	s := &Store{
		db:             opts.DB,
		emb:            opts.Embedder,
		policy:         opts.Policy,
		corpus:         opts.Corpus,
		thresholdsPath: opts.ThresholdsPath,
		logf:           opts.Logf,
		idx:            newIndex(),
		counters:       newCounters(),
		stop:           make(chan struct{}),
		now:            time.Now,
		flushEvery:     5 * time.Minute,
		matchTimeout:   time.Second,
		putTimeout:     20 * time.Second,
		reembedBase:    time.Second,
	}
	s.bgCtx, s.bgCancel = context.WithCancel(context.Background())
	if s.thresholdsPath == "" {
		s.thresholdsPath = DefaultThresholdsPath
	}
	if s.logf == nil {
		s.logf = log.Printf
	}
	t := DefaultThresholds
	s.thresholds.Store(&t)
	if err := s.ReloadThresholds(); err != nil {
		s.logf("articles: ERROR %s invalid (%v) — using the defaults", s.thresholdsPath, err)
	}
	return s, nil
}

// ReloadThresholds re-reads the thresholds file; an invalid file keeps the previous values.
func (s *Store) ReloadThresholds() error {
	t, err := LoadThresholds(s.thresholdsPath)
	if err != nil {
		return err
	}
	s.thresholds.Store(&t)
	return nil
}

func (s *Store) th() Thresholds { return *s.thresholds.Load() }

// Readiness reports whether the startup rebuild has finished.
func (s *Store) Readiness() *v1.Readiness { return &s.rd }

// WriteLocker is the store-level write lock, for the policy's config swap.
func (s *Store) WriteLocker() sync.Locker { return &s.writeMu }

func (s *Store) nowString() string { return s.now().UTC().Format(time.RFC3339) }

// record returns the current record for id; records are never mutated in place.
func (s *Store) record(id string) *Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.idx.recs[id]
}

// view runs fn on the index under the read lock.
func (s *Store) view(fn func(ix *index)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn(s.idx)
}

// update runs fn on the index under the index write lock; callers hold writeMu.
func (s *Store) update(fn func(ix *index)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.idx)
}

func (s *Store) golive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.idx.golive != nil
}

// Rebuild loads the index from Pebble and marks the store ready.
func (s *Store) Rebuild(ctx context.Context) error {
	if s.rebuildHook != nil {
		s.rebuildHook()
	}
	ix := newIndex()
	lower, upper := familyBounds()
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	var reembed []string
	var excluded, bad int
	model, dim := s.embedderShape()
	counts := map[string]map[string]int{}
	for ok := it.First(); ok; ok = it.Next() {
		if err := ctx.Err(); err != nil {
			_ = it.Close()
			return err
		}
		pk, valid := parseKey(it.Key())
		if !valid {
			bad++
			continue
		}
		val := it.Value()
		switch pk.sub {
		case subRecord:
			r, err := decodeRecord(val)
			if err != nil || r.ID != pk.suffix {
				bad++
				continue
			}
			ix.putRecord(r)
		case subSlug:
			var e slugEntry
			if json.Unmarshal(val, &e) != nil {
				bad++
				continue
			}
			ix.slugs[pk.suffix] = e
		case subTopic:
			ix.claims[pk.suffix] = string(val)
		case subEmbedding:
			m, d, rows, err := decodeEmbeddings(val)
			if err != nil {
				bad++
				continue
			}
			if s.emb != nil && (m != model || d != dim) {
				excluded += len(rows)
				reembed = append(reembed, pk.suffix)
				continue
			}
			ix.rows[pk.suffix] = rows
		case subFingerprint:
			m, d, v, err := decodeFingerprint(val)
			if err != nil {
				bad++
				continue
			}
			if s.emb != nil && (m != model || d != dim) {
				ix.fpOther[pk.suffix] = true
				continue
			}
			ix.fps[pk.suffix] = v
		case subCounters:
			var c countersJSON
			if json.Unmarshal(val, &c) == nil {
				counts[pk.suffix] = c.Days
			}
		case subMeta:
			if pk.suffix == "golive" {
				var g goliveMeta
				if json.Unmarshal(val, &g) == nil {
					ix.golive = &g
				}
			}
		}
	}
	if err := it.Close(); err != nil {
		return err
	}
	for id := range counts {
		if r, ok := ix.recs[id]; !ok || r.Residue {
			delete(counts, id)
		}
	}
	for id := range ix.rows {
		if r, ok := ix.recs[id]; !ok || r.Residue {
			delete(ix.rows, id)
		}
	}
	reembed = slices.DeleteFunc(reembed, func(id string) bool {
		r, ok := ix.recs[id]
		return !ok || r.Residue || r.Status == StatusPending
	})
	if _, closer, err := s.db.Get(metaKey("schema")); errors.Is(err, pebble.ErrNotFound) {
		if err := s.db.Set(metaKey("schema"), []byte(`{"articles":1}`), pebble.Sync); err != nil {
			return err
		}
	} else if err == nil {
		closer.Close()
	} else {
		return err
	}
	// No mutation runs before SetReady, so only the swap below needs the write lock.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.counters.load(counts, s.now())
	s.update(func(cur *index) { s.idx = ix })
	s.logf("articles: index ready: records=%d rows=%d fingerprints=%d skipped_keys=%d", len(ix.recs), ix.rowCount(), len(ix.fps)+len(ix.fpOther), bad)
	if excluded > 0 {
		s.logf("articles: %d rows of another embedding model excluded; re-embedding %d records", excluded, len(reembed))
	}
	s.rd.SetReady()
	s.startJob(s.flushLoop)
	if len(reembed) > 0 {
		s.startJob(func() { s.reembed(reembed) })
	}
	return nil
}

func (s *Store) embedderShape() (string, int) {
	if s.emb == nil {
		return "", 0
	}
	return s.emb.Model(), s.emb.Dim()
}

// startJob runs fn as a tracked background job; callers hold writeMu.
func (s *Store) startJob(fn func()) {
	if s.closed {
		return
	}
	s.jobs.Add(1)
	go func() {
		defer s.jobs.Done()
		fn()
	}()
}

// Close stops background work, flushes the read counters and refuses later
// writes. Call it after the listeners stop and before the Pebble store closes.
func (s *Store) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.writeMu.Lock()
		s.closed = true
		s.writeMu.Unlock()
		close(s.stop)
		s.bgCancel()
		s.jobs.Wait()
		s.writeMu.Lock()
		err = s.flushLocked()
		s.writeMu.Unlock()
	})
	return err
}

func (s *Store) flushLoop() {
	t := time.NewTicker(s.flushEvery)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.writeMu.Lock()
			if !s.closed {
				if err := s.flushLocked(); err != nil {
					s.logf("articles: counter flush failed: %v", err)
				}
			}
			s.writeMu.Unlock()
		}
	}
}

// scheduleCompact compacts the 'R' range in the background so erased values
// leave the SSTables; callers hold writeMu.
func (s *Store) scheduleCompact() {
	if s.compacting {
		s.recompact = true
		return
	}
	if s.closed {
		return
	}
	s.compacting = true
	s.startJob(func() {
		for {
			lower, upper := familyBounds()
			if err := s.db.Compact(lower, upper, true); err != nil {
				s.logf("articles: compaction failed: %v", err)
			} else {
				s.compacted.Add(1)
			}
			s.writeMu.Lock()
			again := s.recompact && !s.closed
			s.recompact = false
			if !again {
				s.compacting = false
				s.writeMu.Unlock()
				return
			}
			s.writeMu.Unlock()
		}
	})
}

// reembedMax bounds the back-off between re-embed attempts.
const reembedMax = 5 * time.Minute

// needsReembed returns a record that still has no rows of the running model.
func (s *Store) needsReembed(id string) *Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.idx.recs[id]
	if r == nil || r.Residue || r.Status == StatusPending || s.idx.rows[id] != nil {
		return nil
	}
	return r
}

// reembed re-embeds rows of another model, backing off until each record has current rows.
func (s *Store) reembed(ids []string) {
	done, wait := 0, s.reembedBase
	for pending := ids; len(pending) > 0; {
		var retry []string
		for _, id := range pending {
			if s.bgCtx.Err() != nil {
				return
			}
			rec := s.needsReembed(id)
			if rec == nil {
				continue
			}
			texts := vectorTexts(rec)
			vecs, err := s.embed(s.bgCtx, s.putTimeout, texts)
			if err != nil {
				retry = append(retry, id)
				continue
			}
			s.writeMu.Lock()
			if cur := s.needsReembed(id); !s.closed && cur != nil && slices.Equal(vectorTexts(cur), texts) {
				rows := rowsFor(cur, vecs)
				model, dim := s.embedderShape()
				if err := s.db.Set(embeddingKey(id), encodeEmbeddings(model, dim, rows), pebble.Sync); err == nil {
					s.update(func(ix *index) { ix.rows[id] = rows })
					done++
				} else {
					retry = append(retry, id)
				}
			} else if cur != nil {
				retry = append(retry, id)
			}
			s.writeMu.Unlock()
		}
		if len(retry) == 0 {
			break
		}
		s.logf("articles: re-embed: embedder_unavailable; %d records pending, retry in %s", len(retry), wait)
		select {
		case <-s.bgCtx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(2*wait, reembedMax)
		pending = retry
	}
	s.logf("articles: re-embedded %d records", done)
}

// vectorTexts lists the texts an article's rows embed: title, lead, aliases.
func vectorTexts(r *Record) []string {
	return append([]string{r.Title, r.Lead}, r.Aliases...)
}

func rowsFor(r *Record, vecs [][]float32) []row {
	rows := []row{{kind: kindTitle, vec: vecs[0]}, {kind: kindLead, vec: vecs[1]}}
	for i := range r.Aliases {
		rows = append(rows, row{kind: kindAlias, alias: i, vec: vecs[2+i]})
	}
	return rows
}

var errNoEmbedder = errors.New("no embedder")

// embed embeds texts with the uncached embedder and returns unit vectors,
// giving up at the deadline even when the embedder does not.
func (s *Store) embed(ctx context.Context, timeout time.Duration, texts []string) ([][]float32, error) {
	if s.emb == nil {
		return nil, errNoEmbedder
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	type result struct {
		v   [][]float32
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := s.emb.Embed(ctx, texts)
		ch <- result{v, err}
	}()
	var res result
	select {
	case res = <-ch:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if res.err != nil {
		return nil, res.err
	}
	if len(res.v) != len(texts) {
		return nil, fmt.Errorf("embedder returned %d vectors for %d texts", len(res.v), len(texts))
	}
	dim := s.emb.Dim()
	out := make([][]float32, len(res.v))
	for i, v := range res.v {
		if len(v) == 0 || (dim > 0 && len(v) != dim) {
			return nil, errors.New("embedder returned a vector of the wrong dimension")
		}
		c := slices.Clone(v)
		if err := normalize(c); err != nil {
			return nil, err
		}
		out[i] = c
	}
	return out, nil
}

// commit writes b with Sync, whatever the store's crawl write options are.
func (s *Store) commit(b *pebble.Batch) error {
	defer b.Close()
	return b.Commit(pebble.Sync)
}
