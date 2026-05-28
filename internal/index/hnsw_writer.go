// HNSW write bridge for the Pebble-backed crawler path.
//
// The PassageWriter interface lets the crawler stream passage
// vectors to whatever backend the operator wires. On SQLite, *store.Store
// satisfies it directly (writes go to the passages table). On Pebble there
// was no implementation — vector indexing during crawl was silently
// skipped, which made the Pebble path BM25-only.
//
// HNSWWriter bridges the gap: passages flow into an in-memory HNSW graph
// and get periodically flushed to the Iter PebbleStore via
// HNSW.Persist. Cheap per-passage cost (one graph insert + one
// GetDocMeta lookup); the heavy on-disk write is amortized across
// many inserts.
package index

import (
	"context"
	"fmt"
	"sync"

	"github.com/pilot-protocol/cosift/internal/store"
)

// HNSWWriter satisfies the crawler's PassageWriter interface against a
// PebbleStore + HNSW pair. Passage URLs/titles are resolved from the
// 'i' family (cheap, no full Document gob decode).
//
// Persistence is checkpointed every PersistEvery passages. Setting
// PersistEvery to 0 (the default if you don't configure it) means
// "never auto-persist" — callers can call Persist manually at safe
// points (e.g. crawler shutdown).
type HNSWWriter struct {
	hnsw         *HNSW
	store        *store.PebbleStore
	persistEvery int

	mu             sync.Mutex
	sinceLastFlush int
}

// NewHNSWWriter returns a PassageWriter that adds each incoming passage
// to the HNSW graph and persists to the PebbleStore every persistEvery
// passages. persistEvery=0 disables auto-persist (caller drives it via
// HNSWWriter.Flush).
func NewHNSWWriter(h *HNSW, ps *store.PebbleStore, persistEvery int) *HNSWWriter {
	return &HNSWWriter{hnsw: h, store: ps, persistEvery: persistEvery}
}

// UpsertPassage satisfies crawler.PassageWriter. Resolves the doc's URL +
// title via PebbleStore.GetDocMeta, adds the passage to HNSW, and
// triggers a checkpoint Persist when the threshold is reached.
//
// store.Passage carries Embedding + DocID + Offset/Length + Model;
// HNSW.AddPassage needs URL + title for hit-display.
func (w *HNSWWriter) UpsertPassage(ctx context.Context, p *store.Passage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	url, title, ok, err := w.store.GetDocMeta(ctx, p.DocID)
	if err != nil {
		return fmt.Errorf("lookup doc %d: %w", p.DocID, err)
	}
	if !ok {
		// Doc was deleted between Upsert and Passage write. Skip silently —
		// no point indexing a vector pointing at nothing.
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.hnsw.AddPassage(url, title, p.Offset, p.Length, p.Embedding)
	w.sinceLastFlush++
	if w.persistEvery > 0 && w.sinceLastFlush >= w.persistEvery {
		w.sinceLastFlush = 0
		// Persist holds the HNSW RLock; we release our own writer mutex
		// briefly first? Actually no — w.mu protects sinceLastFlush, not
		// HNSW. HNSW.Persist takes its own RLock independent of ours.
		if err := w.hnsw.Persist(ctx, w.store); err != nil {
			return fmt.Errorf("hnsw persist: %w", err)
		}
	}
	return nil
}

// MarkURLInvalid zeros out every HNSW node whose url matches. Returns
// the count zeroed. Lets the crawler tell us "this URL's prior passages
// are stale" before pushing the new chunk batch. Satisfies the optional
// crawler.URLInvalidator interface.
func (w *HNSWWriter) MarkURLInvalid(ctx context.Context, url string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return w.hnsw.MarkURLPassagesInvalid(url), nil
}

// Flush forces an immediate Persist of the current HNSW state. Crawler
// callers should invoke this at shutdown so the last partial batch of
// in-memory inserts isn't lost on next startup.
func (w *HNSWWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sinceLastFlush = 0
	return w.hnsw.Persist(ctx, w.store)
}
