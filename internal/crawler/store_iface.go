// backend interfaces for the crawler so it can run against
// either *store.Store (SQLite, the production path through) or
// *store.PebbleStore (the path-2 rework target).
//
// Each Pebble equivalent of an SQLite method already exists with a
// matching signature; this file just names the contract.

package crawler

import (
	"context"

	"github.com/pilot-protocol/cosift/internal/store"
)

// CrawlerStore is the storage surface the crawler depends on. Both
// *store.Store and *store.PebbleStore satisfy it (compile-time asserted
// below) so a crawler can be built against either backend.
//
// Excludes UpsertPassage — vector-write semantics differ between the two
// backends (SQLite writes a passages table row; Pebble writes to an HNSW
// graph that lives in a different package). Vector writes are handled by
// the optional PassageWriter below, so the BM25-only crawl path is
// fully covered by this interface.
type CrawlerStore interface {
	// Frontier
	PushFrontier(ctx context.Context, url string, depth int, priority float64) error
	ClaimFrontier(ctx context.Context) (store.FrontierItem, bool, error)
	CompleteFrontier(ctx context.Context, url string) error
	FailFrontier(ctx context.Context, url, errMsg string) error
	RecoverInFlight(ctx context.Context) error
	RecrawlURL(ctx context.Context, url string) error
	GetFrontierStats(ctx context.Context) (store.FrontierStats, error)
	CountQueuedPerHost(ctx context.Context, hosts []string) (map[string]int, error)

	// Documents
	UpsertDocument(ctx context.Context, d *store.Document) (int64, error)
	GetDocByURL(ctx context.Context, url string) (*store.Document, error)
}

// LexicalIndexer abstracts the BM25 writer. Both *index.BM25 (SQLite) and
// *index.PebbleBM25 satisfy the single-method signature.
type LexicalIndexer interface {
	IndexDocument(ctx context.Context, docID int64, title, text string) error
}

// PassageWriter is the optional vector-write surface. *store.Store
// satisfies it via UpsertPassage; *store.PebbleStore does NOT (Pebble's
// vector path goes through index.HNSW.AddPassage + periodic Persist —
// a different shape than per-passage SQL rows).
//
// When non-nil on a Crawler, the embedding-enabled crawl path persists
// passage vectors via this interface. When nil, dense indexing during
// crawl is skipped silently (BM25 still works). Keeps the
// SQLite + Pebble paths cleanly separated without a stub no-op.
type PassageWriter interface {
	UpsertPassage(ctx context.Context, p *store.Passage) error
}

// PassageWriterBatch is an optional optimization: callers (the crawler)
// with all of a doc's chunks already in memory can hand them over in one
// call so the underlying writer can take the HNSW lock once.
type PassageWriterBatch interface {
	UpsertPassageBatch(ctx context.Context, ps []*store.Passage) error
}

// URLInvalidator is the optional zombie-reclaim surface. When a doc is
// re-crawled (URL already in famDoc), the old passage vectors persist in
// the HNSW graph alongside the new ones — same URL, multiple generations
// of chunks. The main Search dedupes by URL at the candidate level, but
// the dead nodes still steal ef-search slots and graph memory, dragging
// recall on bloated graphs. This interface lets the crawler ask the
// writer to mark all prior passages for a URL as invalid (vec=nil) right
// before the new chunk batch is inserted. Returns the count zeroed.
// Optional: writers without invalidation support are skipped silently.
type URLInvalidator interface {
	MarkURLInvalid(ctx context.Context, url string) (int, error)
}

// Compile-time interface satisfaction checks. Build fails fast if either
// backend drifts.
var (
	_ CrawlerStore  = (*store.Store)(nil)
	_ CrawlerStore  = (*store.PebbleStore)(nil)
	_ PassageWriter = (*store.Store)(nil)
)
