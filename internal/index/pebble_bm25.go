// PebbleBM25 is a BM25 search implementation that reads/writes through
// PebbleStore. Mirrors the SQLite-backed BM25 above (same algorithm: k1=1.2,
// b=0.75, title-boost at index time, phrase filter at query time, doc-level
// max-passage aggregation handled by callers) but consumes the
// Pebble postings primitives instead of SQL joins.
//
// The two BM25 implementations
// coexist; operators pick a backend via config. Behavioral parity is
// asserted via TestPebbleBM25MatchesSQLite, which runs the same corpus +
// queries through both and compares hit URLs.

package index

import (
	"context"
	"math"
	"sort"
	"strings"

	"github.com/pilot-protocol/cosift/internal/store"
)

// PebbleBM25 mirrors BM25 but reads from a PebbleStore.
type PebbleBM25 struct {
	store *store.PebbleStore
	// Default to the package
	// constants (K1=1.2, B=0.75); operators tuning for a specific corpus
	// can override via WithBM25Params(). Length normalization (B) is the
	// knob most likely to want tuning — long-doc corpora often prefer
	// B≈0.5; short-doc corpora often prefer B=1.0.
	k1 float64
	b  float64
}

// NewPebbleBM25 returns a search-only handle over the given PebbleStore.
// Indexing happens via store.PebbleStore.IndexDocument (caller passes
// Tokenize + TitleBoost from this package).
func NewPebbleBM25(s *store.PebbleStore) *PebbleBM25 {
	return &PebbleBM25{store: s, k1: K1, b: B}
}

// WithBM25Params overrides the default k1 / b for this instance. Values ≤0
// are ignored (keep the package-constant default for that knob). Chainable.
func (b *PebbleBM25) WithBM25Params(k1, blen float64) *PebbleBM25 {
	if k1 > 0 {
		b.k1 = k1
	}
	if blen > 0 {
		b.b = blen
	}
	return b
}

// K1 returns the currently configured BM25 k1 parameter. Lets
// operator-visible endpoints surface 'what scoring params is this instance
// actually using' without grepping env or constants.
func (b *PebbleBM25) K1() float64 { return b.k1 }

// B returns the currently configured BM25 b parameter.
func (b *PebbleBM25) B() float64 { return b.b }

// IndexDocument is a convenience wrapper that calls PebbleStore.IndexDocument
// with this package's tokenizer + title-boost constant. Keeps callers from
// having to import internal/store just to plumb those two values.
func (b *PebbleBM25) IndexDocument(ctx context.Context, docID int64, title, text string) error {
	return b.store.IndexDocument(ctx, docID, title, text, Tokenize, TitleBoost)
}

// Search returns the top-k hits for q from the Pebble-backed index.
// Algorithm matches BM25.Search verbatim (k1, b, title-boost, phrase filter);
// only the storage backend differs.
func (b *PebbleBM25) Search(ctx context.Context, q string, k int) ([]Hit, error) {
	searchQ, phrases := parsePhrases(q)
	tokens := Tokenize(searchQ)
	if len(tokens) == 0 {
		return nil, nil
	}

	totalDocs, avgDocLen, err := b.corpusStats(ctx)
	if err != nil {
		return nil, err
	}
	if totalDocs == 0 {
		return nil, nil
	}
	if avgDocLen < 1 {
		avgDocLen = 1
	}

	// Per-doc accumulated BM25 score. docLenCache removed —
	// doc_len is now inline in each posting value (no per-doc Get).
	scores := make(map[int64]float64)

	uniqTerms := dedupeStrings(tokens)
	for _, term := range uniqTerms {
		info, ok, err := b.store.GetTermInfo(ctx, term)
		if err != nil {
			return nil, err
		}
		if !ok || info.DocFreq == 0 {
			continue
		}
		idf := math.Log(1.0 + (float64(totalDocs)-float64(info.DocFreq)+0.5)/(float64(info.DocFreq)+0.5))

		err = b.store.IteratePostings(ctx, info.ID, func(p store.PostingEntry) bool {
			// docLen is inline in the posting value — no separate
			// GetDocLen call. Removed ~25k Pebble Gets per query at N=10k.
			docLen := p.DocLen
			if docLen <= 0 {
				docLen = 1
			}
			tfF := float64(p.TF)
			lenNorm := 1.0 - b.b + b.b*(float64(docLen)/avgDocLen)
			score := idf * ((tfF * (b.k1 + 1.0)) / (tfF + b.k1*lenNorm))
			scores[p.DocID] += score
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	// Resolve URL/title for scored docs via the cheap 'i' side-blob
	// (single Get + varint decode per doc, no gob). At 10k docs this dropped
	// PebbleBM25 query latency from p50≈800ms to <p50=80ms — the full-Document
	// gob decode was 95% of the read cost.
	type docMeta struct {
		url, title string
	}
	metas := make(map[int64]docMeta, len(scores))
	for docID := range scores {
		url, title, ok, err := b.store.GetDocMeta(ctx, docID)
		if err != nil || !ok {
			continue
		}
		metas[docID] = docMeta{url: url, title: title}
	}

	hits := make([]Hit, 0, len(scores))
	for docID, sc := range scores {
		m := metas[docID]
		if m.url == "" {
			continue
		}
		hits = append(hits, Hit{DocID: docID, URL: m.url, Title: m.title, Score: sc})
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })

	if len(phrases) > 0 {
		hits = b.filterByPhrasesPebble(ctx, hits, phrases, k)
	} else if k > 0 && len(hits) > k {
		hits = hits[:k]
	}
	return hits, nil
}

// corpusStats returns (indexedDocs, avgDocLen). Reads the
// running (sum_doc_len, indexed_docs) counters maintained by
// PebbleStore.IndexDocument. O(1) per query; replaces the per-query
// O(N) scan over the 'l' family that the bench surfaced as
// PebbleBM25.Search's dominant cost.
func (b *PebbleBM25) corpusStats(ctx context.Context) (int64, float64, error) {
	sumLen, count, err := b.store.CorpusStats(ctx)
	if err != nil {
		return 0, 0, err
	}
	if count == 0 {
		return 0, 0, nil
	}
	return count, float64(sumLen) / float64(count), nil
}

// filterByPhrasesPebble walks sorted hits and keeps docs whose body OR title
// contains every phrase verbatim. Mirrors BM25.filterByPhrases but fetches
// text via PebbleStore.GetDocByID (one Get per candidate; phrase queries
// typically retain ≥50% of top candidates so chunked batching wouldn't help
// much at the k=10 scale).
func (b *PebbleBM25) filterByPhrasesPebble(ctx context.Context, hits []Hit, phrases []string, k int) []Hit {
	if k <= 0 {
		k = 10
	}
	out := make([]Hit, 0, k)
	for _, h := range hits {
		if len(out) >= k {
			break
		}
		d, err := b.store.GetDocByID(ctx, h.DocID)
		if err != nil {
			continue
		}
		body := strings.ToLower(d.Text)
		titleLC := strings.ToLower(h.Title)
		ok := true
		for _, p := range phrases {
			if !strings.Contains(body, p) && !strings.Contains(titleLC, p) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, h)
		}
	}
	return out
}
