// PebbleBM25 is a BM25 search implementation that reads/writes through
// PebbleStore. Mirrors the SQLite-backed BM25 above (same algorithm: k1=1.2,
// b=0.75, title-boost at index time, phrase filter at query time, doc-level
// max-passage aggregation handled by callers) but consumes the iter-201
// Pebble postings primitives instead of SQL joins.
//
// Iter 202 — third piece of the path-2 rework. The two BM25 implementations
// coexist; operators pick a backend via config. Behavioral parity is
// asserted via TestPebbleBM25MatchesSQLite, which runs the same corpus +
// queries through both and compares hit URLs.
package index

import (
	"context"
	"math"
	"sort"
	"strings"

	"github.com/calinteodor/cosift/internal/store"
)

// PebbleBM25 mirrors BM25 but reads from a PebbleStore.
type PebbleBM25 struct {
	store *store.PebbleStore
}

// NewPebbleBM25 returns a search-only handle over the given PebbleStore.
// Indexing happens via store.PebbleStore.IndexDocument (caller passes
// Tokenize + TitleBoost from this package).
func NewPebbleBM25(s *store.PebbleStore) *PebbleBM25 {
	return &PebbleBM25{store: s}
}

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

	// Per-doc accumulated BM25 score and doc-meta cache (URL/title) that
	// gets filled lazily as scored docs surface.
	scores := make(map[int64]float64)
	docLenCache := make(map[int64]int64)

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
			docLen, ok := docLenCache[p.DocID]
			if !ok {
				dl, found, err := b.store.GetDocLen(ctx, p.DocID)
				if err != nil || !found {
					return true // skip silently — doc was likely deleted between writes
				}
				docLen = dl
				docLenCache[p.DocID] = docLen
			}
			if docLen <= 0 {
				docLen = 1
			}
			tfF := float64(p.TF)
			lenNorm := 1.0 - B + B*(float64(docLen)/avgDocLen)
			score := idf * ((tfF * (K1 + 1.0)) / (tfF + K1*lenNorm))
			scores[p.DocID] += score
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	// Resolve URL/title for scored docs. One Get per doc — small at typical
	// k=10 scale; can batch with a multi-get if it becomes a hot spot.
	type docMeta struct {
		url, title string
	}
	metas := make(map[int64]docMeta, len(scores))
	for docID := range scores {
		d, err := b.store.GetDocByID(ctx, docID)
		if err != nil {
			// Doc rows can transiently vanish under heavy reindex traffic;
			// skip rather than fail the whole query.
			continue
		}
		metas[docID] = docMeta{url: d.URL, title: d.Title}
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

// corpusStats returns (totalDocs, avgDocLen). Currently scans the 'l' family
// on every query — acceptable at test scale; a follow-up iter persists a
// running average in the 'm' family to make this O(1).
func (b *PebbleBM25) corpusStats(ctx context.Context) (int64, float64, error) {
	stats, err := b.store.Stats(ctx)
	if err != nil {
		return 0, 0, err
	}
	if stats.Documents == 0 {
		return 0, 0, nil
	}
	totalLen, count, err := b.store.SumDocLengths(ctx)
	if err != nil {
		return 0, 0, err
	}
	if count == 0 {
		return stats.Documents, 1.0, nil
	}
	return stats.Documents, float64(totalLen) / float64(count), nil
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
