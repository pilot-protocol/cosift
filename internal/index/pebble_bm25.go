// PebbleBM25 is a BM25 search implementation that reads/writes through
// PebbleStore. Mirrors the SQLite-backed BM25 above (same scoring: k1=1.2,
// b=0.75, title-boost at index time, phrase filter at query time, doc-level
// max-passage aggregation handled by callers) but consumes the
// Pebble postings primitives instead of SQL joins.
//
// The two BM25 implementations
// coexist; operators pick a backend via config. Behavioral parity is
// asserted via TestPebbleBM25MatchesSQLite, which runs the same corpus +
// queries through both and compares hit URLs. This backend additionally
// carries large-corpus optimizations the SQLite twin doesn't need or have:
// IDF stopword pruning, MaxScore early termination, and top-k pool
// resolution (metadata is resolved for a bounded score-selected pool, not
// every scored candidate — see resolveTopKPool).

package index

import (
	"context"
	"errors"
	"log"
	"math"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/pilot-protocol/cosift/internal/authority"
	"github.com/pilot-protocol/cosift/internal/store"
)

// bm25MinIDF is the IDF floor below which a query term is treated as a
// stopword and skipped. Default 0.5 catches terms in >60% of the corpus
// ("the", "is", "of"). Operators can tune via COSIFT_BM25_MIN_IDF; set
// to 0 to disable pruning entirely (backward-compat for benchmark runs).
func bm25MinIDF() float64 {
	if v := os.Getenv("COSIFT_BM25_MIN_IDF"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			return f
		}
	}
	return 0.5
}

// bm25TopKPoolFactor sizes the metadata-resolution pool at factor*k
// candidates. Tune via COSIFT_BM25_TOPK_POOL_FACTOR (min 1); raise it if
// the pool-cap log line fires on ranking-sensitive traffic.
func bm25TopKPoolFactor() int {
	if v := os.Getenv("COSIFT_BM25_TOPK_POOL_FACTOR"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			return n
		}
	}
	return 50
}

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

	// Per-host authority multiplier applied at hit-resolution time.
	// Wikipedia / kernel.org / arxiv float above the spam directories
	// in the long tail without requiring a separate ranking pass. Nil
	// = passthrough (multiplier = 1.0 for every hit).
	authority *authority.Scorer

	// boostIDs maps docID → score multiplier. Applied post-scoring so
	// site-filtered queries can surface small-site docs that BM25 ranks
	// below top-k globally. Created via WithBoost (returns a shallow copy);
	// nil = no boost.
	boostIDs map[int64]float64
}

// NewPebbleBM25 returns a search-only handle over the given PebbleStore.
// Indexing happens via store.PebbleStore.IndexDocument (caller passes
// Tokenize + TitleBoost from this package).
func NewPebbleBM25(s *store.PebbleStore) *PebbleBM25 {
	return &PebbleBM25{store: s, k1: K1, b: B}
}

// WithAuthority attaches an authority Scorer; hits get their score
// multiplied by Scorer.Multiplier(host) before final ranking. Pass nil
// to disable. Chainable.
func (b *PebbleBM25) WithAuthority(a *authority.Scorer) *PebbleBM25 {
	b.authority = a
	return b
}

// WithBoost returns a shallow copy of b with the given docID→multiplier map
// applied post-scoring. Use this for site= queries: enumerate the site's
// docIDs, pass a 50× multiplier, and the site's docs will always appear in
// top-k even when they rank outside the global top-k on raw BM25 score.
func (b *PebbleBM25) WithBoost(ids map[int64]float64) *PebbleBM25 {
	c := *b
	c.boostIDs = ids
	return &c
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

// IndexDocumentBulk is IndexDocument without the host-partition write — the
// fast path for bulk WET ingest. See PebbleStore.IndexDocumentBulk.
func (b *PebbleBM25) IndexDocumentBulk(ctx context.Context, docID int64, title, text string) error {
	return b.store.IndexDocumentBulk(ctx, docID, title, text, Tokenize, TitleBoost)
}

// Search returns the top-k hits for q from the Pebble-backed index.
// Scoring matches BM25.Search (k1, b, title-boost, phrase filter), but this
// backend adds large-corpus optimizations the SQLite twin doesn't carry:
// IDF stopword pruning, MaxScore early termination, and top-k pool
// metadata resolution (resolveTopKPool).
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

	// Collect (term, idf, info) tuples first so we can drop high-DF stopwords
	// before scanning postings. At 6M+-doc corpora the killer terms ("what",
	// "is", "the") account for nearly all BM25 latency without contributing
	// to ranking. Lossless on the kept terms; safety: if every term gets
	// pruned we fall back to the single highest-IDF candidate so a query
	// like "what is the" still returns something.
	minIDF := bm25MinIDF()
	type termPost struct {
		term string
		idf  float64
		info store.TermInfo
	}
	uniqTerms := dedupeStrings(tokens)
	candidates := make([]termPost, 0, len(uniqTerms))
	for _, term := range uniqTerms {
		info, ok, err := b.store.GetTermInfo(ctx, term)
		if err != nil {
			return nil, err
		}
		if !ok || info.DocFreq == 0 {
			continue
		}
		idf := math.Log(1.0 + (float64(totalDocs)-float64(info.DocFreq)+0.5)/(float64(info.DocFreq)+0.5))
		candidates = append(candidates, termPost{term: term, idf: idf, info: info})
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	// Sort descending by IDF so the fallback below picks the rarest term.
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].idf > candidates[j].idf })
	active := candidates[:0]
	for _, c := range candidates {
		if c.idf >= minIDF {
			active = append(active, c)
		}
	}
	if len(active) == 0 {
		// Every term was a stopword. Keep the top-IDF candidate so we
		// surface SOMETHING relevant rather than 0 hits.
		active = candidates[:1]
	}

	// MaxScore-style early termination. Walk terms in descending IDF order
	// (most informative first), accumulating partial scores. After each
	// term, check whether the remaining terms' max-possible contributions
	// can still push an unseen doc into top-K — if not, break.
	//
	// The BM25 contribution of one term to any doc is bounded above by
	// idf*(k1+1) — saturates as tf grows relative to docLen. Sum of these
	// upper bounds across remaining terms = the maximum score a doc not
	// yet seen can ever accumulate. If that sum drops below the current
	// K-th best partial score, no future doc can enter top-K. Top-K
	// membership stays lossless; in-top-K ranking can shift (the rerank
	// pipeline re-orders anyway, and the score-decay/MMR stages tolerate
	// approximate partial scores). COSIFT_BM25_DISABLE_MAXSCORE=1 disables
	// the optimization for benchmark-grade lossless ranking.
	maxScoreEnabled := os.Getenv("COSIFT_BM25_DISABLE_MAXSCORE") == ""
	remainingMax := 0.0
	if maxScoreEnabled {
		for _, c := range active {
			remainingMax += c.idf * (b.k1 + 1.0)
		}
	}
	for i, c := range active {
		// Pre-scan MaxScore check: decide whether term i (and everything
		// after it, since active is sorted descending by IDF) can still push
		// an unseen doc into top-K BEFORE paying to scan term i's postings.
		// remainingMax here is the sum of max contributions of terms i..end
		// (idf*(k1+1) each). A doc not yet in `scores` can gain at most
		// remainingMax from the un-scanned terms; if that's below the current
		// K-th best score, scanning i..end can only reorder within top-K —
		// which the reranker fixes — so stop. This catches the common-term
		// last-term full scan the old post-scan i==len-1 guard always paid.
		if maxScoreEnabled && i > 0 && len(scores) >= k {
			theta := kthLargest(scores, k)
			if remainingMax < theta {
				break
			}
		}
		err := b.store.IteratePostings(ctx, c.info.ID, func(p store.PostingEntry) bool {
			// docLen is inline in the posting value — no separate
			// GetDocLen call. Removed ~25k Pebble Gets per query at N=10k.
			docLen := p.DocLen
			if docLen <= 0 {
				docLen = 1
			}
			tfF := float64(p.TF)
			lenNorm := 1.0 - b.b + b.b*(float64(docLen)/avgDocLen)
			score := c.idf * ((tfF * (b.k1 + 1.0)) / (tfF + b.k1*lenNorm))
			scores[p.DocID] += score
			return true
		})
		if err != nil {
			return nil, err
		}
		// Consume term i's max contribution now that it's been scanned, so
		// remainingMax entering the next iteration covers only un-scanned terms.
		if maxScoreEnabled {
			remainingMax -= c.idf * (b.k1 + 1.0)
		}
	}

	// Apply per-doc boosts (e.g. site= queries) before sorting. Boosted docs
	// already present in `scores` (i.e. they matched ≥1 query term) get the
	// full multiplier. Boosted docs with zero term overlap are absent from
	// `scores`; for small boost sets we seed them with a tiny base score
	// (boostSeedBase) before multiplying so they still enter the candidate
	// pool — landing at boostSeedBase*mult (e.g. 0.05), below any genuine
	// single-term BM25 match but enough for the reranker to judge them. This
	// also closes the MaxScore early-termination gap: a boosted doc whose only
	// matching posting list was skipped still gets seeded and surfaced.
	//
	// Seeding is gated by boostSeedMaxIDs: large boost sets (big sites) would
	// inject a GetDocMeta per zero-overlap doc and flood the pool, so above
	// the threshold we keep the old present-only behavior.
	seedZeroOverlap := len(b.boostIDs) > 0 && len(b.boostIDs) <= boostSeedMaxIDs
	for id, mult := range b.boostIDs {
		if cur, ok := scores[id]; ok {
			scores[id] = cur * mult
		} else if seedZeroOverlap {
			scores[id] = boostSeedBase * mult
		}
	}

	// Resolve URL/title via the cheap 'i' side-blob (single Get + varint
	// decode per doc, no gob). The pool path bounds how many docs get
	// resolved; COSIFT_BM25_DISABLE_TOPK_POOL restores the resolve-all
	// path (also taken for k<=0 = "return everything").
	if k > 0 && os.Getenv("COSIFT_BM25_DISABLE_TOPK_POOL") == "" {
		return b.resolveTopKPool(ctx, scores, phrases, k)
	}

	hits := make([]Hit, 0, len(scores))
	for docID, sc := range scores {
		url, title, ok, err := b.store.GetDocMeta(ctx, docID)
		if err != nil || !ok || url == "" {
			continue
		}
		// Apply the per-host authority multiplier. Wikipedia /
		// kernel.org / arxiv float over the spam directories that
		// share the same BM25 lexical score.
		if b.authority != nil {
			sc *= b.authority.Multiplier(hostFromURL(url))
		}
		hits = append(hits, Hit{DocID: docID, URL: url, Title: title, Score: sc})
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })

	if len(phrases) > 0 {
		hits = b.filterByPhrasesPebble(ctx, hits, phrases, k)
	} else if k > 0 && len(hits) > k {
		hits = hits[:k]
	}
	return hits, nil
}

const (
	boostSeedBase   = 0.001
	boostSeedMaxIDs = 256
)

// topKResolveSlack pads the initial resolve depth past k so occasional
// missing-meta drops (orphaned postings after SoftDeleteDocument) don't
// force an immediate expansion round.
const topKResolveSlack = 16

// resolveTopKPool is the bounded replacement for resolve-all: heap-select
// the top factor*k candidates by accumulated raw score, resolve metadata +
// authority for only the prefix that can still reach the final top-k, and
// expand (doubling, then falling back to the full candidate set) only when
// the phrase filter or missing-meta drops leave fewer than k hits.
//
// Lossless vs the resolve-all path whenever the pool cap doesn't bind:
// authority multipliers are >= 1 and <= 1+alpha, so a candidate with
// raw*(1+alpha) below the k-th raw score can never displace the top-k. When
// the cap does bind (more than factor*k candidates inside that band) the
// truncation is logged — the operator signal to raise the factor.
func (b *PebbleBM25) resolveTopKPool(ctx context.Context, scores map[int64]float64, phrases []string, k int) ([]Hit, error) {
	maxMult := 1.0
	if b.authority != nil {
		maxMult += b.authority.Alpha()
	}

	poolCap := bm25TopKPoolFactor() * k
	pool := topCandidates(scores, poolCap)
	if len(pool) == 0 {
		return nil, nil
	}
	poolCoversAll := len(pool) == len(scores)

	kIdx := k
	if kIdx > len(pool) {
		kIdx = len(pool)
	}
	kthRaw := pool[kIdx-1].score

	resolved := make([]Hit, 0, kIdx+topKResolveSlack)
	seen := make(map[int64]struct{}, kIdx+topKResolveSlack)
	resolve := func(cands []scoredCand) {
		for _, c := range cands {
			if _, dup := seen[c.docID]; dup {
				continue
			}
			seen[c.docID] = struct{}{}
			url, title, ok, err := b.store.GetDocMeta(ctx, c.docID)
			if err != nil || !ok || url == "" {
				continue
			}
			sc := c.score
			if b.authority != nil {
				sc *= b.authority.Multiplier(hostFromURL(url))
			}
			resolved = append(resolved, Hit{DocID: c.docID, URL: url, Title: title, Score: sc})
		}
	}

	// Seeded site= boost docs sit at ~boostSeedBase*mult, far below any
	// genuine match — a raw-score pool would evict them, so give the (small,
	// gated by boostSeedMaxIDs) boost set unconditional resolution.
	if n := len(b.boostIDs); n > 0 && n <= boostSeedMaxIDs && !poolCoversAll {
		boosted := make([]scoredCand, 0, n)
		for id := range b.boostIDs {
			if sc, ok := scores[id]; ok {
				boosted = append(boosted, scoredCand{docID: id, score: sc})
			}
		}
		resolve(boosted)
	}

	// Initial depth: every candidate whose ceiling raw*maxMult still clears
	// the k-th raw score, floored at k+slack. Beyond that index nothing can
	// enter the top-k, however authority reorders.
	limit := sort.Search(len(pool), func(i int) bool { return pool[i].score*maxMult < kthRaw })
	if floor := kIdx + topKResolveSlack; limit < floor {
		limit = floor
	}
	if limit > len(pool) {
		limit = len(pool)
	}

	next := 0
	var phraseMemo map[int64]bool
	if len(phrases) > 0 {
		phraseMemo = make(map[int64]bool)
	}
	for {
		resolve(pool[next:limit])
		next = limit
		sort.Slice(resolved, func(i, j int) bool { return resolved[i].Score > resolved[j].Score })

		out := resolved
		if len(phrases) > 0 {
			out = b.filterByPhrasesMemo(ctx, resolved, phrases, k, phraseMemo)
		} else if len(out) > k {
			out = out[:k]
		}

		exhausted := limit >= len(pool) && poolCoversAll
		if exhausted {
			return out, nil
		}
		if len(out) >= k {
			// Unresolved candidates all have raw <= pool[limit] (or the
			// pool floor when the cap bound the pool); if their ceiling
			// can't beat our k-th final score the result is exact.
			nextRaw := pool[len(pool)-1].score
			if limit < len(pool) {
				nextRaw = pool[limit].score
			}
			if nextRaw*maxMult < out[len(out)-1].Score {
				return out, nil
			}
			if limit >= len(pool) {
				// Possible displacers exist beyond the selected pool: the
				// cap bound the exact-safe set. Accept the approximation
				// (never dig past the cap just to refine an already-full
				// result) but say so.
				inBand := 0
				for _, sc := range scores {
					if sc*maxMult >= kthRaw {
						inBand++
					}
				}
				log.Printf("PebbleBM25: top-k pool cap bound (pool=%d resolved=%d in-band=%d k=%d) — results approximate; raise COSIFT_BM25_TOPK_POOL_FACTOR if ranking-sensitive", len(pool), next, inBand, k)
				return out, nil
			}
			// Headroom left inside the pool — resolve deeper before
			// settling (phrase matches or authority risers may be there).
		}

		// Dig deeper. Only a shortfall (selective phrase or meta drops —
		// fewer than k hits) may spill past the pool cap into the full
		// candidate set, matching the resolve-all path's unbounded phrase
		// depth at worst; refinement of a full result stays pool-bounded.
		limit *= 2
		if limit > len(pool) {
			if len(out) < k && !poolCoversAll {
				pool = topCandidates(scores, len(scores))
				poolCoversAll = true
			}
			if limit > len(pool) {
				limit = len(pool)
			}
		}
	}
}

// filterByPhrasesMemo is filterByPhrasesPebble with a docID->match memo so
// the pool expansion loop never fetches or scans a doc's text twice.
func (b *PebbleBM25) filterByPhrasesMemo(ctx context.Context, hits []Hit, phrases []string, k int, memo map[int64]bool) []Hit {
	if k <= 0 {
		k = 10
	}
	out := make([]Hit, 0, k)
	for _, h := range hits {
		if len(out) >= k {
			break
		}
		match, ok := memo[h.DocID]
		if !ok {
			match = false
			if d, err := b.store.GetDocByID(ctx, h.DocID); err == nil {
				body := strings.ToLower(d.Text)
				titleLC := strings.ToLower(h.Title)
				match = true
				for _, p := range phrases {
					if !strings.Contains(body, p) && !strings.Contains(titleLC, p) {
						match = false
						break
					}
				}
			}
			memo[h.DocID] = match
		}
		if match {
			out = append(out, h)
		}
	}
	return out
}

// ErrHostPartitionEmpty signals that the host has no 'P'-family partition
// (never indexed under COSIFT_HOST_PARTITION, or backfill pending). Callers
// fall back to the global Search + post-filter path.
var ErrHostPartitionEmpty = errors.New("pebble-bm25: host partition empty")

// SearchInHost is Search scoped to a single host via the 'P' posting
// partition: it scans only that host's posting lists (O(site_docs)) instead of
// the global lists (O(corpus)). IDF and avgDocLen stay GLOBAL — a host-local
// IDF would over-weight terms common across the web but rare on the site, and
// no per-host DocFreq exists. Returns ErrHostPartitionEmpty when the host has
// no partition so the caller can fall back. MaxScore early-termination is
// omitted: host lists are tiny, so a full lossless scan is cheap.
func (b *PebbleBM25) SearchInHost(ctx context.Context, q, host string, k int) ([]Hit, error) {
	hostID, ok, err := b.store.GetHostID(ctx, host)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrHostPartitionEmpty
	}

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

	scores := make(map[int64]float64)
	minIDF := bm25MinIDF()
	type termPost struct {
		idf  float64
		info store.TermInfo
	}
	uniqTerms := dedupeStrings(tokens)
	candidates := make([]termPost, 0, len(uniqTerms))
	for _, term := range uniqTerms {
		info, ok, err := b.store.GetTermInfo(ctx, term)
		if err != nil {
			return nil, err
		}
		if !ok || info.DocFreq == 0 {
			continue
		}
		idf := math.Log(1.0 + (float64(totalDocs)-float64(info.DocFreq)+0.5)/(float64(info.DocFreq)+0.5))
		candidates = append(candidates, termPost{idf: idf, info: info})
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].idf > candidates[j].idf })
	active := candidates[:0]
	for _, c := range candidates {
		if c.idf >= minIDF {
			active = append(active, c)
		}
	}
	if len(active) == 0 {
		active = candidates[:1]
	}

	for _, c := range active {
		idf := c.idf
		if err := b.store.IterateHostPostings(ctx, hostID, c.info.ID, func(p store.PostingEntry) bool {
			docLen := p.DocLen
			if docLen <= 0 {
				docLen = 1
			}
			tfF := float64(p.TF)
			lenNorm := 1.0 - b.b + b.b*(float64(docLen)/avgDocLen)
			scores[p.DocID] += idf * ((tfF * (b.k1 + 1.0)) / (tfF + b.k1*lenNorm))
			return true
		}); err != nil {
			return nil, err
		}
	}

	// Hit resolution: cheap 'i' side-blob, authority multiplier, phrase
	// filter, sort, top-k. No boostIDs path — the partition already
	// guarantees host membership, so the 50× site boost is unnecessary here.
	// Resolve-all (no top-k pool) is intentional: host partitions are
	// O(site_docs), so the pool machinery in Search would buy nothing.
	hits := make([]Hit, 0, len(scores))
	for docID, sc := range scores {
		url, title, ok, err := b.store.GetDocMeta(ctx, docID)
		if err != nil || !ok || url == "" {
			continue
		}
		if b.authority != nil {
			sc *= b.authority.Multiplier(hostFromURL(url))
		}
		hits = append(hits, Hit{DocID: docID, URL: url, Title: title, Score: sc})
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

// hostFromURL extracts the lowercased host from a URL string; returns
// empty when parsing fails (caller treats that as "no authority data").
// Cheap enough to inline-call once per hit candidate (k≈thousands).
func hostFromURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	return strings.ToLower(u.Host)
}

// scoredCand is one (docID, accumulated raw BM25 score) pool entry.
type scoredCand struct {
	docID int64
	score float64
}

// topCandidates returns the top-n entries of scores sorted by descending
// score (ties broken by ascending docID for determinism). Same partial
// heap-select idiom as kthLargest, but keeps the entries.
func topCandidates(scores map[int64]float64, n int) []scoredCand {
	if n <= 0 || len(scores) == 0 {
		return nil
	}
	if n > len(scores) {
		n = len(scores)
	}
	less := func(a, b scoredCand) bool {
		if a.score != b.score {
			return a.score < b.score
		}
		return a.docID > b.docID
	}
	// Min-heap of size n; root is the weakest kept candidate.
	h := make([]scoredCand, 0, n)
	siftDown := func(i int) {
		for {
			l, r := 2*i+1, 2*i+2
			smallest := i
			if l < len(h) && less(h[l], h[smallest]) {
				smallest = l
			}
			if r < len(h) && less(h[r], h[smallest]) {
				smallest = r
			}
			if smallest == i {
				return
			}
			h[i], h[smallest] = h[smallest], h[i]
			i = smallest
		}
	}
	for id, s := range scores {
		c := scoredCand{docID: id, score: s}
		if len(h) < n {
			h = append(h, c)
			for i := len(h) - 1; i > 0; {
				p := (i - 1) / 2
				if !less(h[i], h[p]) {
					break
				}
				h[p], h[i] = h[i], h[p]
				i = p
			}
			continue
		}
		if !less(h[0], c) {
			continue
		}
		h[0] = c
		siftDown(0)
	}
	sort.Slice(h, func(i, j int) bool { return less(h[j], h[i]) })
	return h
}

// kthLargest returns the k-th highest value in the scores map. If the map
// has fewer than k entries, returns the smallest present (0 if empty).
// Implemented with a partial heap-select to avoid sorting the entire map
// on every MaxScore check — scores can be very large during query
// processing on multi-million-doc corpora.
func kthLargest(scores map[int64]float64, k int) float64 {
	if k <= 0 || len(scores) == 0 {
		return 0
	}
	if k >= len(scores) {
		// Smallest present value.
		minS := math.Inf(1)
		for _, s := range scores {
			if s < minS {
				minS = s
			}
		}
		return minS
	}
	// Maintain a min-heap of size k. The root is the k-th largest seen.
	h := make([]float64, 0, k)
	for _, s := range scores {
		if len(h) < k {
			h = append(h, s)
			// Sift up.
			for i := len(h) - 1; i > 0; {
				p := (i - 1) / 2
				if h[p] <= h[i] {
					break
				}
				h[p], h[i] = h[i], h[p]
				i = p
			}
			continue
		}
		if s <= h[0] {
			continue
		}
		// Replace root, sift down.
		h[0] = s
		for i := 0; ; {
			l, r := 2*i+1, 2*i+2
			smallest := i
			if l < len(h) && h[l] < h[smallest] {
				smallest = l
			}
			if r < len(h) && h[r] < h[smallest] {
				smallest = r
			}
			if smallest == i {
				break
			}
			h[i], h[smallest] = h[smallest], h[i]
			i = smallest
		}
	}
	return h[0]
}
