// Vector index + hybrid fusion live here. Pure Go, no external vector lib.
//
// At ≤1M passages, brute-force cosine over float32 is fine — back-of-envelope:
// 1M × 1536 × 4 B = 6 GB hot; per-query work is one pass, ~6 GB/s on modern RAM,
// so ~1 s/query worst case. For Cosift's lean profile (≤100k passages target),
// per-query work is ~60 ms. HNSW arrives only when the brute scan becomes the
// bottleneck — not before.
//
// We pre-normalize stored vectors so cosine reduces to dot-product (one mul per
// dim instead of two muls + a sqrt). Same accuracy, ~2× faster scan.
package index

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

// VectorHit is a single result from the vector index.
type VectorHit struct {
	URL    string
	Title  string
	Score  float64 // cosine similarity, [-1, 1]
	Offset int     // byte offset of the matching passage in the source doc
	Length int     // byte length of the matching passage
}

// VectorIndex is an in-memory brute-force kNN index over passage embeddings.
//
// Multi-passage support: a single URL can appear in multiple entries (one per
// chunk). Search dedups by URL, keeping the highest-scoring passage per doc —
// the "max passage score" aggregation standard in passage-level retrieval.
type VectorIndex struct {
	mu   sync.RWMutex
	docs []vecDoc
	dim  int
}

type vecDoc struct {
	url, title     string
	offset, length int       // byte span in source; (0,0) for single-passage docs
	vec            []float32 // pre-normalized to unit length
}

// NewVectorIndex constructs an empty index for vectors of the given dim.
func NewVectorIndex(dim int) *VectorIndex {
	return &VectorIndex{dim: dim}
}

// AddPassage inserts a passage with explicit byte span info. The vector is
// L2-normalized in place. Use this when you have chunking metadata.
func (v *VectorIndex) AddPassage(url, title string, offset, length int, vec []float32) {
	if len(vec) != v.dim {
		return
	}
	cp := make([]float32, len(vec))
	copy(cp, vec)
	normalizeInPlace(cp)

	v.mu.Lock()
	v.docs = append(v.docs, vecDoc{url: url, title: title, offset: offset, length: length, vec: cp})
	v.mu.Unlock()
}

// Add inserts a doc-level embedding without span info. Equivalent to
// AddPassage(url, title, 0, 0, vec) — kept for tests and back-compat.
func (v *VectorIndex) Add(url, title string, vec []float32) {
	v.AddPassage(url, title, 0, 0, vec)
}

// Search returns the top-k by cosine similarity, deduplicated by URL.
//
// Dedup policy: for any URL with multiple passages, the entry with the highest
// cosine wins — the passage's offset/length come along for highlight support.
// This is the "max passage score" doc-level aggregation that's the standard in
// passage-level retrieval (ColBERT, BGE, voyage-context all assume the same).
func (v *VectorIndex) Search(_ context.Context, query []float32, k int) []VectorHit {
	if len(query) != v.dim {
		return nil
	}
	q := make([]float32, len(query))
	copy(q, query)
	normalizeInPlace(q)

	v.mu.RLock()
	defer v.mu.RUnlock()

	// Single pass: track the best (highest-score) passage per URL.
	type entry struct {
		docIdx int
		score  float32
	}
	bestByURL := make(map[string]entry, len(v.docs))
	for i := range v.docs {
		s := dot(q, v.docs[i].vec)
		url := v.docs[i].url
		cur, ok := bestByURL[url]
		if !ok || s > cur.score {
			bestByURL[url] = entry{docIdx: i, score: s}
		}
	}

	out := make([]entry, 0, len(bestByURL))
	for _, e := range bestByURL {
		out = append(out, e)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].score > out[b].score })
	if k > len(out) {
		k = len(out)
	}
	hits := make([]VectorHit, 0, k)
	for _, e := range out[:k] {
		d := v.docs[e.docIdx]
		hits = append(hits, VectorHit{
			URL: d.url, Title: d.title, Score: float64(e.score),
			Offset: d.offset, Length: d.length,
		})
	}
	return hits
}

// SearchMMR returns top-k via Maximal Marginal Relevance re-ranking of the
// top-candPool candidates by cosine. Frontier-grade IR.
//
// MMR balances relevance (cosine-to-query) with diversity (1 - cosine to
// already-selected hits). lambda controls the tradeoff:
//   - lambda = 1.0 → pure relevance (equivalent to Search)
//   - lambda = 0.0 → pure diversity (first hit is the top-relevance; later
//     hits prefer URLs maximally dissimilar from selected ones)
//   - lambda = 0.7 (recommended) → modest diversity bias; surfaces alternative
//     viewpoints without abandoning relevance
//
// candPool controls how many candidates to consider before MMR. Larger pools
// give MMR more room to find diverse alternatives at O(k*candPool) compute.
// Pass 0 for the auto-default (max of 5*k, 50). candPool < k → k itself.
//
// Carbonell & Goldstein 1998 (the original MMR paper) — standard recipe
// across information-retrieval systems for de-duplicating top-K results when
// the corpus has near-duplicates (paraphrases, mirror sites, syndicated news).
//
// Implementation uses the same brute-force scoring as Search — at v0 scale
// (≤1M passages, k≤100) the O(k*candPool) extra cost is dwarfed by the
// initial Search pass.
func (v *VectorIndex) SearchMMR(ctx context.Context, query []float32, k int, lambda float64, candPool int) []VectorHit {
	if k <= 0 {
		return nil
	}
	if lambda < 0 {
		lambda = 0
	}
	if lambda > 1 {
		lambda = 1
	}
	if candPool <= 0 {
		candPool = 5 * k
		if candPool < 50 {
			candPool = 50
		}
	}
	if candPool < k {
		candPool = k
	}

	// Phase 1: get the top-candPool candidates by cosine (Search already
	// dedups by URL — we get one representative passage per URL).
	candidates := v.Search(ctx, query, candPool)
	if len(candidates) <= k || lambda == 1.0 {
		// Either nothing to re-rank (already ≤k) or pure-relevance mode.
		if len(candidates) > k {
			return candidates[:k]
		}
		return candidates
	}

	// Phase 2: look up each candidate's stored embedding so we can compute
	// pairwise diversity. Embedding vectors are stored alongside the URL in
	// v.docs; reach in under the read lock.
	v.mu.RLock()
	defer v.mu.RUnlock()
	type cand struct {
		hit VectorHit
		vec []float32 // already normalized
	}
	// Build URL → vector map for quick lookup. v.docs may have multiple
	// passages per URL (Search picked the best-scoring); take the vector
	// matching the URL+(offset, length) tuple from candidates.
	byKey := make(map[string][]float32, len(v.docs))
	for i := range v.docs {
		key := vecDocKey(v.docs[i].url, v.docs[i].offset, v.docs[i].length)
		byKey[key] = v.docs[i].vec
	}
	cands := make([]cand, 0, len(candidates))
	for _, h := range candidates {
		if vec, ok := byKey[vecDocKey(h.URL, h.Offset, h.Length)]; ok {
			cands = append(cands, cand{hit: h, vec: vec})
		}
	}

	// Phase 3: greedy MMR selection. Start with the top-relevance candidate;
	// at each step add the one that maximizes lambda*relevance - (1-lambda)*max_sim_to_selected.
	selected := make([]cand, 0, k)
	remaining := cands
	// Track max similarity to selected for each remaining candidate, updated
	// incrementally so the inner loop stays O(|remaining|) per pick.
	maxSimToSel := make([]float32, len(remaining))
	// Greedy: first pick is always the top-relevance (max λ*rel with no
	// diversity penalty since selected is empty).
	for len(selected) < k && len(remaining) > 0 {
		bestIdx := -1
		var bestScore float64
		for i, c := range remaining {
			mmrScore := lambda*c.hit.Score - (1-lambda)*float64(maxSimToSel[i])
			if bestIdx < 0 || mmrScore > bestScore {
				bestIdx = i
				bestScore = mmrScore
			}
		}
		picked := remaining[bestIdx]
		selected = append(selected, picked)
		// Remove picked from remaining (swap with last + truncate).
		last := len(remaining) - 1
		remaining[bestIdx] = remaining[last]
		maxSimToSel[bestIdx] = maxSimToSel[last]
		remaining = remaining[:last]
		maxSimToSel = maxSimToSel[:last]
		// Update maxSimToSel for the now-shorter remaining slice using picked.vec.
		for i := range remaining {
			sim := dot(picked.vec, remaining[i].vec)
			if sim > maxSimToSel[i] {
				maxSimToSel[i] = sim
			}
		}
	}

	out := make([]VectorHit, len(selected))
	for i, s := range selected {
		out[i] = s.hit
	}
	return out
}

// vecDocKey produces a stable lookup key for a (url, offset, length) triple.
// used by SearchMMR to round-trip back to stored embeddings.
func vecDocKey(url string, offset, length int) string {
	return fmt.Sprintf("%s\x00%d\x00%d", url, offset, length)
}

// Len reports the number of indexed passages (NOT unique URLs).
func (v *VectorIndex) Len() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.docs)
}

func dot(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var s float32
	// Hand-unrolled by 4 — modest speed up without going SIMD.
	i := 0
	for ; i <= len(a)-4; i += 4 {
		s += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
	}
	for ; i < len(a); i++ {
		s += a[i] * b[i]
	}
	return s
}

func normalizeInPlace(v []float32) {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	if s == 0 {
		return
	}
	norm := float32(math.Sqrt(s))
	for i := range v {
		v[i] /= norm
	}
}

// --- Hybrid fusion (RRF) ---

// RRF merges multiple ranked lists into one by reciprocal rank fusion.
// k is the standard "RRF k" damping constant (60 is the published default;
// lower values weight top-rank more aggressively).
//
// Each input list is a slice of URLs ordered by descending relevance from one
// retriever. Returns the fused top-k URLs.
//
// Equal-weight wrapper around RRFWeighted — see that function for the
// per-retriever weighted variant.
func RRF(lists [][]string, topK int, rrfK float64) []string {
	return RRFWeighted(lists, nil, topK, rrfK)
}

// RRFWeighted is RRF with per-retriever weight multipliers. Use when different
// retrievers have known per-corpus quality differences — e.g., dense beats
// BM25 on jargon-heavy domains, BM25 beats dense on entity-heavy queries.
//
// weights[i] scales retriever i's per-rank contribution: list i contributes
// `weights[i] / (rrfK + rank+1)` per URL. weights and lists must be the same
// length, and every weight must be strictly positive. Any of: nil weights,
// length mismatch, or a non-positive weight → fall back to equal-weight
// (i.e., exactly RRF). Defensive default — avoids silently dropping a
// retriever when the caller fumbles the slice length.
//
// Equal weights of any positive constant value are equivalent to standard RRF
// (the constant cancels in pairwise comparison), so the meaningful inputs are
// weight RATIOS — {2.0, 1.0} gives the first retriever twice the influence,
// {1.0, 1.0, 0.5} treats the third as a tiebreaker.
//
// "Frontier-grade IR" angle: weighted fusion is what Elastic's RRF API and
// OpenSearch's Hybrid Query expose to operators. Cosift now matches.
func RRFWeighted(lists [][]string, weights []float64, topK int, rrfK float64) []string {
	if rrfK <= 0 {
		rrfK = 60
	}
	useWeights := len(weights) == len(lists)
	if useWeights {
		for _, w := range weights {
			if w <= 0 {
				useWeights = false
				break
			}
		}
	}
	scores := make(map[string]float64)
	for i, list := range lists {
		w := 1.0
		if useWeights {
			w = weights[i]
		}
		for rank, url := range list {
			scores[url] += w / (rrfK + float64(rank+1))
		}
	}
	type pair struct {
		url   string
		score float64
	}
	pairs := make([]pair, 0, len(scores))
	for u, s := range scores {
		pairs = append(pairs, pair{u, s})
	}
	sort.Slice(pairs, func(a, b int) bool { return pairs[a].score > pairs[b].score })

	if topK <= 0 || topK > len(pairs) {
		topK = len(pairs)
	}
	out := make([]string, 0, topK)
	for i := 0; i < topK; i++ {
		out = append(out, pairs[i].url)
	}
	return out
}

// RRFWithHostBoosts is RRFWeighted with an additional per-host score
// multiplier applied to the fused score before final sort. Operators use
// it to upweight authoritative hosts (wikipedia.org, arxiv.org, nih.gov)
// or downweight long-tail aggregators without rebuilding the index.
//
// hostBoosts maps host suffix → multiplier. A URL's effective multiplier
// is the most-specific matching suffix (longest match wins), or 1.0 when
// nothing matches. Suffix matching mirrors ExcludeDomains semantics, so
// "wikipedia.org": 1.5 boosts every subdomain (en., zh., …).
//
// keeps the no-host-boost callers on the cheap path — when
// hostBoosts is empty, falls through to RRFWeighted.
func RRFWithHostBoosts(lists [][]string, weights []float64, hostBoosts map[string]float64, topK int, rrfK float64) []string {
	if len(hostBoosts) == 0 {
		return RRFWeighted(lists, weights, topK, rrfK)
	}
	if rrfK <= 0 {
		rrfK = 60
	}
	useWeights := len(weights) == len(lists)
	if useWeights {
		for _, w := range weights {
			if w <= 0 {
				useWeights = false
				break
			}
		}
	}
	scores := make(map[string]float64)
	for i, list := range lists {
		w := 1.0
		if useWeights {
			w = weights[i]
		}
		for rank, url := range list {
			scores[url] += w / (rrfK + float64(rank+1))
		}
	}
	// Apply host-suffix boosts. Longest-match wins so an operator can set
	// blogs.example.com=0.3 while keeping example.com=1.2.
	for url, base := range scores {
		mult := HostBoostFor(url, hostBoosts)
		if mult != 1.0 {
			scores[url] = base * mult
		}
	}
	type pair struct {
		url   string
		score float64
	}
	pairs := make([]pair, 0, len(scores))
	for u, s := range scores {
		pairs = append(pairs, pair{u, s})
	}
	sort.Slice(pairs, func(a, b int) bool { return pairs[a].score > pairs[b].score })
	if topK <= 0 || topK > len(pairs) {
		topK = len(pairs)
	}
	out := make([]string, 0, topK)
	for i := 0; i < topK; i++ {
		out = append(out, pairs[i].url)
	}
	return out
}

// HostBoostFor extracts the host from a URL and returns the longest-suffix
// match from boosts (1.0 when nothing matches or url unparseable). Pure
// function so retrieval-time fusion can stay lock-free.
func HostBoostFor(rawURL string, boosts map[string]float64) float64 {
	// Cheap host extraction without net/url to avoid the alloc per URL.
	// All URLs we see come from the crawler / store and are already
	// well-formed; fallback to 1.0 if parsing fails.
	i := strings.Index(rawURL, "://")
	if i < 0 {
		return 1.0
	}
	rest := rawURL[i+3:]
	end := strings.IndexAny(rest, "/?#")
	host := rest
	if end >= 0 {
		host = rest[:end]
	}
	host = strings.ToLower(host)
	// Strip trailing port.
	if c := strings.LastIndex(host, ":"); c >= 0 {
		host = host[:c]
	}
	best := 1.0
	bestLen := -1
	for suf, mult := range boosts {
		if suf == "" {
			continue
		}
		s := strings.ToLower(suf)
		if host == s || strings.HasSuffix(host, "."+s) {
			if len(s) > bestLen {
				best = mult
				bestLen = len(s)
			}
		}
	}
	return best
}
