package server

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/pilot-protocol/cosift/internal/index"
)

// selectPRFTerms picks the most distinctive terms across a result set for
// pseudo-relevance feedback (PRF) query expansion.
//
// Algorithm (lightweight Rocchio variant, no per-corpus IDF lookup):
//  1. Tokenize each result's text (reuses index.Tokenize → lowercase +
//     stopword + len<2 filtering, so the input is already clean).
//  2. Per-doc DEDUPE — one heavy doc can't dominate the expansion just
//     because it repeats a term many times.
//  3. Count documents containing each term across the result set.
//  4. Filter out terms in the original query (no point re-asking for what
//     we already asked).
//  5. Filter out terms appearing in only 1 doc — the floor for "this term
//     is consistently associated with the topic" rather than a one-off.
//  6. Sort by document-frequency descending, return top-n.
//
// This isn't full RM3 (which weights terms by log P(term|relevant) /
// P(term|background) — needs corpus IDF). It's a faster approximation that
// gives reasonable expansion terms without an extra SQL pass for IDF
// lookup. Upgrade path: pull doc_freq from `terms` table per candidate;
// rank by tf*idf. Defer until measured to be needed.
//
// `texts` is the URL → body map (from store.GetDocTexts). `originalQuery`
// is the user's raw query string (we re-tokenize it the same way to filter).
// `n` caps the expansion. Returns nil if no terms qualify.
func selectPRFTerms(texts map[string]string, originalQuery string, n int) []string {
	if n <= 0 || len(texts) == 0 {
		return nil
	}
	qTokens := make(map[string]struct{})
	for _, t := range index.Tokenize(originalQuery) {
		qTokens[t] = struct{}{}
	}

	docFreq := make(map[string]int)
	for _, body := range texts {
		seen := make(map[string]struct{})
		for _, tok := range index.Tokenize(body) {
			if _, q := qTokens[tok]; q {
				continue
			}
			if _, s := seen[tok]; s {
				continue
			}
			seen[tok] = struct{}{}
			docFreq[tok]++
		}
	}

	type pair struct {
		term  string
		count int
	}
	candidates := make([]pair, 0, len(docFreq))
	for t, c := range docFreq {
		if c < 2 {
			// "Appears in only 1 doc" is too weak a signal; could be noise.
			continue
		}
		candidates = append(candidates, pair{t, c})
	}
	// Sort by count desc, then term asc for deterministic output (so
	// flake lesson holds — never compare ranks under map-iteration order).
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].count != candidates[j].count {
			return candidates[i].count > candidates[j].count
		}
		return candidates[i].term < candidates[j].term
	})
	if n > len(candidates) {
		n = len(candidates)
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = candidates[i].term
	}
	return out
}

// applyPRFIfRequested mines distinctive terms from `hits`, then re-runs
// retrieval with the expanded query when ?prf=true is set. Returns the
// updated hits/spans + a source-tag suffix to append (empty when PRF was
// skipped or produced no expansion terms). Extracted from
// /search's inline block as the second call site (and counting
// /answer + streaming /answer as #3 + #4) reuses the same logic.
//
// PRF only applies to bm25 and hybrid retrievers. Dense embeddings don't
// benefit from lexical term expansion. Caller must have ≥3 initial hits;
// fewer means the doc-frequency ≥ 2 floor in selectPRFTerms produces
// little expansion signal.
//
// runner is the retrieval function — usually s.runSearch — passed in so
// the helper doesn't depend on the Server type directly. textsLoader
// fetches per-URL bodies for term-mining; usually s.store.GetDocTexts.
func (s *Server) applyPRFIfRequested(ctx context.Context, r *http.Request, retriever, q string, hits []index.Hit, spans []denseSpan, innerK int) ([]index.Hit, []denseSpan, string) {
	v := r.URL.Query().Get("prf")
	if v != "true" && v != "1" {
		return hits, spans, ""
	}
	if retriever != "bm25" && retriever != "hybrid" {
		return hits, spans, ""
	}
	if len(hits) < 3 {
		return hits, spans, ""
	}
	prfTerms := 5
	if pv := r.URL.Query().Get("prf_terms"); pv != "" {
		if n, perr := strconv.Atoi(pv); perr == nil && n > 0 {
			prfTerms = n
		}
	}
	prfDocs := 10
	if pv := r.URL.Query().Get("prf_docs"); pv != "" {
		if n, perr := strconv.Atoi(pv); perr == nil && n > 0 {
			prfDocs = n
		}
	}
	topN := prfDocs
	if topN > len(hits) {
		topN = len(hits)
	}
	urls := make([]string, 0, topN)
	for i := 0; i < topN; i++ {
		urls = append(urls, hits[i].URL)
	}
	texts, _ := s.store.GetDocTexts(ctx, urls, 2000)
	expansion := selectPRFTerms(texts, q, prfTerms)
	if len(expansion) == 0 {
		return hits, spans, ""
	}
	expandedQ := q + " " + strings.Join(expansion, " ")
	var eh []index.Hit
	var es []denseSpan
	var eerr error
	if retriever == "hybrid" {
		// Hybrid: BM25 sub-call uses expanded; dense stays on q (or HyDE).
		expCtx := context.WithValue(ctx, bm25QueryOverrideKey{}, expandedQ)
		eh, es, _, eerr = s.runSearch(expCtx, retriever, q, innerK)
	} else {
		eh, es, _, eerr = s.runSearch(ctx, retriever, expandedQ, innerK)
	}
	if eerr != nil || len(eh) == 0 {
		return hits, spans, ""
	}
	return eh, es, fmt.Sprintf("+prf(%d)", len(expansion))
}

// applyPRFToResearchPassages augments a /research passage list with results
// from a PRF-expanded BM25 search.
//
// Design choice (vs per-variant PRF): /research's planner / paraphrase
// strategies already do N retrievals (one per variant); per-variant PRF
// would double that to 2N. The post-fusion augment instead does ONE extra
// BM25 retrieval over the expanded query and merges dedup'd results into
// the existing passage list. Trades depth-per-variant for breadth across
// the whole result.
//
// PRF term mining uses the passages' bodies that are ALREADY in memory
// (gatherResearchPassages fetched them via GetDocByURL) — no extra SQL
// for the mining step, only for the post-mining BM25 search and the per-URL
// fetch on new passages. Matches the cap heuristics
// (?prf_terms, ?prf_docs) for consistency.
//
// Returns the augmented passage list + a source-tag suffix ("+prf(N)") that
// callers can append to the response's strategy/source field. Empty suffix
// means PRF was off or produced no expansion.
func (s *Server) applyPRFToResearchPassages(ctx context.Context, r *http.Request, q string, passages []researchPassage) ([]researchPassage, string) {
	v := r.URL.Query().Get("prf")
	if v != "true" && v != "1" {
		return passages, ""
	}
	if len(passages) < 3 {
		return passages, ""
	}
	prfTerms := 5
	if pv := r.URL.Query().Get("prf_terms"); pv != "" {
		if n, perr := strconv.Atoi(pv); perr == nil && n > 0 {
			prfTerms = n
		}
	}
	prfDocs := 10
	if pv := r.URL.Query().Get("prf_docs"); pv != "" {
		if n, perr := strconv.Atoi(pv); perr == nil && n > 0 {
			prfDocs = n
		}
	}
	topN := prfDocs
	if topN > len(passages) {
		topN = len(passages)
	}
	// Use passages' bodies (already loaded) for term mining. Saves the
	// GetDocTexts SQL roundtrip /search does.
	texts := make(map[string]string, topN)
	for i := 0; i < topN; i++ {
		texts[passages[i].url] = passages[i].text
	}
	expansion := selectPRFTerms(texts, q, prfTerms)
	if len(expansion) == 0 {
		return passages, ""
	}
	expandedQ := q + " " + strings.Join(expansion, " ")
	// One BM25 retrieval over the expansion. limit reuses /research's
	// ResearchSynthK so the augmented list doesn't exceed synth cap.
	limit := 10
	if s.defaults.ResearchSynthK > 0 {
		limit = s.defaults.ResearchSynthK
	}
	hits, _, _, err := s.runSearch(ctx, "bm25", expandedQ, limit)
	if err != nil {
		return passages, ""
	}
	seen := make(map[string]bool, len(passages)+limit)
	for _, p := range passages {
		seen[p.url] = true
	}
	for _, h := range hits {
		if seen[h.URL] {
			continue
		}
		seen[h.URL] = true
		doc, err := s.store.GetDocByURL(ctx, h.URL)
		if err != nil {
			continue
		}
		// preserve retriever score for AnswerSource.Score downstream.
		passages = append(passages, researchPassage{
			url: doc.URL, title: doc.Title, text: doc.Text, author: doc.Author,
			score: h.Score,
		})
		if len(passages) >= limit {
			break
		}
	}
	return passages, fmt.Sprintf("+prf(%d)", len(expansion))
}
