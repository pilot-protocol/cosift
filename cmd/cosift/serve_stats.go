package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/store"
)

func (s *pebbleHTTP) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

// handleDomains returns indexed hosts by doc count.
//
// Query params:
//   - top:    legacy alias for limit (capped at 500). Preserved for the
//     landing page contract.
//   - q:      substring filter (case-insensitive) on the host name.
//   - offset: pagination offset (default 0).
//   - limit:  page size (default 50, capped at 500).
//
// Response:
//
//	{"total": N, "offset": M, "limit": L, "domains": [...]}
func (s *pebbleHTTP) handleDomains(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	// Legacy 'top' alias from When set, behaves like limit + no
	// search/offset; lands as the first page.
	if v := r.URL.Query().Get("top"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
			offset = 0
		}
	}
	domains, total, err := s.store.ListDomains(r.Context(), q, offset, limit)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":   total,
		"offset":  offset,
		"limit":   limit,
		"q":       q,
		"domains": domains,
	})
}

// handleQueue surfaces frontier queue depth + top-N hosts currently queued
// for crawl. Fills the gap left by /domains, which only shows
// already-indexed hosts.
func (s *pebbleHTTP) handleQueue(w http.ResponseWriter, r *http.Request) {
	topN := 25
	if v := r.URL.Query().Get("top"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			topN = n
		}
	}
	fs, err := s.store.GetFrontierStats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	hosts, err := s.store.TopQueuedHosts(r.Context(), topN)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	body := map[string]any{
		"queued":    fs.Queued,
		"in_flight": fs.InFlight,
		"done":      fs.Done,
		"errored":   fs.Errored,
		"top_hosts": hosts,
	}
	// Lane breakdown: PebbleStore-only (SQLite Store has no lanes). When the
	// store is a PebbleStore, surface the per-lane queued/in_flight counts so
	// operators can see whether the weighted RR is actually draining RSS
	// (lane 1) ahead of bulk (lane 3).
	if ps, ok := any(s.store).(*store.PebbleStore); ok {
		if ls, lerr := ps.GetLaneStats(r.Context()); lerr == nil {
			laneNames := [4]string{"submitted", "refresh", "discovered", "bulk"}
			lanesOut := make([]map[string]any, 0, 4)
			for i, n := range laneNames {
				lanesOut = append(lanesOut, map[string]any{
					"lane":      i,
					"name":      n,
					"queued":    ls.Lanes[i].Queued,
					"in_flight": ls.Lanes[i].InFlight,
				})
			}
			body["lanes"] = lanesOut
			body["legacy_queued"] = ls.LegacyQueued
			body["legacy_in_flight"] = ls.LegacyInFlight
		}
	}
	writeJSON(w, http.StatusOK, body)
}

// handleDomainsAudit streams the entire 'h' family as JSONL, one
// {host, count, authority, etld1, tld} record per line. Single pass
// over the index — at 9M hosts on the GH200 this is ~30 seconds of
// streaming, vs the 18000 full scans that brute-paginating /domains
// would force. Read-only; safe to run live.
//
// Query params:
//
//	min_count=N — emit only hosts with count >= N (default 1)
//	limit=N     — cap the number of emitted rows (default unlimited)
//	below=F     — emit only hosts with authority score < F (filter
//	              for the blocklist proposal stream; default 1.0)
//	above=F     — emit only hosts with score >= F (filter for the
//	              authority-list inspection; default 0.0)
func (s *pebbleHTTP) handleDomainsAudit(w http.ResponseWriter, r *http.Request) {
	minCount := 1
	if v := r.URL.Query().Get("min_count"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			minCount = n
		}
	}
	limit := -1
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	below := 1.0
	if v := r.URL.Query().Get("below"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			below = f
		}
	}
	above := 0.0
	if v := r.URL.Query().Get("above"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			above = f
		}
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	emitted := 0
	flusher, _ := w.(http.Flusher)
	flushEvery := 256
	err := s.store.IterateDomains(r.Context(), func(host string, count int) bool {
		if count < minCount {
			return true
		}
		var score float64 = 0.5
		if s.authority != nil {
			score = s.authority.Score(host)
		}
		if score >= below || score < above {
			return true
		}
		rec := map[string]any{
			"host":      host,
			"count":     count,
			"authority": score,
			"recommend": classify(score),
		}
		if err := enc.Encode(rec); err != nil {
			return false
		}
		emitted++
		if limit > 0 && emitted >= limit {
			return false
		}
		if flusher != nil && emitted%flushEvery == 0 {
			flusher.Flush()
		}
		return true
	})
	if err != nil {
		// Best-effort: log and stop; we may have already flushed
		// partial output, so don't try to writeProblem.
		log.Printf("handleDomainsAudit: %v", err)
	}
	if flusher != nil {
		flusher.Flush()
	}
}

// handleSLA returns the current SLA evaluation snapshot. Read-only.
// Surfaces per-endpoint p50/p95/p99 + error rate, configured targets,
// and the most recent violations. Operators consume this in dashboards
// + on-call paging.
func (s *pebbleHTTP) handleSLA(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if s.sla == nil {
		_, _ = w.Write([]byte(`{"enabled":false}`))
		return
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(s.sla.Snapshot())
}

func (s *pebbleHTTP) handleStats(w http.ResponseWriter, r *http.Request) {
	// stale-while-revalidate cache.
	//   - Fresh hit (< TTL): return cached, X-Cache: HIT.
	//   - Stale (have cache, past TTL but under maxStale): return cached,
	//     X-Cache: STALE, kick a background refresh (single-flight).
	//   - Very stale (past maxStale): fall through to synchronous compute
	//     — protects against the case where a refresh goroutine
	//     hangs under HNSW lock contention and the cache shows ancient
	//     values (we saw a startup-time vec=654 served for ~15 min).
	//   - Cold (no cache): compute synchronously, X-Cache: MISS.
	const statsBodyTTL = 5 * time.Second
	const statsBodyMaxStale = 60 * time.Second
	s.statsBodyMu.Lock()
	body := s.statsBodyBlob
	age := time.Since(s.statsBodyAt)
	cached := body != nil && !s.statsBodyAt.IsZero()
	s.statsBodyMu.Unlock()

	if cached && age < statsBodyTTL {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		_, _ = w.Write(body)
		return
	}
	if cached && age < statsBodyMaxStale {
		if s.statsRefreshing.CompareAndSwap(false, true) {
			go func() {
				defer s.statsRefreshing.Store(false)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if newBody, err := s.buildStatsBody(ctx); err == nil {
					s.statsBodyMu.Lock()
					s.statsBodyBlob = newBody
					s.statsBodyAt = time.Now()
					s.statsBodyMu.Unlock()
				}
			}()
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "STALE")
		_, _ = w.Write(body)
		return
	}

	// Cold path: compute synchronously and populate the cache.
	newBody, err := s.buildStatsBody(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.statsBodyMu.Lock()
	s.statsBodyBlob = newBody
	s.statsBodyAt = time.Now()
	s.statsBodyMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	_, _ = w.Write(newBody)
}

// buildStatsBody collects every signal /stats surfaces and marshals it.
// Heavy paths: PebbleStore.Stats (d-family scan, partially counter-cached
// since), HNSW.PQStatus (O(N) under h.mu read lock that
// contends with crawler writers). Called by handleStats both
// synchronously (cold) and from a background goroutine (SWR refresh).
func (s *pebbleHTTP) buildStatsBody(ctx context.Context) ([]byte, error) {
	// /stats was observed taking 31s under load
	// when the SWR cache exceeded maxStale: store.Stats() iterates the
	// entire 'd' family (10.87M entries) every cold compute. The
	// CorpusStats counter is an O(1) atomic read mirror of the persisted
	// indexed_docs meta — equivalent doc count for any operator-facing
	// purpose, microseconds instead of seconds. Terms isn't populated in
	// the Pebble path (always 0) so dropping the scan loses nothing.
	sumLen, indexedDocs, err := s.store.CorpusStats(ctx)
	if err != nil {
		return nil, err
	}
	var avg float64
	if indexedDocs > 0 {
		avg = float64(sumLen) / float64(indexedDocs)
	}
	// surface config knobs that affect retrieval/synth quality so
	// operators can verify env overrides took effect from one endpoint.
	out := map[string]any{
		"documents":    indexedDocs,
		"terms":        int64(0),
		"indexed_docs": indexedDocs,
		"sum_doc_len":  sumLen,
		"avg_doc_len":  avg,
		"uptime":       time.Since(s.started).String(),
		"backend":      "pebble",
		"bm25_k1":      s.idx.K1(),
		"bm25_b":       s.idx.B(),
	}
	if s.reranker != nil {
		out["reranker"] = s.reranker.Name()
		out["rerank_candidate_k"] = s.rerankCandK
	}
	if s.chat != nil {
		out["chat_model"] = s.chat.Model()
		// surface the cache caps so operators can verify
		// COSIFT_HYDE_CACHE_SIZE / _PARA_CACHE_SIZE overrides took effect.
		out["hyde_cache_size"] = s.hydeCacheCap
		out["paraphrase_cache_size"] = s.paraCacheCap
	}
	// Bounded LLM concurrency + circuit-breaker state. Surfaced so
	// operators can see whether the gate is shedding load (rejected > 0)
	// or holding requests (in_flight = cap) and whether the breaker has
	// tripped (state != "closed").
	if s.chatGate != nil {
		out["llm_gate"] = s.chatGate.Stats()
	}
	if s.chatSafe != nil {
		out["llm_answer_circuit"] = s.chatSafe.CircuitState()
	}
	if s.rerankSafe != nil {
		out["llm_rerank_circuit"] = s.rerankSafe.CircuitState()
	}
	if s.answerCache != nil {
		out["answer_cache"] = s.answerCache.Stats()
	}
	if s.llmProbe != nil {
		out["llm_load"] = s.llmProbe.Stats()
	}
	if s.sla != nil {
		out["sla"] = s.sla.Snapshot()
	}
	if s.authority != nil {
		out["authority"] = s.authority.Stats()
	}
	// embed cache hit/miss counters so operators can see how
	// often re-fetches are skipping ollama. The cache wraps the round-
	// robin embedder when cfg.Embeddings.CacheDir is set; type-assert
	// down to extract the counters.
	if ce, ok := s.embedder.(*embed.CachedEmbedder); ok {
		hits := ce.Hits()
		misses := ce.Misses()
		total := hits + misses
		hitRate := 0.0
		if total > 0 {
			hitRate = 100 * float64(hits) / float64(total)
		}
		out["embed_cache"] = map[string]any{
			"hits":         hits,
			"misses":       misses,
			"hit_rate_pct": hitRate,
		}
	}
	// signal whether the store has HNSW vectors persisted, and
	// (when meta is available) surface dim + node count.
	// when the graph is loaded in memory, report s.hnsw.Len()
	// instead of the startup-cached vectorNodes count — otherwise /stats
	// shows the corpus frozen in time while the in-serve crawler keeps
	// growing the graph.
	// "% of the indexed web" novelty stat. The web is too big to
	// give a meaningful number, but Common Crawl's monthly snapshot is
	// ~3.5 B unique pages — a recognizable reference point. Operators can
	// override with COSIFT_WEB_DENOMINATOR if they want a different scale
	// (e.g. their own intranet, a TLD slice, etc.).
	webDenom := int64(3_500_000_000)
	if v := os.Getenv("COSIFT_WEB_DENOMINATOR"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			webDenom = n
		}
	}
	out["web_pct"] = 100.0 * float64(indexedDocs) / float64(webDenom)
	out["web_denominator"] = webDenom
	out["has_vectors"] = s.hasVectors
	switch {
	case s.hnsw != nil:
		out["vector_nodes"] = s.hnsw.Len()
		out["vector_dim"] = s.vectorDim
	case s.vectorNodes > 0:
		out["vector_nodes"] = s.vectorNodes
		out["vector_dim"] = s.vectorDim
	}
	// whether the graph is loaded into memory for dense retrieval.
	out["hnsw_loaded"] = s.hnsw != nil
	// PQ status — operator-facing visibility into compression
	// state. Only present when the graph is loaded; nil otherwise.
	if s.hnsw != nil {
		pq := s.hnsw.PQStatus()
		// coverage is over VALID nodes (vec != nil), not raw total —
		// zombie slots from pre partial persists inflate the total
		// without being searchable. NodesTotal still surfaced for context.
		denom := pq.NodesValid
		if denom == 0 {
			denom = pq.NodesTotal // avoid div-by-zero on a fresh load
		}
		coverage := 0.0
		if denom > 0 {
			coverage = 100 * float64(pq.NodesWithCode) / float64(denom)
		}
		zombies := pq.NodesTotal - pq.NodesValid
		pqInfo := map[string]any{
			"enabled":         pq.Enabled,
			"nodes_with_code": pq.NodesWithCode,
			"nodes_valid":     pq.NodesValid,
			"nodes_total":     pq.NodesTotal,
			"zombie_nodes":    zombies,
			"coverage_pct":    coverage,
		}
		if pq.Enabled {
			pqInfo["dim"] = pq.Dim
			pqInfo["m"] = pq.M
			pqInfo["k"] = pq.K
		}
		out["pq"] = pqInfo
	}
	// Clients can read
	// this once instead of probing ?retriever=dense + parsing the warning.
	// bm25 always — index is always available. dense/hybrid require a
	// loaded graph; the embedder lets dense/hybrid handle text-mode and
	// new queries. /find_similar URL-mode dense works without embedder, but
	// that's an endpoint-specific carve-out — keep this list general.
	retrievers := []string{"bm25", "bm25-mlt"}
	if s.hnsw != nil && s.embedder != nil {
		retrievers = append(retrievers, "dense", "hybrid")
	} else if s.hnsw != nil {
		// Graph but no embedder — only URL-mode /find_similar can use dense.
		retrievers = append(retrievers, "dense:find_similar_url_only")
	}
	out["retrievers"] = retrievers
	// crawl-rate surface for the in-serve crawler.
	// Computes docs added since process start and per-minute rate.
	if s.crawlActive {
		uptimeMin := time.Since(s.started).Minutes()
		added := int(indexedDocs) - s.startupDocs
		if added < 0 {
			added = 0
		}
		rate := 0.0
		if uptimeMin > 0 {
			rate = float64(added) / uptimeMin
		}
		out["crawl_active"] = true
		out["docs_added_since_start"] = added
		out["docs_per_minute"] = rate
	}
	return json.Marshal(out)
}

// Prometheus-format scrape endpoint. Hand-written plain text
// (no client_golang dep) — exposition format is simple enough that
// pulling in a dep just to print four gauges isn't justified.
// Quantities chosen to be O(1) reads (CorpusStats counters + uptime);
// frontier scans deliberately excluded so scrape latency stays flat
// regardless of corpus size.
func (s *pebbleHTTP) handleMetrics(w http.ResponseWriter, r *http.Request) {
	sumLen, count, err := s.store.CorpusStats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	var avg float64
	if count > 0 {
		avg = float64(sumLen) / float64(count)
	}
	uptime := time.Since(s.started).Seconds()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "# HELP cosift_indexed_docs Number of documents passed through IndexDocument.\n")
	fmt.Fprintf(w, "# TYPE cosift_indexed_docs gauge\n")
	fmt.Fprintf(w, "cosift_indexed_docs %d\n", count)
	fmt.Fprintf(w, "# HELP cosift_sum_doc_len_total Sum of indexed document lengths in tokens.\n")
	fmt.Fprintf(w, "# TYPE cosift_sum_doc_len_total counter\n")
	fmt.Fprintf(w, "cosift_sum_doc_len_total %d\n", sumLen)
	fmt.Fprintf(w, "# HELP cosift_avg_doc_len Average document length in tokens (derived).\n")
	fmt.Fprintf(w, "# TYPE cosift_avg_doc_len gauge\n")
	fmt.Fprintf(w, "cosift_avg_doc_len %.2f\n", avg)
	fmt.Fprintf(w, "# HELP cosift_uptime_seconds Seconds since pebble-serve started.\n")
	fmt.Fprintf(w, "# TYPE cosift_uptime_seconds counter\n")
	fmt.Fprintf(w, "cosift_uptime_seconds %.0f\n", uptime)
	// crawl_active gauges whether
	// the crawler goroutine is running; docs_added is monotonic
	// since process start; docs_per_minute is the rolling rate.
	crawlActiveGauge := 0
	if s.crawlActive {
		crawlActiveGauge = 1
	}
	fmt.Fprintf(w, "# HELP cosift_crawl_active 1 if in-serve crawler is running, 0 otherwise.\n")
	fmt.Fprintf(w, "# TYPE cosift_crawl_active gauge\n")
	fmt.Fprintf(w, "cosift_crawl_active %d\n", crawlActiveGauge)
	if s.crawlActive {
		added := count - int64(s.startupDocs)
		if added < 0 {
			added = 0
		}
		fmt.Fprintf(w, "# HELP cosift_crawl_docs_added_total Documents added by the in-serve crawler since process start.\n")
		fmt.Fprintf(w, "# TYPE cosift_crawl_docs_added_total counter\n")
		fmt.Fprintf(w, "cosift_crawl_docs_added_total %d\n", added)
		rate := 0.0
		if uptime > 0 {
			rate = float64(added) / (uptime / 60)
		}
		fmt.Fprintf(w, "# HELP cosift_crawl_docs_per_minute Recent crawl rate (docs added per minute, averaged since process start).\n")
		fmt.Fprintf(w, "# TYPE cosift_crawl_docs_per_minute gauge\n")
		fmt.Fprintf(w, "cosift_crawl_docs_per_minute %.2f\n", rate)
	}
	// HyDE cache effectiveness. Hits/misses both monotonic so
	// Prometheus rate() over these gives cache pressure under load.
	fmt.Fprintf(w, "# HELP cosift_hyde_cache_hits_total HyDE cache hits (expandQuery served from memory).\n")
	fmt.Fprintf(w, "# TYPE cosift_hyde_cache_hits_total counter\n")
	fmt.Fprintf(w, "cosift_hyde_cache_hits_total %d\n", s.hydeHits.Load())
	fmt.Fprintf(w, "# HELP cosift_hyde_cache_misses_total HyDE cache misses (expandQuery called the LLM).\n")
	fmt.Fprintf(w, "# TYPE cosift_hyde_cache_misses_total counter\n")
	fmt.Fprintf(w, "cosift_hyde_cache_misses_total %d\n", s.hydeMisses.Load())
	fmt.Fprintf(w, "# HELP cosift_paraphrase_cache_hits_total Paraphrase cache hits (paraphraseQuery served from memory).\n")
	fmt.Fprintf(w, "# TYPE cosift_paraphrase_cache_hits_total counter\n")
	fmt.Fprintf(w, "cosift_paraphrase_cache_hits_total %d\n", s.paraHits.Load())
	fmt.Fprintf(w, "# HELP cosift_paraphrase_cache_misses_total Paraphrase cache misses (paraphraseQuery called the LLM).\n")
	fmt.Fprintf(w, "# TYPE cosift_paraphrase_cache_misses_total counter\n")
	fmt.Fprintf(w, "cosift_paraphrase_cache_misses_total %d\n", s.paraMisses.Load())
	fmt.Fprintf(w, "# HELP cosift_rerank_attempts_total Rerank calls invoked (any endpoint with ?rerank=true).\n")
	fmt.Fprintf(w, "# TYPE cosift_rerank_attempts_total counter\n")
	fmt.Fprintf(w, "cosift_rerank_attempts_total %d\n", s.rerankAttempts.Load())
	fmt.Fprintf(w, "# HELP cosift_rerank_failures_total Rerank calls that returned an error (silently fell back to BM25 order).\n")
	fmt.Fprintf(w, "# TYPE cosift_rerank_failures_total counter\n")
	fmt.Fprintf(w, "cosift_rerank_failures_total %d\n", s.rerankFailures.Load())
	fmt.Fprintf(w, "# HELP cosift_chat_attempts_total Chat-client calls invoked (HyDE, /answer synth, /research plan + synth).\n")
	fmt.Fprintf(w, "# TYPE cosift_chat_attempts_total counter\n")
	fmt.Fprintf(w, "cosift_chat_attempts_total %d\n", s.chatAttempts.Load())
	fmt.Fprintf(w, "# HELP cosift_chat_failures_total Chat-client calls that returned an error.\n")
	fmt.Fprintf(w, "# TYPE cosift_chat_failures_total counter\n")
	fmt.Fprintf(w, "cosift_chat_failures_total %d\n", s.chatFailures.Load())
	fmt.Fprintf(w, "# HELP cosift_chat_duration_seconds_sum Cumulative wall-clock spent inside chat-client calls.\n")
	fmt.Fprintf(w, "# TYPE cosift_chat_duration_seconds_sum counter\n")
	fmt.Fprintf(w, "cosift_chat_duration_seconds_sum %.6f\n", float64(s.chatDurationNanos.Load())/1e9)
	fmt.Fprintf(w, "# HELP cosift_warnings_emitted_total Responses that carried at least one warning (misconfigured request).\n")
	fmt.Fprintf(w, "# TYPE cosift_warnings_emitted_total counter\n")
	fmt.Fprintf(w, "cosift_warnings_emitted_total %d\n", s.warningsEmitted.Load())
	// HNSW vector index shape. Live count when graph is
	// loaded; startup-cached otherwise.
	vectorNodesLive := s.vectorNodes
	if s.hnsw != nil {
		vectorNodesLive = s.hnsw.Len()
	}
	if vectorNodesLive > 0 {
		fmt.Fprintf(w, "# HELP cosift_vector_nodes Number of HNSW vector nodes in memory (or persisted when not loaded).\n")
		fmt.Fprintf(w, "# TYPE cosift_vector_nodes gauge\n")
		fmt.Fprintf(w, "cosift_vector_nodes %d\n", vectorNodesLive)
		fmt.Fprintf(w, "# HELP cosift_vector_dim Embedding dimension of the persisted HNSW index.\n")
		fmt.Fprintf(w, "# TYPE cosift_vector_dim gauge\n")
		fmt.Fprintf(w, "cosift_vector_dim %d\n", s.vectorDim)
	}
	// PromQL
	// rate(cosift_request_duration_seconds_sum) / rate(cosift_requests_total)
	// gives mean latency in any window. Labels = path; misrouted calls (404)
	// don't share a label with any handled path.
	fmt.Fprintf(w, "# HELP cosift_requests_total HTTP requests served, by endpoint.\n")
	fmt.Fprintf(w, "# TYPE cosift_requests_total counter\n")
	fmt.Fprintf(w, "# HELP cosift_request_duration_seconds_sum Cumulative request duration, by endpoint.\n")
	fmt.Fprintf(w, "# TYPE cosift_request_duration_seconds_sum counter\n")
	s.requestCounts.Range(func(k, v any) bool {
		path := k.(string)
		m := v.(*endpointMetrics)
		fmt.Fprintf(w, "cosift_requests_total{endpoint=%q} %d\n", path, m.count.Load())
		fmt.Fprintf(w, "cosift_request_duration_seconds_sum{endpoint=%q} %.6f\n", path, float64(m.sumNanos.Load())/1e9)
		return true
	})
}

// HTTP form of `cosift verify`. Same comparison (running
// counters vs authoritative 'l' family scan) on the already-open store, so
// monitoring can poll without contending for the single-writer lock or
// shelling into the container. 503 on drift makes this composable with
// Kubernetes liveness / load-balancer health checks.
func (s *pebbleHTTP) handleVerify(w http.ResponseWriter, r *http.Request) {
	counterSum, counterCount, err := s.store.CorpusStats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	scanSum, scanCount, err := s.store.SumDocLengths(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	driftCount := counterCount - scanCount
	driftSum := counterSum - scanSum
	ok := driftCount == 0 && driftSum == 0
	body := map[string]any{
		"ok":                   ok,
		"indexed_docs_counter": counterCount,
		"indexed_docs_scan":    scanCount,
		"indexed_docs_drift":   driftCount,
		"sum_doc_len_counter":  counterSum,
		"sum_doc_len_scan":     scanSum,
		"sum_doc_len_drift":    driftSum,
	}
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, body)
}
