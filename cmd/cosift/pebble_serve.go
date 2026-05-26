// Iter 205 — minimal HTTP server backed by PebbleStore + PebbleBM25.
//
// Parallel to the SQLite-backed `cosift serve`. Read-only endpoints only:
// /healthz, /stats, /search, /contents. No crawler, no admin, no /answer
// or /research yet — those need the SQLite-side server's chat-client +
// admin-token plumbing, which is the iter-206+ work.
//
// Purpose: proves the path-2 storage rework works end-to-end through HTTP,
// and gives a clean benchmark surface against the existing SQLite server.
// Operators evaluating cosift's billion-scale capability run this against
// a Pebble store and compare /search latency + index size to the SQLite
// equivalent.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/embed"
	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/rerank"
	"github.com/calinteodor/cosift/internal/store"
)

func runPebbleServe(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("pebble-serve", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required; the SQLite cfg.DataDir is ignored)")
	addr := fs.String("addr", cfg.Server.Addr, "listen address (defaults to server.addr from cosift.json)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return errors.New("pebble-serve: -dir is required")
	}
	if *addr == "" {
		*addr = "127.0.0.1:7777"
	}

	ps, err := store.OpenPebble(*dir)
	if err != nil {
		return fmt.Errorf("open pebble: %w", err)
	}
	defer ps.Close()

	// Iter 286: log corpus size at startup so operators see whether the
	// store opened with the expected doc count (silent open would force them
	// to curl /stats just to confirm a restart loaded the right data dir).
	if _, indexedDocs, err := ps.CorpusStats(ctx); err == nil && indexedDocs > 0 {
		log.Printf("pebble-serve: opened store with %d indexed docs", indexedDocs)
	}

	// Iter 357/358: peek for persisted HNSW vectors. The iter-358 meta read
	// is 20 bytes (dim+nodeCount); the iter-357 first-entry probe falls back
	// when meta is absent but vector entries exist (edge case during a
	// partial persist). Loading the full graph stays a future-iter concern
	// — gigabytes of RAM at 10M-vector scale.
	hasVectors := false
	var vectorDim, vectorNodes int
	if meta, ok, err := index.LoadHNSWMeta(ctx, ps); err == nil && ok {
		hasVectors = true
		vectorDim = meta.Dim
		vectorNodes = meta.NodeCount
	} else {
		_ = ps.IterateVectorNodes(ctx, func(_ uint64, _ []byte) bool {
			hasVectors = true
			return false
		})
	}
	// Iter 362: optional graph load via COSIFT_LOAD_HNSW=true. Loading is
	// gigabytes of RAM at production scale (10M vectors × 1536 dim ≈ 60GB),
	// so it's opt-in. When the env var isn't set we just keep the cheap meta
	// snapshot from iter 358 and operators see has_vectors=true.
	var hnswGraph *index.HNSW
	if hasVectors && os.Getenv("COSIFT_LOAD_HNSW") == "true" {
		g, ok, err := index.LoadHNSW(ctx, ps)
		switch {
		case err != nil:
			log.Printf("pebble-serve: COSIFT_LOAD_HNSW=true but LoadHNSW failed: %v", err)
		case !ok:
			log.Printf("pebble-serve: COSIFT_LOAD_HNSW=true but no HNSW meta on store")
		default:
			hnswGraph = g
			log.Printf("pebble-serve: HNSW graph loaded into memory: %d nodes, dim=%d", g.Len(), vectorDim)
		}
	}
	if hasVectors {
		if vectorNodes > 0 {
			suffix := "(graph not loaded into memory; set COSIFT_LOAD_HNSW=true to load)"
			if hnswGraph != nil {
				suffix = "(graph loaded)"
			}
			log.Printf("pebble-serve: HNSW index present: %d nodes, dim=%d %s", vectorNodes, vectorDim, suffix)
		} else {
			log.Printf("pebble-serve: HNSW vector entries present (no meta blob — partial persist?)")
		}
	}

	// Iter 282: configurable HyDE + paraphrase cache caps (defaults 256).
	hydeCap := 256
	if v := os.Getenv("COSIFT_HYDE_CACHE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			hydeCap = n
			log.Printf("pebble-serve: HyDE cache size override = %d", n)
		} else {
			// Iter 314: surface unparseable env vars as warnings at startup
			// instead of silently keeping defaults. Catches typos like
			// COSIFT_HYDE_CACHE_SIZE="512m" or negative values.
			log.Printf("pebble-serve: WARN COSIFT_HYDE_CACHE_SIZE=%q is not a positive integer — using default %d", v, hydeCap)
		}
	}
	paraCap := 256
	if v := os.Getenv("COSIFT_PARA_CACHE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			paraCap = n
			log.Printf("pebble-serve: paraphrase cache size override = %d", n)
		} else {
			log.Printf("pebble-serve: WARN COSIFT_PARA_CACHE_SIZE=%q is not a positive integer — using default %d", v, paraCap)
		}
	}
	idx := index.NewPebbleBM25(ps)
	// Iter 279/301: COSIFT_BM25_K1 / COSIFT_BM25_B override per instance.
	// Shared with runQuery via applyBM25EnvOverrides so any PebbleBM25 built
	// from CLI or server honors the same env.
	o := applyBM25EnvOverrides(idx)
	if o.k1Set {
		log.Printf("pebble-serve: BM25 k1 override = %.2f", o.k1Val)
	}
	if o.bSet {
		log.Printf("pebble-serve: BM25 b override = %.2f", o.bVal)
	}
	if o.k1Bad != "" {
		log.Printf("pebble-serve: WARN COSIFT_BM25_K1=%q is not a positive float — using default %.2f", o.k1Bad, idx.K1())
	}
	if o.bBad != "" {
		log.Printf("pebble-serve: WARN COSIFT_BM25_B=%q is not a positive float — using default %.2f", o.bBad, idx.B())
	}
	srv := &pebbleHTTP{
		store:        ps,
		idx:          idx,
		hydeCache:    make(map[string]string, hydeCap),
		hydeCacheCap: hydeCap,
		paraCache:    make(map[string][]string, paraCap),
		paraCacheCap: paraCap,
		hasVectors:   hasVectors,
		vectorDim:    vectorDim,
		vectorNodes:  vectorNodes,
		hnsw:         hnswGraph,
		started:      time.Now(),
	}
	// Iter 240: optional /answer wiring. Uses the same OpenAI-compatible chat
	// client the SQLite-side server uses; works against OpenAI, Together,
	// Azure, llama.cpp, vLLM, Ollama, anything speaking /v1/chat/completions.
	if cfg.Chat.Model != "" {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		srv.chat = embed.NewOpenAIChat(apiKey, cfg.Chat.URL, cfg.Chat.Model)
		log.Printf("pebble-serve: /answer enabled (chat model=%s)", cfg.Chat.Model)
	}
	// Iter 363: embedder for ?retriever=dense. Built when cfg.Embeddings.Model
	// is set; same OPENAI key path as the chat client. Required alongside the
	// iter-362 loaded HNSW graph — having either alone gives a warning.
	if cfg.Embeddings.Model != "" {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		srv.embedder = embed.NewOpenAIClient(apiKey, cfg.Embeddings.URL, cfg.Embeddings.Model, cfg.Embeddings.Dim)
		log.Printf("pebble-serve: embedder configured (model=%s, dim=%d)", cfg.Embeddings.Model, cfg.Embeddings.Dim)
	}
	// Iter 248: wire rerank.Reranker when cfg.Rerank is configured. Two paths
	// (matching the SQLite-side): cfg.Rerank.URL → HTTPReranker (Cohere/Voyage/
	// Jina/TEI wire shape); otherwise cfg.Chat.Model present → LLMReranker
	// (RankGPT-style listwise via chat). /search?rerank=true gates the wrapper.
	if cfg.Rerank.URL != "" {
		apiKey := cfg.Rerank.APIKey
		if apiKey == "" {
			apiKey = os.Getenv("COHERE_API_KEY")
			if apiKey == "" {
				apiKey = os.Getenv("VOYAGE_API_KEY")
			}
		}
		srv.reranker = rerank.NewHTTPReranker(cfg.Rerank.URL, apiKey, cfg.Rerank.Model)
		log.Printf("pebble-serve: rerank enabled (http: %s, model=%s)", cfg.Rerank.URL, cfg.Rerank.Model)
	} else if cfg.Rerank.Enabled && srv.chat != nil {
		srv.reranker = rerank.NewLLMReranker(srv.chat)
		log.Printf("pebble-serve: rerank enabled (llm: %s)", cfg.Chat.Model)
	}
	srv.rerankCandK = cfg.Rerank.CandidateK
	if srv.rerankCandK <= 0 {
		srv.rerankCandK = 20
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.count(srv.handleHealthz))
	mux.HandleFunc("GET /stats", srv.count(srv.handleStats))
	mux.HandleFunc("GET /search", srv.count(srv.handleSearch))
	mux.HandleFunc("POST /search", srv.count(srv.handleSearchPOST))
	mux.HandleFunc("GET /contents", srv.count(srv.handleContents))
	mux.HandleFunc("POST /contents", srv.count(srv.handleContentsBatch))
	mux.HandleFunc("GET /verify", srv.count(srv.handleVerify))
	mux.HandleFunc("GET /metrics", srv.count(srv.handleMetrics))
	mux.HandleFunc("GET /find_similar", srv.count(srv.handleFindSimilar))
	mux.HandleFunc("POST /find_similar", srv.count(srv.handleFindSimilarPOST))
	mux.HandleFunc("GET /answer", srv.count(srv.handleAnswer))
	mux.HandleFunc("POST /answer", srv.count(srv.handleAnswerPOST))
	mux.HandleFunc("GET /research", srv.count(srv.handleResearch))
	mux.HandleFunc("POST /research", srv.count(srv.handleResearchPOST))

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
	}

	log.Printf("pebble-serve: listening on %s (PebbleStore at %s)", *addr, *dir)
	go func() {
		<-ctx.Done()
		log.Printf("pebble-serve: shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
	}()
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// pebbleHTTP holds the read-side handles. Stays small on purpose — the
// SQLite-side Server struct accumulated a lot of config knobs over many iters;
// the Pebble surface starts minimal and grows feature-by-feature.
type pebbleHTTP struct {
	store      *store.PebbleStore
	idx        *index.PebbleBM25
	chat       embed.ChatClient // nil when cfg.Chat.Model is unset; /answer returns 501
	reranker   rerank.Reranker  // nil when no rerank is configured; ?rerank=true is a no-op then
	rerankCandK int            // candidates pulled for rerank; default 20

	// Iter 259: bounded in-memory HyDE cache. /research?expand=true issues a
	// chat call PER sub-query, so a sticky workload (repeated queries, slow
	// sub-query rephrasings from the planner) hits the chat provider many
	// times for the same passage. Cap at 256 entries; on overflow drop one
	// arbitrary entry (Go map iteration order is randomized, good enough).
	hydeMu       sync.RWMutex
	hydeCache    map[string]string
	hydeCacheCap int // iter 282: env-configurable via COSIFT_HYDE_CACHE_SIZE

	// Iter 276: bounded paraphrase cache. /research?expand=paraphrase fans
	// out 3 paraphrases × N sub-queries — same hot path as HyDE but each
	// miss is 3x larger by output volume. Keyed on q only (fixed n=3 today).
	paraMu       sync.RWMutex
	paraCache    map[string][]string
	paraCacheCap int // iter 282: env-configurable via COSIFT_PARA_CACHE_SIZE
	paraHits     atomic.Int64
	paraMisses   atomic.Int64

	// Iter 260: atomic counters surfaced on /metrics so operators can size the
	// HyDE cache against a real workload — if misses dominate, raise the cap
	// or move to an L2 store; if hits dominate at a tight working set, the
	// cap is sufficient and you can mostly forget about it.
	hydeHits   atomic.Int64
	hydeMisses atomic.Int64

	// Iter 357: snapshot at startup of whether persisted HNSW vectors exist
	// in the 'v' family. Cheap peek (first-hit short-circuit). Surfaced on
	// /stats so operators know whether the store is ready for the future
	// ?retriever=dense path without needing to grep pebble-info.
	hasVectors bool
	// Iter 358: when an HNSW meta blob is persisted (the normal case),
	// surface dim + node count too. Cheap 20-byte read at startup.
	vectorDim   int
	vectorNodes int
	// Iter 362: optional in-memory HNSW graph for dense retrieval. Loaded
	// only when COSIFT_LOAD_HNSW=true at startup (gigabytes RAM at scale).
	// Nil = graph not loaded; /search?retriever=dense returns a warning.
	hnsw *index.HNSW
	// Iter 363: embedder client for query-time vectorization. Required by
	// /search?retriever=dense alongside s.hnsw. Built at startup when
	// cfg.Embeddings.Model is set. Nil → ?retriever=dense warns + falls back.
	embedder embed.Embedder

	// Iter 263: rerank attempt + failure counters. Rerank failures fall back
	// to BM25 order silently — that's the right reliability move, but without
	// a counter operators can't tell whether their LLM/HTTP reranker is
	// healthy or quietly broken.
	rerankAttempts atomic.Int64
	rerankFailures atomic.Int64

	// Iter 264: chat call attempt + failure counters across HyDE expansion,
	// /answer synth (sync + SSE), and /research plan + synth (sync + SSE).
	// A spike in failures (provider 429s, network blips) is the clearest
	// early signal that synth endpoints are degraded.
	chatAttempts atomic.Int64
	chatFailures atomic.Int64

	// Iter 294: count responses that carried at least one warning (iter 292/293).
	// Operators alerting on this catch 'a deploy started sending malformed
	// requests' without having to parse response bodies.
	warningsEmitted atomic.Int64

	// Iter 267: per-call chat duration sum. /metrics divides this by
	// chatAttempts to give mean chat latency, separated from the iter-262
	// per-endpoint duration. Diagnoses 'where did the seconds go' on a
	// slow /research stream: chat-side or retrieval-side?
	chatDurationNanos atomic.Int64

	// Iter 261/262: per-endpoint request counters + duration sums via a
	// counting middleware wrapping every mux entry. sync.Map keeps the hot
	// path lock-free; /metrics reads via Range. Path is the label so a
	// misrouted call (404) doesn't get counted under an existing endpoint.
	// rate(sum)/rate(count) over the duration sum gives mean latency in PromQL.
	requestCounts sync.Map // map[string]*endpointMetrics

	started    time.Time
}

type endpointMetrics struct {
	count    atomic.Int64
	sumNanos atomic.Int64
}

// count is the iter-261/262 request-counting middleware. Bumps a per-path
// {count, sumNanos} struct, lazily created on first request to a path. Hot
// path is sync.Map.Load + two atomic Adds — no contention even under high
// RPS. Duration is sampled after the handler returns so streaming endpoints
// account for their full open connection time.
func (s *pebbleHTTP) count(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path
		v, ok := s.requestCounts.Load(key)
		if !ok {
			v, _ = s.requestCounts.LoadOrStore(key, &endpointMetrics{})
		}
		m := v.(*endpointMetrics)
		m.count.Add(1)
		start := time.Now()
		h(w, r)
		m.sumNanos.Add(time.Since(start).Nanoseconds())
	}
}

func (s *pebbleHTTP) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *pebbleHTTP) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.store.Stats(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Iter 238: surface the iter-207 running counters here too so /stats is
	// the one canonical "shape of the index" call instead of "ask /stats for
	// doc count, then ask /metrics for average length". Both reads are O(1).
	sumLen, indexedDocs, _ := s.store.CorpusStats(r.Context())
	var avg float64
	if indexedDocs > 0 {
		avg = float64(sumLen) / float64(indexedDocs)
	}
	// Iter 280: surface config knobs that affect retrieval/synth quality so
	// operators can verify env overrides took effect from one endpoint.
	out := map[string]any{
		"documents":    st.Documents,
		"terms":        st.Terms,
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
		// Iter 345: surface the iter-282 cache caps so operators can verify
		// COSIFT_HYDE_CACHE_SIZE / _PARA_CACHE_SIZE overrides took effect.
		out["hyde_cache_size"] = s.hydeCacheCap
		out["paraphrase_cache_size"] = s.paraCacheCap
	}
	// Iter 357/358: signal whether the store has HNSW vectors persisted, and
	// (when meta is available) surface dim + node count. Cheap fields — meta
	// is 20 bytes, read once at startup.
	out["has_vectors"] = s.hasVectors
	if s.vectorNodes > 0 {
		out["vector_nodes"] = s.vectorNodes
		out["vector_dim"] = s.vectorDim
	}
	// Iter 362: whether the graph is loaded into memory for dense retrieval.
	out["hnsw_loaded"] = s.hnsw != nil
	// Iter 375: which retrievers actually work right now. Clients can read
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
	writeJSON(w, http.StatusOK, out)
}

// Iter 231: Prometheus-format scrape endpoint. Hand-written plain text
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
	fmt.Fprintf(w, "# HELP cosift_indexed_docs Number of documents passed through IndexDocument (iter-207 counter).\n")
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
	// Iter 260: HyDE cache effectiveness. Hits/misses both monotonic so
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
	// Iter 361: HNSW vector index shape (iter 358 cheap meta read). Gauges
	// rather than counters — these are static snapshots at startup.
	if s.vectorNodes > 0 {
		fmt.Fprintf(w, "# HELP cosift_vector_nodes Number of HNSW vector nodes persisted in the 'v' family.\n")
		fmt.Fprintf(w, "# TYPE cosift_vector_nodes gauge\n")
		fmt.Fprintf(w, "cosift_vector_nodes %d\n", s.vectorNodes)
		fmt.Fprintf(w, "# HELP cosift_vector_dim Embedding dimension of the persisted HNSW index.\n")
		fmt.Fprintf(w, "# TYPE cosift_vector_dim gauge\n")
		fmt.Fprintf(w, "cosift_vector_dim %d\n", s.vectorDim)
	}
	// Iter 261/262: per-endpoint request counters + duration sums. PromQL
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

// Iter 230: HTTP form of `cosift verify`. Same comparison (iter-207 running
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
		"ok":                     ok,
		"indexed_docs_counter":   counterCount,
		"indexed_docs_scan":      scanCount,
		"indexed_docs_drift":     driftCount,
		"sum_doc_len_counter":    counterSum,
		"sum_doc_len_scan":       scanSum,
		"sum_doc_len_drift":      driftSum,
	}
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, body)
}

// searchHit is the minimal hit shape returned by pebble-serve's /search.
// Intentionally narrower than the SQLite server's SearchHit — feature
// parity (highlight, excerpt, calibration, paragraph filters) grows as
// follow-up iters port each one through the Pebble side.
type searchHit struct {
	URL          string     `json:"url"`
	Title        string     `json:"title"`
	Score        float64    `json:"score"`
	Excerpt      string     `json:"excerpt,omitempty"`
	PublishedAt  *time.Time `json:"published_at,omitempty"`
	Author       string     `json:"author,omitempty"`
	Text         string     `json:"text,omitempty"`
}

type searchResponse struct {
	Query           string      `json:"query"`
	EffectiveQuery  string      `json:"effective_query,omitempty"`
	Expand          string      `json:"expand,omitempty"`
	Retriever       string      `json:"retriever"`
	Hits            []searchHit `json:"hits"`
	TotalCandidates int         `json:"total_candidates,omitempty"`
	Warnings        []string    `json:"warnings,omitempty"`
	Took            string      `json:"took"`
}

// Iter 277: POST /search with JSON body — for callers whose query lists,
// quoted phrases, or filter CSVs are awkward to URL-encode. Decodes into a
// searchRequest, re-encodes as r.URL.RawQuery, then calls handleSearch so
// every param the GET form supports works identically here. The hand-off
// shape (URL.Values) is the cosift contract — easier to keep one parser
// than to fork it across method handlers.
type searchRequest struct {
	Q              string `json:"q"`
	K              int    `json:"k,omitempty"`
	IncludeDomains string `json:"include_domains,omitempty"`
	ExcludeDomains string `json:"exclude_domains,omitempty"`
	Since          string `json:"since,omitempty"`
	Until          string `json:"until,omitempty"`
	Sort           string `json:"sort,omitempty"`
	Enrich         *bool  `json:"enrich,omitempty"`
	IncludeText    bool   `json:"include_text,omitempty"`
	Rerank         bool   `json:"rerank,omitempty"`
	Expand         string `json:"expand,omitempty"`
}

func (s *pebbleHTTP) handleSearchPOST(w http.ResponseWriter, r *http.Request) {
	var req searchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	v := url.Values{}
	if req.Q != "" {
		v.Set("q", req.Q)
	}
	if req.K > 0 {
		v.Set("k", strconv.Itoa(req.K))
	}
	if req.IncludeDomains != "" {
		v.Set("include_domains", req.IncludeDomains)
	}
	if req.ExcludeDomains != "" {
		v.Set("exclude_domains", req.ExcludeDomains)
	}
	if req.Since != "" {
		v.Set("since", req.Since)
	}
	if req.Until != "" {
		v.Set("until", req.Until)
	}
	if req.Sort != "" {
		v.Set("sort", req.Sort)
	}
	if req.Enrich != nil && !*req.Enrich {
		v.Set("enrich", "false")
	}
	if req.IncludeText {
		v.Set("include_text", "true")
	}
	if req.Rerank {
		v.Set("rerank", "true")
	}
	if req.Expand != "" {
		v.Set("expand", req.Expand)
	}
	r.URL.RawQuery = v.Encode()
	s.handleSearch(w, r)
}

// Iter 278: POST variants of /find_similar, /answer, /research. Same pattern
// as POST /search — re-encode JSON body as URL.Values, hand off to the GET
// handler. The GET handlers own the param semantics; POST is a wire-level
// alternative for callers whose payloads don't fit cleanly into a query string.

type findSimilarRequest struct {
	URL            string `json:"url,omitempty"`
	Text           string `json:"text,omitempty"`  // iter 298: content-based MLT, no source URL needed
	Title          string `json:"title,omitempty"` // iter 298: optional title-boost when using text mode
	K              int    `json:"k,omitempty"`
	Q              string `json:"q,omitempty"`
	IncludeDomains string `json:"include_domains,omitempty"`
	ExcludeDomains string `json:"exclude_domains,omitempty"`
	Since          string `json:"since,omitempty"`
	Until          string `json:"until,omitempty"`
	IncludeText    bool   `json:"include_text,omitempty"`
	Rerank         bool   `json:"rerank,omitempty"`
}

func (s *pebbleHTTP) handleFindSimilarPOST(w http.ResponseWriter, r *http.Request) {
	var req findSimilarRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	v := url.Values{}
	if req.URL != "" {
		v.Set("url", req.URL)
	}
	if req.Text != "" {
		v.Set("text", req.Text)
	}
	if req.Title != "" {
		v.Set("title", req.Title)
	}
	if req.K > 0 {
		v.Set("k", strconv.Itoa(req.K))
	}
	if req.Q != "" {
		v.Set("q", req.Q)
	}
	if req.IncludeDomains != "" {
		v.Set("include_domains", req.IncludeDomains)
	}
	if req.ExcludeDomains != "" {
		v.Set("exclude_domains", req.ExcludeDomains)
	}
	if req.Since != "" {
		v.Set("since", req.Since)
	}
	if req.Until != "" {
		v.Set("until", req.Until)
	}
	if req.IncludeText {
		v.Set("include_text", "true")
	}
	if req.Rerank {
		v.Set("rerank", "true")
	}
	r.URL.RawQuery = v.Encode()
	s.handleFindSimilar(w, r)
}

type synthRequest struct {
	Q              string `json:"q"`
	K              int    `json:"k,omitempty"`
	IncludeDomains string `json:"include_domains,omitempty"`
	ExcludeDomains string `json:"exclude_domains,omitempty"`
	Since          string `json:"since,omitempty"`
	Until          string `json:"until,omitempty"`
	IncludeText    bool   `json:"include_text,omitempty"`
	Rerank         bool   `json:"rerank,omitempty"`
	Expand         string `json:"expand,omitempty"`
	Stream         bool   `json:"stream,omitempty"`
}

func (req synthRequest) toValues() url.Values {
	v := url.Values{}
	if req.Q != "" {
		v.Set("q", req.Q)
	}
	if req.K > 0 {
		v.Set("k", strconv.Itoa(req.K))
	}
	if req.IncludeDomains != "" {
		v.Set("include_domains", req.IncludeDomains)
	}
	if req.ExcludeDomains != "" {
		v.Set("exclude_domains", req.ExcludeDomains)
	}
	if req.Since != "" {
		v.Set("since", req.Since)
	}
	if req.Until != "" {
		v.Set("until", req.Until)
	}
	if req.IncludeText {
		v.Set("include_text", "true")
	}
	if req.Rerank {
		v.Set("rerank", "true")
	}
	if req.Expand != "" {
		v.Set("expand", req.Expand)
	}
	if req.Stream {
		v.Set("stream", "true")
	}
	return v
}

func (s *pebbleHTTP) handleAnswerPOST(w http.ResponseWriter, r *http.Request) {
	var req synthRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	r.URL.RawQuery = req.toValues().Encode()
	s.handleAnswer(w, r)
}

func (s *pebbleHTTP) handleResearchPOST(w http.ResponseWriter, r *http.Request) {
	var req synthRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	r.URL.RawQuery = req.toValues().Encode()
	s.handleResearch(w, r)
}

func (s *pebbleHTTP) handleSearch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing q parameter")
		return
	}
	k := 10
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			k = n
		}
	}
	// Iter 232: include_domains / exclude_domains — mirrors the
	// SQLite-side server semantics (CSV, dot-boundary suffix match).
	// Iter 234: since / until — ISO-date filters on doc.PublishedAt.
	// When any filter is active we over-fetch so the post-filter has
	// enough candidates to fill k; same brute-force shape SQLite used
	// before it grew a proper index, fine at pebble-serve's scale.
	include := splitDomainsCSV(r.URL.Query().Get("include_domains"))
	exclude := splitDomainsCSV(r.URL.Query().Get("exclude_domains"))
	since, sinceErr := parseDateBound(r.URL.Query().Get("since"))
	if sinceErr != nil {
		writeProblem(w, http.StatusBadRequest, "since: "+sinceErr.Error())
		return
	}
	until, untilErr := parseDateBound(r.URL.Query().Get("until"))
	if untilErr != nil {
		writeProblem(w, http.StatusBadRequest, "until: "+untilErr.Error())
		return
	}
	dateFilter := !since.IsZero() || !until.IsZero()
	// Iter 248: rerank widens both the fetch and the keep-cap before filtering,
	// so the reranker sees a healthy candidate pool even with restrictive filters.
	wantRerank := r.URL.Query().Get("rerank") == "true" && s.reranker != nil
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	fetchK := keepCap
	if len(include) > 0 || len(exclude) > 0 || dateFilter {
		mult := 5
		if dateFilter {
			mult = 10
		}
		fetchK = keepCap * mult
		if fetchK > 500 {
			fetchK = 500
		}
	} else if wantRerank {
		fetchK = keepCap * 2
	}
	// Iter 252/272/274: expansion dispatch (bare / HyDE / paraphrase+RRF).
	// Iter 363/364/365: retriever dispatch (bm25 / dense / hybrid). Shared
	// helper used by /answer + /research so all three get the same matrix.
	// Reranker still scores against the original q regardless of strategy.
	expandMode := r.URL.Query().Get("expand")
	retrieverParam := r.URL.Query().Get("retriever")
	denseReady := s.hnsw != nil && s.embedder != nil
	hits, effectiveQuery, err := s.retrieve(r.Context(), q, fetchK, retrieverParam, expandMode)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Iter 233: enrich each surviving hit with Excerpt + PublishedAt + Author
	// via a single GetDocByURL per hit. Cost: k extra Gets — block-cache hot,
	// ~ms-scale at the k≤100 we accept. Opt out with ?enrich=false for callers
	// that only need scoring. The date filter (iter 234) forces a doc fetch
	// even when enrich=false, since PublishedAt lives on the gob.
	// Iter 237: ?include_text=true inlines doc.Text on each hit so research
	// pipelines avoid the N+1 round trip to /contents. Off by default —
	// payload size grows linearly with k and average doc length.
	enrich := r.URL.Query().Get("enrich") != "false"
	if wantRerank {
		enrich = true // rerank needs per-doc text — overrides enrich opt-out
	}
	includeText := r.URL.Query().Get("include_text") == "true"
	out := make([]searchHit, 0, keepCap)
	var rerankTexts []string
	if wantRerank {
		rerankTexts = make([]string, 0, keepCap)
	}
	for _, h := range hits {
		if len(include) > 0 || len(exclude) > 0 {
			host := hostOf(h.URL)
			if len(include) > 0 && !matchesAnyDomain(host, include) {
				continue
			}
			if len(exclude) > 0 && matchesAnyDomain(host, exclude) {
				continue
			}
		}
		hit := searchHit{URL: h.URL, Title: h.Title, Score: h.Score}
		if enrich || dateFilter || includeText {
			doc, derr := s.store.GetDocByURL(r.Context(), h.URL)
			if derr != nil || doc == nil {
				continue
			}
			if dateFilter {
				if doc.PublishedAt.IsZero() {
					continue // zero PublishedAt = unknown; drop under any date filter
				}
				if !since.IsZero() && doc.PublishedAt.Before(since) {
					continue
				}
				if !until.IsZero() && doc.PublishedAt.After(until) {
					continue
				}
			}
			if enrich {
				hit.Excerpt = textExcerpt(doc.Text, 320)
				if !doc.PublishedAt.IsZero() {
					t := doc.PublishedAt
					hit.PublishedAt = &t
				}
				hit.Author = doc.Author
			}
			if includeText {
				hit.Text = doc.Text
			}
			if wantRerank {
				rerankTexts = append(rerankTexts, doc.Title+"\n"+doc.Text)
			}
		}
		out = append(out, hit)
		if len(out) >= keepCap {
			break
		}
	}
	// Iter 248: rerank now that we have keepCap candidates with text.
	if wantRerank && len(out) > 1 && len(rerankTexts) == len(out) {
		cands := make([]rerank.Candidate, len(out))
		for i := range out {
			cands[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: rerankTexts[i]}
		}
		if order, rerr := s.doRerank(r.Context(), q, cands); rerr == nil && len(order) > 0 {
			reordered := make([]searchHit, 0, len(out))
			seen := make(map[int]bool, len(out))
			for _, id := range order {
				if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(out) && !seen[n] {
					reordered = append(reordered, out[n])
					seen[n] = true
				}
			}
			// Drop nothing: if the reranker omitted any IDs, append in original order.
			for i, h := range out {
				if !seen[i] {
					reordered = append(reordered, h)
				}
			}
			out = reordered
		}
	}
	// Iter 384: MMR diversification. ?mmr=<lambda> (0..1) reorders the
	// candidate pool to balance query relevance against diversity from
	// previously-selected hits. Requires HNSW (per-hit vectors) and an
	// embedder when the query vector wasn't already computed by the
	// dense/hybrid path. Composes after rerank — rerank gives quality
	// ordering, MMR diversifies it. warningsFor() handles the silent
	// fall-through when MMR was requested but couldn't fire.
	if mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr")); mmrSet && len(out) > 1 && s.hnsw != nil {
		var qVec []float32
		// Hybrid/dense path already embedded the query — try to reuse it.
		// For BM25-only path we need to embed q if an embedder is configured.
		if s.embedder != nil {
			if vecs, embErr := s.embedder.Embed(r.Context(), []string{q}); embErr == nil && len(vecs) > 0 {
				qVec = vecs[0]
			}
		}
		if len(qVec) > 0 {
			hitVecs := make([][]float32, len(out))
			for i := range out {
				if v, ok := s.hnsw.LookupVectorByURL(out[i].URL); ok {
					hitVecs[i] = v
				}
			}
			out = mmrSelect(qVec, out, hitVecs, mmrLambda)
		}
	}
	if len(out) > k {
		out = out[:k]
	}
	// Iter 235: sort=date_desc | date_asc | relevance (default). Applies to
	// the already-collected top-k pool; raise k to widen the pool before
	// re-sorting if you need more candidates for date ordering.
	switch r.URL.Query().Get("sort") {
	case "date_desc":
		sortHitsByDate(out, false)
	case "date_asc":
		sortHitsByDate(out, true)
	}
	// Iter 363/364/366: label centralized in buildRetrieverLabel so /answer
	// and /research report the same vocabulary as /search.
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, effectiveQuery != q, wantRerank)
	// Iter 384: mmr suffix when diversification actually fired (HNSW loaded
	// + embedder available). Same conditional as the apply site above —
	// the label tracks the real pipeline.
	if mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr")); mmrSet && s.hnsw != nil && s.embedder != nil && mmrLambda < 1.0 {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	resp := searchResponse{
		Query:           q,
		Expand:          normalizeExpandMode(expandMode),
		Retriever:       retrieverLabel,
		Hits:            out,
		// Iter 283: total_candidates = BM25 candidates considered before
		// filter (capped at fetchK). Operators tuning over-fetch can see
		// whether their filter is dropping a lot — when out=k but
		// total_candidates is close to fetchK, the filter is restrictive
		// enough that you may want to raise k or relax filters.
		TotalCandidates: len(hits),
		Took:            time.Since(start).String(),
	}
	// Iter 265: surface the post-HyDE query when it actually changed, so callers
	// can debug whether expand=true contributed any extra terms or returned q
	// unchanged (chat down, empty passage, etc).
	if effectiveQuery != q {
		resp.EffectiveQuery = effectiveQuery
	}
	resp.Warnings = s.warningsFor(r)
	writeJSON(w, http.StatusOK, resp)
}

// sortHitsByDate orders hits by PublishedAt. Hits with no PublishedAt sink to
// the end regardless of direction — they have no comparable value. asc=true
// gives oldest-first; asc=false gives newest-first. Iter 235.
func sortHitsByDate(hits []searchHit, asc bool) {
	sort.SliceStable(hits, func(i, j int) bool {
		ai := hits[i].PublishedAt != nil
		aj := hits[j].PublishedAt != nil
		if ai != aj {
			return ai // hits with dates come before hits without
		}
		if !ai {
			return false
		}
		if asc {
			return hits[i].PublishedAt.Before(*hits[j].PublishedAt)
		}
		return hits[i].PublishedAt.After(*hits[j].PublishedAt)
	})
}

// Iter 236: BM25-only "more like this". Mirrors EXA's /findSimilar shape but
// stays dependency-free: no embeddings required. Algorithm is Lucene's MLT —
// pick the source doc's top-N terms by tf·idf, build a query from them, run
// the existing BM25 search, drop the source URL itself. With dense vectors
// off the table on the Pebble path (HNSW indexing during crawl is the iter
// follow-up), this is the cheapest credible /find_similar we can ship.
func (s *pebbleHTTP) handleFindSimilar(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	// Iter 298: accept either ?url= (existing behavior) or ?text= (content-
	// based similarity for unindexed drafts). text path skips the source-URL
	// exclusion since there's no source URL to exclude.
	rawURL := r.URL.Query().Get("url")
	rawText := r.URL.Query().Get("text")
	if rawURL == "" && rawText == "" {
		writeProblem(w, http.StatusBadRequest, "missing url or text parameter")
		return
	}
	var srcTitle, srcText, srcURL string
	if rawURL != "" {
		decoded, err := url.QueryUnescape(rawURL)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, "url parameter is not URL-encoded")
			return
		}
		src, err := s.store.GetDocByURL(r.Context(), decoded)
		if errors.Is(err, store.ErrNotFound) {
			writeProblem(w, http.StatusNotFound, "url not in index")
			return
		}
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, err.Error())
			return
		}
		srcTitle, srcText, srcURL = src.Title, src.Text, src.URL
	} else {
		srcText = rawText
		srcTitle = r.URL.Query().Get("title") // optional title boost when text mode
	}
	k := 10
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			k = n
		}
	}

	// Tokenize title (×3) and body; mirror IndexDocument's title boost so
	// title terms dominate the similarity query.
	tf := make(map[string]int, 256)
	for _, t := range index.Tokenize(srcTitle) {
		tf[t] += 3
	}
	for _, t := range index.Tokenize(srcText) {
		tf[t]++
	}

	_, n, _ := s.store.CorpusStats(r.Context())
	if n <= 0 {
		n = 1
	}
	logN := math.Log(float64(n))

	type termScore struct {
		term  string
		score float64
	}
	scored := make([]termScore, 0, len(tf))
	for term, freq := range tf {
		info, ok, err := s.store.GetTermInfo(r.Context(), term)
		if err != nil || !ok || info.DocFreq <= 0 {
			continue
		}
		idf := logN - math.Log(float64(info.DocFreq))
		if idf <= 0 {
			continue // term appears in every doc; useless signal
		}
		scored = append(scored, termScore{term: term, score: float64(freq) * idf})
	}
	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })

	topN := 10
	if len(scored) < topN {
		topN = len(scored)
	}
	if topN == 0 {
		// Iter 322: previously referenced `decoded` (only in scope in the
		// url-mode branch) — broken since iter 298's text-mode addition.
		// Use srcURL when present, else echo the bare text/title as the
		// source identifier so the empty response still tells the caller
		// what was searched.
		emptyQuery := srcURL
		if emptyQuery == "" {
			emptyQuery = srcTitle
			if emptyQuery == "" {
				emptyQuery = srcText
			}
		}
		writeJSON(w, http.StatusOK, searchResponse{
			Query: emptyQuery, Retriever: "bm25-mlt",
			Hits: []searchHit{}, Took: time.Since(start).String(),
		})
		return
	}
	terms := make([]string, topN)
	for i := 0; i < topN; i++ {
		terms[i] = scored[i].term
	}
	queryStr := strings.Join(terms, " ")
	// Iter 239: optional ?q= augments the auto-derived MLT query so callers
	// can constrain "more like this URL" with an extra concept (e.g.
	// /find_similar?url=...&q=pricing). Appended verbatim — supports the
	// same quoted-phrase shape /search accepts.
	if extra := strings.TrimSpace(r.URL.Query().Get("q")); extra != "" {
		queryStr = queryStr + " " + extra
	}

	// Iter 245: scope MLT with the same retrieval filters /search and /answer
	// accept. 'find pages similar to X but only on docs.example.com' is the
	// archetype EXA findSimilar shape. Over-fetch enough that the source
	// exclusion + filter still fills k.
	include := splitDomainsCSV(r.URL.Query().Get("include_domains"))
	exclude := splitDomainsCSV(r.URL.Query().Get("exclude_domains"))
	since, sinceErr := parseDateBound(r.URL.Query().Get("since"))
	if sinceErr != nil {
		writeProblem(w, http.StatusBadRequest, "since: "+sinceErr.Error())
		return
	}
	until, untilErr := parseDateBound(r.URL.Query().Get("until"))
	if untilErr != nil {
		writeProblem(w, http.StatusBadRequest, "until: "+untilErr.Error())
		return
	}
	dateFilter := !since.IsZero() || !until.IsZero()
	includeText := r.URL.Query().Get("include_text") == "true"
	// Iter 251: ?rerank=true closes /find_similar's parity gap with the other
	// retrieval endpoints. The MLT query is auto-derived from the source doc,
	// so reranking the candidate neighbors against THAT query is exactly what
	// EXA's findSimilar quality boost is doing under the hood.
	wantRerank := r.URL.Query().Get("rerank") == "true" && s.reranker != nil
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	fetchK := keepCap + 1 // +1 to absorb the source-URL exclusion
	if len(include) > 0 || len(exclude) > 0 || dateFilter {
		mult := 5
		if dateFilter {
			mult = 10
		}
		fetchK = keepCap * mult
		if fetchK > 500 {
			fetchK = 500
		}
	} else if wantRerank {
		fetchK = keepCap * 2
	}

	// Iter 371/373: /find_similar?retriever=dense reuses the source's
	// persisted vector (URL-mode) or embeds the user's text (text-mode) and
	// runs an HNSW cosine search instead of BM25-MLT. ?retriever=hybrid
	// (iter 373) runs BOTH BM25-MLT and dense, then RRF-fuses — the
	// strongest "find similar" signal: lexical precision + semantic recall.
	//
	// Requires COSIFT_LOAD_HNSW=true at server start (for the graph);
	// text-mode additionally needs a configured embedder. URL-mode dense
	// works without an embedder — the source vector is already in the graph
	// from indexing. Missing requirements fall through to BM25-MLT;
	// warningsFor() flags it (iter 372 carves out the URL-mode-no-embedder
	// case so the warning isn't misleading).
	retrieverParam := r.URL.Query().Get("retriever")
	useDense := retrieverParam == "dense" && s.hnsw != nil
	useHybrid := retrieverParam == "hybrid" && s.hnsw != nil
	var (
		hits        []index.Hit
		denseFired  bool
		bm25Fired   bool
	)
	// Helper: fetch the query vector (URL-mode lookup, falling back to
	// text-mode embed when allowed). Returns ok=false when neither path
	// produced a usable vector.
	getQueryVec := func() ([]float32, bool) {
		if srcURL != "" {
			if v, ok := s.hnsw.LookupVectorByURL(srcURL); ok {
				return v, true
			}
		}
		if s.embedder != nil && (srcText != "" || srcTitle != "") {
			seed := strings.TrimSpace(srcTitle + " " + srcText)
			if seed != "" {
				vecs, embErr := s.embedder.Embed(r.Context(), []string{seed})
				if embErr == nil && len(vecs) > 0 {
					return vecs[0], true
				}
			}
		}
		return nil, false
	}
	if useDense {
		if queryVec, ok := getQueryVec(); ok {
			vhits := s.hnsw.Search(r.Context(), queryVec, fetchK)
			hits = make([]index.Hit, len(vhits))
			for i, vh := range vhits {
				hits[i] = index.Hit{URL: vh.URL, Title: vh.Title, Score: vh.Score}
			}
			denseFired = true
		}
	} else if useHybrid {
		// Run BM25-MLT and dense in series; RRF-fuse the two ranked lists.
		// Each retriever votes fetchK candidates so the fused top-fetchK has
		// room to balance both signals before filter/enrich/rerank.
		bm25Hits, bm25Err := s.idx.Search(r.Context(), queryStr, fetchK)
		if bm25Err != nil {
			writeProblem(w, http.StatusInternalServerError, bm25Err.Error())
			return
		}
		bm25Fired = true
		if queryVec, ok := getQueryVec(); ok {
			vhits := s.hnsw.Search(r.Context(), queryVec, fetchK)
			denseHits := make([]index.Hit, len(vhits))
			for i, vh := range vhits {
				denseHits[i] = index.Hit{URL: vh.URL, Title: vh.Title, Score: vh.Score}
			}
			hits = rrfFuse([][]index.Hit{bm25Hits, denseHits}, 60)
			if len(hits) > fetchK {
				hits = hits[:fetchK]
			}
			denseFired = true
		} else {
			// Hybrid fell through to BM25-only (e.g. text-mode + no embedder).
			// Keep the BM25 hits we already paid for.
			hits = bm25Hits
		}
	}
	if !denseFired && !bm25Fired {
		var err error
		hits, err = s.idx.Search(r.Context(), queryStr, fetchK)
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	type fsCand struct {
		hit        searchHit
		rerankText string
	}
	cands := make([]fsCand, 0, keepCap)
	for _, h := range hits {
		if srcURL != "" && h.URL == srcURL {
			continue
		}
		if len(include) > 0 || len(exclude) > 0 {
			host := hostOf(h.URL)
			if len(include) > 0 && !matchesAnyDomain(host, include) {
				continue
			}
			if len(exclude) > 0 && matchesAnyDomain(host, exclude) {
				continue
			}
		}
		hit := searchHit{URL: h.URL, Title: h.Title, Score: h.Score}
		doc, derr := s.store.GetDocByURL(r.Context(), h.URL)
		if derr != nil || doc == nil {
			continue
		}
		if dateFilter {
			if doc.PublishedAt.IsZero() {
				continue
			}
			if !since.IsZero() && doc.PublishedAt.Before(since) {
				continue
			}
			if !until.IsZero() && doc.PublishedAt.After(until) {
				continue
			}
		}
		hit.Excerpt = textExcerpt(doc.Text, 320)
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt
			hit.PublishedAt = &t
		}
		hit.Author = doc.Author
		if includeText {
			hit.Text = doc.Text
		}
		c := fsCand{hit: hit}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
		if len(cands) >= keepCap {
			break
		}
	}
	if wantRerank && len(cands) > 1 {
		rc := make([]rerank.Candidate, len(cands))
		for i := range cands {
			rc[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: cands[i].rerankText}
		}
		if order, rerr := s.doRerank(r.Context(), queryStr, rc); rerr == nil && len(order) > 0 {
			reordered := make([]fsCand, 0, len(cands))
			seen := make(map[int]bool, len(cands))
			for _, id := range order {
				if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(cands) && !seen[n] {
					reordered = append(reordered, cands[n])
					seen[n] = true
				}
			}
			for i, c := range cands {
				if !seen[i] {
					reordered = append(reordered, c)
				}
			}
			cands = reordered
		}
	}
	// Iter 386: MMR diversification on /find_similar. Anchored at the
	// source vector — URL-mode reuses the persisted vec, text-mode embeds
	// the seed via getQueryVec (defined above for dense/hybrid dispatch).
	// Without an explicit user query, the source IS the query — this is
	// the right anchor for "find similar but diverse".
	mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr"))
	mmrFired := false
	if mmrSet && len(cands) > 1 && s.hnsw != nil {
		if qVec, ok := getQueryVec(); ok {
			hitVecs := make([][]float32, len(cands))
			for i, c := range cands {
				if v, vok := s.hnsw.LookupVectorByURL(c.hit.URL); vok {
					hitVecs[i] = v
				}
			}
			order := mmrOrder(qVec, hitVecs, mmrLambda)
			if order != nil {
				reordered := make([]fsCand, len(order))
				for i, idx := range order {
					reordered[i] = cands[idx]
				}
				cands = reordered
				mmrFired = true
			}
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	out := make([]searchHit, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.hit)
	}
	// Iter 371/373: label tracks which retrievers actually fired.
	//   bm25-mlt              — default, BM25 only
	//   dense                 — ?retriever=dense fired (graph + vec found)
	//   bm25-mlt+dense:rrf    — ?retriever=hybrid, both BM25 and dense fired
	// If dense was requested but fell through (no graph, no vec), the label
	// stays bm25-mlt and warningsFor() carries the reason.
	retrieverLabel := "bm25-mlt"
	switch {
	case bm25Fired && denseFired:
		retrieverLabel = "bm25-mlt+dense:rrf"
	case denseFired:
		retrieverLabel = "dense"
	}
	if wantRerank {
		retrieverLabel += "+rerank:" + s.reranker.Name()
	}
	if mmrFired {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	writeJSON(w, http.StatusOK, searchResponse{
		Query:           queryStr,
		Retriever:       retrieverLabel,
		Hits:            out,
		TotalCandidates: len(hits),
		Warnings:        s.warningsFor(r),
		Took:            time.Since(start).String(),
	})
}

// Iter 240: /answer — synthesis over BM25 retrieval. Mirrors the SQLite-side
// /answer in spirit (top-k sources → cited synthesis) but stays minimal: no
// streaming, no rerank, no query expansion. Those land in follow-up iters
// once this surface is exercised. Returns 501 when no chat model is
// configured so the absent capability fails loud instead of silent.
const answerSystemPrompt = `You are a research assistant. Answer the user's question using ONLY the provided sources.
- Cite sources by their numeric id in square brackets, e.g. [1] or [2,3]. Every factual claim needs a citation.
- If the sources do not contain the answer, say so plainly. Do not invent facts.
- Keep the answer focused on what the sources actually say; do not pad.`

// Iter 252: HyDE-style query expansion prompt. Borrowed verbatim from
// internal/server/hyde.go so the SQLite and Pebble paths produce comparable
// expansions and operators don't have to learn two prompt shapes.
const hydeSystemPrompt = `Write a brief, factual passage (2-4 sentences) that would directly answer the user's question. Output ONLY the passage — no preamble, no commentary, no apology if you're uncertain. If the question is ambiguous, pick the most plausible interpretation and answer that. The passage doesn't need to be true; it needs to be the SHAPE of what a relevant document would say. Embedding this passage and searching by its vector will find documents that look like real answers, even if the user's original query was just a few keywords.`

// doChat wraps ChatClient.Chat with attempt/failure counters. Iter 264.
// Takes the client as a parameter so it works for both s.chat and the
// StreamingChatClient passed into streamResearch/streamAnswer.
func (s *pebbleHTTP) doChat(ctx context.Context, c embed.ChatClient, msgs []embed.ChatMsg) (string, error) {
	s.chatAttempts.Add(1)
	start := time.Now()
	out, err := c.Chat(ctx, msgs)
	s.chatDurationNanos.Add(time.Since(start).Nanoseconds())
	if err != nil {
		s.chatFailures.Add(1)
	}
	return out, err
}

// doChatStream wraps StreamingChatClient.ChatStream with the same counters.
func (s *pebbleHTTP) doChatStream(ctx context.Context, c embed.StreamingChatClient, msgs []embed.ChatMsg, onChunk func(string)) (string, error) {
	s.chatAttempts.Add(1)
	start := time.Now()
	out, err := c.ChatStream(ctx, msgs, onChunk)
	s.chatDurationNanos.Add(time.Since(start).Nanoseconds())
	if err != nil {
		s.chatFailures.Add(1)
	}
	return out, err
}

// doRerank wraps s.reranker.Rerank with attempt/failure counters. Iter 263.
// Returning the original error unchanged so callers keep their existing
// silent-fallback behavior; only the operator-side visibility changed.
func (s *pebbleHTTP) doRerank(ctx context.Context, q string, cands []rerank.Candidate) ([]string, error) {
	s.rerankAttempts.Add(1)
	order, err := s.reranker.Rerank(ctx, q, cands)
	if err != nil {
		s.rerankFailures.Add(1)
	}
	return order, err
}

// Iter 301: applyBM25EnvOverrides reads COSIFT_BM25_K1 / _B and applies them
// to idx. Shared by runPebbleServe and runQuery so both honor the same env.
// Returns which knobs landed so callers can log selectively.
type bm25EnvResult struct {
	k1Set, bSet     bool
	k1Val, bVal     float64
	k1Bad, bBad     string // iter 314: non-empty when env was set but unparseable
}

func applyBM25EnvOverrides(idx *index.PebbleBM25) bm25EnvResult {
	var out bm25EnvResult
	if v := os.Getenv("COSIFT_BM25_K1"); v != "" {
		if k1, err := strconv.ParseFloat(v, 64); err == nil && k1 > 0 {
			idx.WithBM25Params(k1, 0)
			out.k1Set, out.k1Val = true, k1
		} else {
			out.k1Bad = v
		}
	}
	if v := os.Getenv("COSIFT_BM25_B"); v != "" {
		if b, err := strconv.ParseFloat(v, 64); err == nil && b > 0 {
			idx.WithBM25Params(0, b)
			out.bSet, out.bVal = true, b
		} else {
			out.bBad = v
		}
	}
	return out
}

// Iter 272: paraphraseQuery returns up to n paraphrases of q via the chat
// client, parses the JSON array shape the SQLite-side paraphraser already
// uses, and falls back to nil on any failure (no chat client, empty / malformed
// reply, parse error). Caller decides what to do with an empty list — the
// downstream RRF strategy treats it as 'no expansion, fall back to single
// query'. No cache yet — keyed on (q, n) would need a different shape than
// the iter-259 HyDE cache; revisit when workload justifies.
func (s *pebbleHTTP) paraphraseQuery(ctx context.Context, q string, n int) []string {
	if s.chat == nil || n <= 0 {
		return nil
	}
	s.paraMu.RLock()
	if cached, ok := s.paraCache[q]; ok {
		s.paraMu.RUnlock()
		s.paraHits.Add(1)
		return cached
	}
	s.paraMu.RUnlock()
	s.paraMisses.Add(1)
	const sys = `Generate paraphrases of a search query. Each paraphrase preserves the semantic intent but uses different vocabulary — different keywords that a target document might also use. Output ONLY a JSON array of strings.
Example output for "go programming language": ["golang concurrent compiled language", "Google's systems programming language with goroutines"]`
	resp, err := s.doChat(ctx, s.chat, []embed.ChatMsg{
		{Role: "system", Content: sys},
		{Role: "user", Content: fmt.Sprintf("Generate %d paraphrases of: %s", n, q)},
	})
	if err != nil {
		return nil
	}
	raw := strings.TrimSpace(resp)
	for _, fence := range []string{"```json", "```"} {
		raw = strings.TrimPrefix(raw, fence)
		raw = strings.TrimSuffix(raw, "```")
	}
	raw = strings.TrimSpace(raw)
	startIdx := strings.Index(raw, "[")
	endIdx := strings.LastIndex(raw, "]")
	if startIdx < 0 || endIdx <= startIdx {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(raw[startIdx:endIdx+1]), &arr); err != nil {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, p := range arr {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}
	if len(out) > 0 {
		s.paraMu.Lock()
		if len(s.paraCache) >= s.paraCacheCap {
			for k := range s.paraCache {
				delete(s.paraCache, k)
				break
			}
		}
		s.paraCache[q] = out
		s.paraMu.Unlock()
	}
	return out
}

// Iter 272: rrfFuse implements Reciprocal Rank Fusion across N ranked lists.
// k=60 is the standard Cormack et al. constant; tweaking rarely changes top-k
// ordering meaningfully. Each list contributes 1/(k + rank+1) to a URL's
// fused score; URLs appearing in more lists at higher ranks rise. Returns
// synthesized Hits ordered by fused score (URL/Title from first encounter,
// Score = fused RRF score).
func rrfFuse(lists [][]index.Hit, fuseK int) []index.Hit {
	if fuseK <= 0 {
		fuseK = 60
	}
	type fused struct {
		hit   index.Hit
		score float64
	}
	scored := make(map[string]*fused, 64)
	for _, list := range lists {
		for rank, h := range list {
			if existing, ok := scored[h.URL]; ok {
				existing.score += 1.0 / float64(fuseK+rank+1)
			} else {
				scored[h.URL] = &fused{hit: h, score: 1.0 / float64(fuseK+rank+1)}
			}
		}
	}
	flat := make([]*fused, 0, len(scored))
	for _, f := range scored {
		flat = append(flat, f)
	}
	sort.Slice(flat, func(i, j int) bool { return flat[i].score > flat[j].score })
	out := make([]index.Hit, len(flat))
	for i, f := range flat {
		out[i] = f.hit
		out[i].Score = f.score
	}
	return out
}

// Iter 292: warningsFor surfaces silent no-ops that callers used to have to
// derive from absent effective_query / retriever fields. Each warning is one
// human-readable sentence; consumers programmatically inspect the slice.
func (s *pebbleHTTP) warningsFor(r *http.Request) []string {
	var w []string
	if mode := r.URL.Query().Get("expand"); mode != "" {
		if s.chat == nil {
			w = append(w, "expand="+mode+" requested but no chat client configured (set cfg.Chat.Model)")
		} else if normalizeExpandMode(mode) == "" {
			// Iter 309: unknown expand value used to be silently ignored.
			w = append(w, "expand="+mode+" is not a known strategy (try: hyde, paraphrase) — treated as no expansion")
		}
	}
	if r.URL.Query().Get("rerank") == "true" && s.reranker == nil {
		w = append(w, "rerank=true requested but no reranker configured (set cfg.Rerank.URL or cfg.Rerank.Enabled)")
	}
	// Iter 363/364: dense + hybrid retrievers need both a loaded HNSW graph
	// and an embedder. Missing either falls through to BM25 with a warning.
	// Iter 372: /find_similar?url=X URL-mode-dense reads the source vector
	// directly from the graph via LookupVectorByURL — no embed RPC. Skip the
	// embedder warning in that case (would otherwise be misleading: the
	// request succeeded without an embedder).
	// Iter 379: /find_similar falls back to bm25-mlt (not bm25), so the
	// warning says so — caller's retriever label matches the warning text.
	if rv := r.URL.Query().Get("retriever"); rv == "dense" || rv == "hybrid" {
		isFindSimilar := strings.HasSuffix(r.URL.Path, "/find_similar")
		isFindSimilarURLMode := isFindSimilar && r.URL.Query().Get("url") != ""
		fallback := "BM25"
		if isFindSimilar {
			fallback = "BM25-MLT"
		}
		switch {
		case s.hnsw == nil:
			w = append(w, "retriever="+rv+" requested but HNSW graph not loaded (set COSIFT_LOAD_HNSW=true at server start) — fell back to "+fallback)
		case s.embedder == nil && !isFindSimilarURLMode:
			w = append(w, "retriever="+rv+" requested but no embedder configured (set cfg.Embeddings.Model) — fell back to "+fallback)
		}
	}
	// Iter 384: MMR diversification needs HNSW + embedder. Bad values (non-
	// float, out of [0,1]) fall through silently — flag them. Missing
	// requirements also warn.
	// Iter 386: /find_similar?url=X reuses the source vector from the graph
	// for the MMR anchor — no embedder needed. Same carve-out as iter 372
	// for the retriever warning.
	if raw := r.URL.Query().Get("mmr"); raw != "" {
		isFindSimilarURLMode := strings.HasSuffix(r.URL.Path, "/find_similar") && r.URL.Query().Get("url") != ""
		if _, ok := parseMMRLambda(raw); !ok {
			w = append(w, "mmr="+raw+" is not a float in [0,1] — diversification skipped")
		} else if s.hnsw == nil {
			w = append(w, "mmr requires HNSW graph (set COSIFT_LOAD_HNSW=true at server start) — diversification skipped")
		} else if s.embedder == nil && !isFindSimilarURLMode {
			w = append(w, "mmr requires an embedder to vectorize the query (set cfg.Embeddings.Model) — diversification skipped")
		}
	}
	// Iter 310: catch unknown ?sort= values (silently treated as relevance).
	if sortVal := r.URL.Query().Get("sort"); sortVal != "" {
		switch sortVal {
		case "relevance", "date_desc", "date_asc":
			// valid
		default:
			w = append(w, "sort="+sortVal+" is not a known mode (try: relevance, date_desc, date_asc) — treated as relevance")
		}
	}
	// Iter 311: catch obviously-bad ?k= values that silently fell back to the
	// per-endpoint default. Upper-bound clamping varies per endpoint so we
	// flag only the universally-invalid cases (non-integer, zero, negative).
	if kVal := r.URL.Query().Get("k"); kVal != "" {
		if n, err := strconv.Atoi(kVal); err != nil || n <= 0 {
			w = append(w, "k="+kVal+" is not a positive integer — using server default")
		}
	}
	// Iter 313: catch URL-shaped values in domain filters. Users sometimes
	// pass 'https://example.com/foo' when 'example.com' was wanted; the
	// dot-boundary matcher silently drops every result.
	for _, key := range []string{"include_domains", "exclude_domains"} {
		if v := r.URL.Query().Get(key); v != "" {
			for _, d := range strings.Split(v, ",") {
				d = strings.TrimSpace(d)
				if strings.Contains(d, "://") || strings.Contains(d, "/") {
					w = append(w, key+" entry "+d+" looks like a URL — pass a hostname (e.g., example.com), not a URL")
					break // one warning per filter list is enough
				}
			}
		}
	}
	if len(w) > 0 {
		s.warningsEmitted.Add(1)
	}
	return w
}

// normalizeExpandMode returns the canonical strategy name for an `expand`
// param value: "true" → "hyde" (alias), "hyde"/"paraphrase" pass through,
// anything else → "" (so the response field doesn't echo a value that had
// no effect). Iter 308.
func normalizeExpandMode(raw string) string {
	switch raw {
	case "true", "hyde":
		return "hyde"
	case "paraphrase":
		return "paraphrase"
	default:
		return ""
	}
}

// parseMMRLambda parses ?mmr= as a float in [0, 1]. Returns (lambda, true)
// when valid. Empty value or out-of-range returns (0, false) so the caller
// can short-circuit without firing MMR. Iter 384.
func parseMMRLambda(raw string) (float64, bool) {
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	if v < 0 || v > 1 {
		return 0, false
	}
	return v, true
}

// cosineUnit returns the dot product of two unit-normalized float32 slices.
// HNSW stores vectors in unit form, so this is equivalent to cosine similarity.
// Returns 0 on length mismatch — caller is responsible for filtering missing
// vectors before calling. Iter 384.
func cosineUnit(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var s float32
	i := 0
	for ; i <= len(a)-4; i += 4 {
		s += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
	}
	for ; i < len(a); i++ {
		s += a[i] * b[i]
	}
	return s
}

// mmrOrder computes a permutation of [0..n-1] via Maximal Marginal Relevance
// (Carbonell & Goldstein '98). At each step picks the candidate maximizing
//
//	score = λ · sim(q, c) - (1-λ) · max_{s∈selected} sim(c, s)
//
// Candidates whose vectors are missing (zero-length) score 0 on relevance
// and 0 against any other missing-vec candidate — MMR can only diversify
// what it can compare. lambda=1 → pure relevance (no diversification);
// lambda=0 → pure diversity. 0.5–0.7 is a typical starting point.
//
// Returns the permutation as a []int; never drops anything. The caller
// applies it to whatever URL-keyed slice it has. Iter 384/385.
func mmrOrder(qVec []float32, hitVecs [][]float32, lambda float64) []int {
	n := len(hitVecs)
	if n == 0 {
		return nil
	}
	selected := make([]bool, n)
	order := make([]int, 0, n)
	rel := make([]float64, n)
	for i := range hitVecs {
		if len(hitVecs[i]) > 0 {
			rel[i] = float64(cosineUnit(qVec, hitVecs[i]))
		}
	}
	for len(order) < n {
		bestI := -1
		bestScore := -math.MaxFloat64
		for i := 0; i < n; i++ {
			if selected[i] {
				continue
			}
			var maxSim float64
			for _, j := range order {
				if len(hitVecs[i]) == 0 || len(hitVecs[j]) == 0 {
					continue
				}
				sim := float64(cosineUnit(hitVecs[i], hitVecs[j]))
				if sim > maxSim {
					maxSim = sim
				}
			}
			score := lambda*rel[i] - (1.0-lambda)*maxSim
			if score > bestScore {
				bestScore = score
				bestI = i
			}
		}
		if bestI < 0 {
			break
		}
		selected[bestI] = true
		order = append(order, bestI)
	}
	return order
}

// mmrSelect — thin /search wrapper around mmrOrder that operates on []searchHit
// directly. Iter 384.
func mmrSelect(qVec []float32, hits []searchHit, hitVecs [][]float32, lambda float64) []searchHit {
	if len(hits) <= 1 || lambda >= 1.0 {
		return hits
	}
	order := mmrOrder(qVec, hitVecs, lambda)
	out := make([]searchHit, 0, len(hits))
	for _, i := range order {
		out = append(out, hits[i])
	}
	return out
}

// applyMMRPermutation embeds q, looks up per-URL vectors from HNSW, and
// returns an MMR permutation. Returns nil when MMR can't fire (no graph,
// no embedder, embed failed, λ≥1, single-element pool). Shared by /answer
// and /research synth endpoints. Iter 385.
func (s *pebbleHTTP) applyMMRPermutation(ctx context.Context, urls []string, q string, lambda float64) []int {
	if s.hnsw == nil || s.embedder == nil || len(urls) <= 1 || lambda >= 1.0 {
		return nil
	}
	vecs, err := s.embedder.Embed(ctx, []string{q})
	if err != nil || len(vecs) == 0 {
		return nil
	}
	hitVecs := make([][]float32, len(urls))
	for i, u := range urls {
		if v, ok := s.hnsw.LookupVectorByURL(u); ok {
			hitVecs[i] = v
		}
	}
	return mmrOrder(vecs[0], hitVecs, lambda)
}

// Iter 366: buildRetrieverLabel produces the human-readable retriever string
// surfaced on /search, /answer, /research responses. Mirrors the iter 363/364
// /search inline switch so all three endpoints report the same vocabulary.
// expansionFired is the post-hoc "did expand=hyde/paraphrase actually run"
// signal (effectiveQuery != q for /search & /answer; chat-present for
// /research where sub-queries are individual). wantRerank appends the
// "+rerank:<name>" suffix when rerank fired.
func (s *pebbleHTTP) buildRetrieverLabel(retrieverParam, expandMode string, denseReady, expansionFired, wantRerank bool) string {
	label := "bm25"
	switch {
	case retrieverParam == "dense" && denseReady:
		label = "dense"
	case retrieverParam == "hybrid" && denseReady:
		label = "bm25+dense:rrf"
	default:
		switch expandMode {
		case "paraphrase":
			if expansionFired {
				label = "bm25+paraphrase"
			}
		case "true", "hyde":
			if expansionFired {
				label = "bm25+hyde"
			}
		}
	}
	if wantRerank && s.reranker != nil {
		label += "+rerank:" + s.reranker.Name()
	}
	return label
}

// Iter 365: retrieve dispatches retriever choice (bm25 / dense / hybrid) and
// then expansion (bare / HyDE / paraphrase+RRF) for BM25 paths. Shared by
// /search, /answer, /research so all three endpoints get the same retriever
// matrix. Dense / hybrid require both the loaded HNSW graph (iter 362) and an
// embedder (iter 363); missing either falls through to BM25 — warningsFor()
// surfaces that to the client.
func (s *pebbleHTTP) retrieve(ctx context.Context, q string, fetchK int, retrieverParam, expandMode string) ([]index.Hit, string, error) {
	denseReady := s.hnsw != nil && s.embedder != nil
	switch {
	case retrieverParam == "dense" && denseReady:
		vecs, err := s.embedder.Embed(ctx, []string{q})
		if err != nil {
			return nil, "", fmt.Errorf("embedder: %w", err)
		}
		vhits := s.hnsw.Search(ctx, vecs[0], fetchK)
		hits := make([]index.Hit, len(vhits))
		for i, vh := range vhits {
			hits[i] = index.Hit{URL: vh.URL, Title: vh.Title, Score: vh.Score}
		}
		return hits, q, nil
	case retrieverParam == "hybrid" && denseReady:
		bm25Hits, bm25Eff, bm25Err := s.retrieveWithExpansion(ctx, q, fetchK, expandMode)
		if bm25Err != nil {
			return nil, "", bm25Err
		}
		vecs, embErr := s.embedder.Embed(ctx, []string{q})
		if embErr != nil {
			return nil, "", fmt.Errorf("embedder: %w", embErr)
		}
		denseV := s.hnsw.Search(ctx, vecs[0], fetchK)
		denseHits := make([]index.Hit, len(denseV))
		for i, vh := range denseV {
			denseHits[i] = index.Hit{URL: vh.URL, Title: vh.Title, Score: vh.Score}
		}
		hits := rrfFuse([][]index.Hit{bm25Hits, denseHits}, 60)
		if len(hits) > fetchK {
			hits = hits[:fetchK]
		}
		return hits, bm25Eff, nil
	default:
		return s.retrieveWithExpansion(ctx, q, fetchK, expandMode)
	}
}

// Iter 274: retrieveWithExpansion dispatches the BM25 call across the three
// expansion strategies /search and /answer share — bare, HyDE, paraphrase+RRF.
// Returns (hits, effectiveQuery, err). effectiveQuery == q when no expansion
// fired (bare path, or expansion no-op'd because chat is down).
func (s *pebbleHTTP) retrieveWithExpansion(ctx context.Context, q string, fetchK int, expandMode string) ([]index.Hit, string, error) {
	switch expandMode {
	case "paraphrase":
		paras := s.paraphraseQuery(ctx, q, 3)
		if len(paras) == 0 {
			hits, err := s.idx.Search(ctx, q, fetchK)
			return hits, q, err
		}
		queries := append([]string{q}, paras...)
		lists := make([][]index.Hit, 0, len(queries))
		for _, qq := range queries {
			h, lerr := s.idx.Search(ctx, qq, fetchK)
			if lerr != nil {
				continue
			}
			lists = append(lists, h)
		}
		hits := rrfFuse(lists, 60)
		if len(hits) > fetchK {
			hits = hits[:fetchK]
		}
		return hits, q + " | " + strings.Join(paras, " | "), nil
	case "true", "hyde":
		eq := s.expandQuery(ctx, q)
		hits, err := s.idx.Search(ctx, eq, fetchK)
		return hits, eq, err
	default:
		hits, err := s.idx.Search(ctx, q, fetchK)
		return hits, q, err
	}
}

// expandQuery returns q + " " + a HyDE-generated passage when a chat client is
// configured. On any error (no chat client, chat call fails, empty passage),
// returns q unchanged so callers can compose safely without explicit guards.
// Iter 257; iter 259 added a bounded in-memory cache to skip the chat call on
// repeat queries — important for /research?expand=true where the same
// sub-query rephrasing can fire across many similar research requests.
func (s *pebbleHTTP) expandQuery(ctx context.Context, q string) string {
	if s.chat == nil {
		return q
	}
	s.hydeMu.RLock()
	cached, hit := s.hydeCache[q]
	s.hydeMu.RUnlock()
	if hit {
		s.hydeHits.Add(1)
		return q + " " + cached
	}
	s.hydeMisses.Add(1)
	passage, err := s.doChat(ctx, s.chat, []embed.ChatMsg{
		{Role: "system", Content: hydeSystemPrompt},
		{Role: "user", Content: q},
	})
	if err != nil {
		return q
	}
	passage = strings.TrimSpace(passage)
	if passage == "" {
		return q
	}
	s.hydeMu.Lock()
	if len(s.hydeCache) >= s.hydeCacheCap {
		// Bounded: drop one arbitrary entry (Go map range is randomized).
		for k := range s.hydeCache {
			delete(s.hydeCache, k)
			break
		}
	}
	s.hydeCache[q] = passage
	s.hydeMu.Unlock()
	return q + " " + passage
}

type answerSource struct {
	// Iter 341: citation ID (1-based). Matches the [N] tokens the synth
	// prompt produces in the answer text. SQLite-side AnswerSource has had
	// this since iter 84; pebble's was missing, which caused the CLI to
	// render every source as '[0]' before iter 339/340's i+1 fallback.
	ID          int        `json:"id"`
	URL         string     `json:"url"`
	Title       string     `json:"title"`
	Excerpt     string     `json:"excerpt,omitempty"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	Author      string     `json:"author,omitempty"`
	Text        string     `json:"text,omitempty"`
}

type answerResponse struct {
	Query           string         `json:"query"`
	EffectiveQuery  string         `json:"effective_query,omitempty"`
	Expand          string         `json:"expand,omitempty"`
	// Iter 366: retriever label — same vocabulary as /search ("bm25",
	// "dense", "bm25+dense:rrf", "+hyde"/"+paraphrase", "+rerank:<name>").
	Retriever       string         `json:"retriever,omitempty"`
	Answer          string         `json:"answer"`
	Sources         []answerSource `json:"sources"`
	Model           string         `json:"model"`
	Warnings        []string       `json:"warnings,omitempty"`
	TotalCandidates int            `json:"total_candidates,omitempty"`
	Took            string         `json:"took"`
}

func (s *pebbleHTTP) handleAnswer(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented,
			"/answer requires cfg.Chat.Model to be set (point cosift.json's chat.url at any OpenAI-compatible endpoint)")
		return
	}
	start := time.Now()
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing q parameter")
		return
	}
	k := 5
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 20 {
			k = n
		}
	}
	// Iter 241: /answer respects the same retrieval filters /search does —
	// scoping research to a domain or date window is the common EXA shape.
	include := splitDomainsCSV(r.URL.Query().Get("include_domains"))
	exclude := splitDomainsCSV(r.URL.Query().Get("exclude_domains"))
	since, sinceErr := parseDateBound(r.URL.Query().Get("since"))
	if sinceErr != nil {
		writeProblem(w, http.StatusBadRequest, "since: "+sinceErr.Error())
		return
	}
	until, untilErr := parseDateBound(r.URL.Query().Get("until"))
	if untilErr != nil {
		writeProblem(w, http.StatusBadRequest, "until: "+untilErr.Error())
		return
	}
	dateFilter := !since.IsZero() || !until.IsZero()
	includeText := r.URL.Query().Get("include_text") == "true"
	// Iter 249: ?rerank=true reorders the BM25 top-pool before synth.
	// Rerank quality > BM25 quality for "which 5 sources answer this question",
	// so this is the highest-impact iter for /answer beyond getting LLMs hooked
	// up. Widens the candidate pool to rerankCandK before truncation.
	wantRerank := r.URL.Query().Get("rerank") == "true" && s.reranker != nil
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	fetchK := keepCap
	if len(include) > 0 || len(exclude) > 0 || dateFilter {
		mult := 5
		if dateFilter {
			mult = 10
		}
		fetchK = keepCap * mult
		if fetchK > 200 {
			fetchK = 200 // /answer caps tighter than /search — full doc text per source is expensive
		}
	} else if wantRerank {
		fetchK = keepCap * 2
	}
		// Iter 256/273/274: expansion dispatch (bare / HyDE / paraphrase+RRF).
		// Iter 365: retriever dispatch (bm25 / dense / hybrid) via shared helper.
	expandMode := r.URL.Query().Get("expand")
	retrieverParam := r.URL.Query().Get("retriever")
	hits, effectiveQuery, err := s.retrieve(r.Context(), q, fetchK, retrieverParam, expandMode)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
	}
	cands := make([]cand, 0, keepCap)
	for _, h := range hits {
		if len(include) > 0 || len(exclude) > 0 {
			host := hostOf(h.URL)
			if len(include) > 0 && !matchesAnyDomain(host, include) {
				continue
			}
			if len(exclude) > 0 && matchesAnyDomain(host, exclude) {
				continue
			}
		}
		doc, derr := s.store.GetDocByURL(r.Context(), h.URL)
		if derr != nil || doc == nil {
			continue
		}
		if dateFilter {
			if doc.PublishedAt.IsZero() {
				continue
			}
			if !since.IsZero() && doc.PublishedAt.Before(since) {
				continue
			}
			if !until.IsZero() && doc.PublishedAt.After(until) {
				continue
			}
		}
		excerpt := textExcerpt(doc.Text, 1200)
		src := answerSource{URL: doc.URL, Title: doc.Title, Excerpt: excerpt, Author: doc.Author}
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt
			src.PublishedAt = &t
		}
		if includeText {
			src.Text = doc.Text
		}
		c := cand{src: src, excerpt: excerpt}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
		if len(cands) >= keepCap {
			break
		}
	}
	// Optional rerank, then truncate to k. Done in this order so citation
	// numbers in the prompt match the final rank-ordered sources list.
	if wantRerank && len(cands) > 1 {
		rc := make([]rerank.Candidate, len(cands))
		for i := range cands {
			rc[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: cands[i].rerankText}
		}
		if order, rerr := s.doRerank(r.Context(), q, rc); rerr == nil && len(order) > 0 {
			reordered := make([]cand, 0, len(cands))
			seen := make(map[int]bool, len(cands))
			for _, id := range order {
				if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(cands) && !seen[n] {
					reordered = append(reordered, cands[n])
					seen[n] = true
				}
			}
			for i, c := range cands {
				if !seen[i] {
					reordered = append(reordered, c)
				}
			}
			cands = reordered
		}
	}
	// Iter 385: MMR diversification on /answer — same pattern as /search.
	// Synth quality benefits when the top-k sources cover different
	// angles instead of being 5 paraphrases of the same paper.
	mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr"))
	mmrFired := false
	if mmrSet && len(cands) > 1 {
		urls := make([]string, len(cands))
		for i := range cands {
			urls[i] = cands[i].src.URL
		}
		if order := s.applyMMRPermutation(r.Context(), urls, q, mmrLambda); order != nil {
			reordered := make([]cand, len(order))
			for i, idx := range order {
				reordered[i] = cands[idx]
			}
			cands = reordered
			mmrFired = true
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	sources := make([]answerSource, 0, len(cands))
	var promptSources strings.Builder
	for i, c := range cands {
		// Iter 341: stamp ID = i+1 so the JSON response matches the [N]
		// citation tokens we emit in the synth prompt below.
		c.src.ID = i + 1
		sources = append(sources, c.src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
	}
	// Iter 366: same retriever label vocabulary as /search.
	denseReady := s.hnsw != nil && s.embedder != nil
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, effectiveQuery != q, wantRerank)
	if mmrFired {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	if len(sources) == 0 {
		empty := answerResponse{
			Query: q, Expand: normalizeExpandMode(expandMode), Retriever: retrieverLabel,
			Answer:  "No matching sources in the index.",
			Sources: sources, Model: s.chat.Model(), TotalCandidates: len(hits), Took: time.Since(start).String(),
		}
		if effectiveQuery != q {
			empty.EffectiveQuery = effectiveQuery
		}
		empty.Warnings = s.warningsFor(r)
		writeJSON(w, http.StatusOK, empty)
		return
	}

	msgs := []embed.ChatMsg{
		{Role: "system", Content: answerSystemPrompt},
		{Role: "user", Content: "Sources:\n\n" + promptSources.String() + "Question: " + q},
	}

	// Iter 242: SSE streaming. Opt in via ?stream=true OR
	// Accept: text/event-stream. Emits three event types:
	//   sources — once, immediately after retrieval (so the client can render
	//             links / citations while the LLM is still thinking)
	//   chunk   — per chat delta; payload = {"delta": "..."}
	//   done    — once, with the final {"took": "..."}
	// Falls back to sync if the chat client doesn't implement StreamingChatClient.
	wantStream := r.URL.Query().Get("stream") == "true" ||
		strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	if wantStream {
		if sc, ok := s.chat.(embed.StreamingChatClient); ok {
			s.streamAnswer(w, r, sc, msgs, sources, q, len(hits), retrieverLabel, start)
			return
		}
		// Not a streaming client — degrade silently to sync rather than 501.
	}

	answer, err := s.doChat(r.Context(), s.chat, msgs)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "chat: "+err.Error())
		return
	}
	resp := answerResponse{
		Query: q, Expand: normalizeExpandMode(expandMode), Retriever: retrieverLabel,
		Answer: answer, Sources: sources,
		Model:  s.chat.Model(), TotalCandidates: len(hits), Took: time.Since(start).String(),
	}
	if effectiveQuery != q {
		resp.EffectiveQuery = effectiveQuery
	}
	resp.Warnings = s.warningsFor(r)
	writeJSON(w, http.StatusOK, resp)
}

func (s *pebbleHTTP) streamAnswer(w http.ResponseWriter, r *http.Request, sc embed.StreamingChatClient, msgs []embed.ChatMsg, sources []answerSource, q string, totalCandidates int, retrieverLabel string, start time.Time) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "streaming requires http.Flusher")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	sse := func(payload any) {
		buf, _ := json.Marshal(payload)
		fmt.Fprintf(w, "data: %s\n\n", buf)
		flusher.Flush()
	}
	if warns := s.warningsFor(r); len(warns) > 0 {
		sse(map[string]any{"type": "warnings", "warnings": warns})
	}
	srcEvt := map[string]any{"type": "sources", "query": q, "sources": sources, "model": sc.Model(), "total_candidates": totalCandidates}
	if retrieverLabel != "" {
		srcEvt["retriever"] = retrieverLabel
	}
	sse(srcEvt)

	_, err := s.doChatStream(r.Context(), sc, msgs, func(delta string) {
		sse(map[string]any{"type": "answer_chunk", "text": delta})
	})
	if err != nil {
		sse(map[string]any{"type": "error", "error": err.Error()})
		return
	}
	sse(map[string]any{"type": "done", "took": time.Since(start).String()})
}

// Iter 243: /research — multi-step retrieval + synthesis. LLM decomposes the
// question into 2-3 sub-queries, each sub-query runs BM25, results are deduped
// by URL keeping the best score, top-k feed a cited synthesis. Mirrors the
// SQLite-side /research planner strategy. No streaming, no rerank, no
// paraphrase strategy yet — those follow once this surface is exercised.
const researchPlanPrompt = `Decompose the user's research question into 2-3 focused sub-queries that, taken together, would cover the answer. Output ONLY a JSON array of strings — no prose, no markdown. Example: ["sub-query 1", "sub-query 2"]`

const researchSynthPrompt = `You are a research assistant. Synthesize an answer to the original question using ONLY the provided sources.
- Cite sources by their numeric id, e.g. [1] or [2,3]. Every factual claim needs a citation.
- If the sources don't cover something, say so plainly — do not invent.
- Keep the answer focused on what the sources actually say.`

type researchResponse struct {
	Query           string         `json:"query"`
	Plan            []string       `json:"plan"`
	Expand          string         `json:"expand,omitempty"`
	// Iter 366: retriever label, mirroring /search/answer vocabulary.
	Retriever       string         `json:"retriever,omitempty"`
	Answer          string         `json:"answer"`
	Sources         []answerSource `json:"sources"`
	Model           string         `json:"model"`
	Warnings        []string       `json:"warnings,omitempty"`
	TotalCandidates int            `json:"total_candidates,omitempty"`
	Took            string         `json:"took"`
}

func (s *pebbleHTTP) handleResearch(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented,
			"/research requires cfg.Chat.Model to be set")
		return
	}
	start := time.Now()
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing q parameter")
		return
	}
	k := 8
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 20 {
			k = n
		}
	}
	// Iter 246: same retrieval filters /search/answer/find_similar accept.
	// Parsed once here so the streaming and sync paths share semantics.
	filt, err := parseRetrievalFilters(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	includeText := r.URL.Query().Get("include_text") == "true"
	// Iter 244: SSE streaming for /research. Same trigger as /answer.
	// Emits phase-aware events so the UI can render the plan and source list
	// before the synth call completes — /research often runs 10–30s
	// (2-3 plan→retrieve→synth chat rounds), so phase visibility matters.
	wantStream := r.URL.Query().Get("stream") == "true" ||
		strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	if wantStream {
		if sc, ok := s.chat.(embed.StreamingChatClient); ok {
			s.streamResearch(w, r, sc, q, k, filt, start)
			return
		}
	}

	// Plan
	planRaw, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
		{Role: "system", Content: researchPlanPrompt},
		{Role: "user", Content: q},
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "plan: "+err.Error())
		return
	}
	subs := parseSubQueries(planRaw, q)
	if len(subs) > 5 {
		subs = subs[:5]
	}

	// Retrieve per sub-query, dedupe by URL keeping best score.
	type ranked struct {
		score float64
		hit   index.Hit
	}
	best := make(map[string]ranked, k*len(subs))
	perSub := k * 2
	if perSub > 40 {
		perSub = 40
	}
	// Iter 257/275: each sub-query goes through retrieveWithExpansion, so
	// ?expand=hyde gives per-sub-query HyDE (was iter 257's behavior) and
	// ?expand=paraphrase fans out 3 paraphrases × N sub-queries → RRF per
	// sub-query → merge into best{}. The cross-sub-query merge stays
	// score-keep-best (not a second RRF) — that's what iter-243 specified.
	// Iter 365: retriever dispatch (bm25 / dense / hybrid) applies per
	// sub-query — every sub-query runs through the same retriever.
	expandMode := r.URL.Query().Get("expand")
	retrieverParam := r.URL.Query().Get("retriever")
	for _, sq := range subs {
		hits, _, err := s.retrieve(r.Context(), sq, perSub, retrieverParam, expandMode)
		if err != nil {
			// Iter 302: log the specific sub-query so operators can diagnose
			// 'why was this research thin on sources' — previously silent.
			log.Printf("pebble-serve: /research sub-query %q failed: %v", sq, err)
			continue
		}
		for _, h := range hits {
			if prev, ok := best[h.URL]; !ok || h.Score > prev.score {
				best[h.URL] = ranked{score: h.Score, hit: h}
			}
		}
	}
	pooled := make([]ranked, 0, len(best))
	for _, v := range best {
		pooled = append(pooled, v)
	}
	sort.Slice(pooled, func(i, j int) bool { return pooled[i].score > pooled[j].score })
	// Iter 250: same rerank wiring as /search and /answer — pool widens to
	// rerankCandK before materialization, rerank reorders the pool, truncate
	// to k after rerank so citation numbers track the final order.
	wantRerank := r.URL.Query().Get("rerank") == "true" && s.reranker != nil
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	if len(pooled) > keepCap {
		pooled = pooled[:keepCap]
	}

	// Materialize candidates; defer sources + prompt build until after rerank.
	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
	}
	cands := make([]cand, 0, len(pooled))
	for _, p := range pooled {
		doc, derr := s.store.GetDocByURL(r.Context(), p.hit.URL)
		if derr != nil || doc == nil {
			continue
		}
		if !filt.allow(doc.URL, doc.PublishedAt) {
			continue
		}
		excerpt := textExcerpt(doc.Text, 1200)
		src := answerSource{URL: doc.URL, Title: doc.Title, Excerpt: excerpt, Author: doc.Author}
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt
			src.PublishedAt = &t
		}
		if includeText {
			src.Text = doc.Text
		}
		c := cand{src: src, excerpt: excerpt}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
	}
	if wantRerank && len(cands) > 1 {
		rc := make([]rerank.Candidate, len(cands))
		for i := range cands {
			rc[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: cands[i].rerankText}
		}
		if order, rerr := s.doRerank(r.Context(), q, rc); rerr == nil && len(order) > 0 {
			reordered := make([]cand, 0, len(cands))
			seen := make(map[int]bool, len(cands))
			for _, id := range order {
				if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(cands) && !seen[n] {
					reordered = append(reordered, cands[n])
					seen[n] = true
				}
			}
			for i, c := range cands {
				if !seen[i] {
					reordered = append(reordered, c)
				}
			}
			cands = reordered
		}
	}
	// Iter 385: MMR diversification on /research sync. /research aggregates
	// across sub-queries — duplicate-ish docs accumulate fast. MMR after
	// rerank, before truncation.
	mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr"))
	mmrFired := false
	if mmrSet && len(cands) > 1 {
		urls := make([]string, len(cands))
		for i := range cands {
			urls[i] = cands[i].src.URL
		}
		if order := s.applyMMRPermutation(r.Context(), urls, q, mmrLambda); order != nil {
			reordered := make([]cand, len(order))
			for i, idx := range order {
				reordered[i] = cands[idx]
			}
			cands = reordered
			mmrFired = true
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	sources := make([]answerSource, 0, len(cands))
	var promptSources strings.Builder
	for i, c := range cands {
		// Iter 341: stamp ID = i+1 so the JSON response matches the [N]
		// citation tokens we emit in the synth prompt below.
		c.src.ID = i + 1
		sources = append(sources, c.src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
	}
	// Iter 366: retriever label for /research. Sub-queries don't yield a
	// single effectiveQuery, so "expansion fired" is approximated by intent:
	// chat is up and expandMode requested it. Matches what warningsFor() uses
	// to decide whether to flag a silent no-op.
	denseReady := s.hnsw != nil && s.embedder != nil
	expandFired := expandMode != "" && s.chat != nil
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, expandFired, wantRerank)
	if mmrFired {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	if len(sources) == 0 {
		writeJSON(w, http.StatusOK, researchResponse{
			Query: q, Plan: subs, Expand: normalizeExpandMode(expandMode), Retriever: retrieverLabel,
			Answer:  "No matching sources for any sub-query.",
			Sources: sources, Model: s.chat.Model(), Warnings: s.warningsFor(r),
			TotalCandidates: len(best), Took: time.Since(start).String(),
		})
		return
	}

	answer, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
		{Role: "system", Content: researchSynthPrompt},
		{Role: "user", Content: "Sources:\n\n" + promptSources.String() + "Original question: " + q},
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "synth: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, researchResponse{
		Query: q, Plan: subs, Expand: normalizeExpandMode(expandMode), Retriever: retrieverLabel,
		Answer: answer, Sources: sources,
		Model:  s.chat.Model(), Warnings: s.warningsFor(r),
		TotalCandidates: len(best), Took: time.Since(start).String(),
	})
}

// streamResearch is the SSE form of handleResearch. Event sequence:
//   plan    — sub-queries returned by the planner
//   sources — deduped + ranked pool fed to the synthesizer
//   chunk   — per chat delta during synth
//   done    — final {"took": "..."}
//   error   — terminal; either plan, retrieval, or synth failed
func (s *pebbleHTTP) streamResearch(w http.ResponseWriter, r *http.Request, sc embed.StreamingChatClient, q string, k int, filt retrievalFilters, start time.Time) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "streaming requires http.Flusher")
		return
	}
	includeText := r.URL.Query().Get("include_text") == "true"
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	sse := func(payload any) {
		buf, _ := json.Marshal(payload)
		fmt.Fprintf(w, "data: %s\n\n", buf)
		flusher.Flush()
	}

	// Iter 293: surface silent no-ops upfront so SSE clients can render them
	// while the plan call is still in flight. Fires before plan to give the
	// fastest visual feedback when a misconfigured request arrives.
	if warns := s.warningsFor(r); len(warns) > 0 {
		sse(map[string]any{"type": "warnings", "warnings": warns})
	}

	planRaw, err := s.doChat(r.Context(), sc, []embed.ChatMsg{
		{Role: "system", Content: researchPlanPrompt},
		{Role: "user", Content: q},
	})
	if err != nil {
		sse(map[string]any{"type": "error", "phase": "plan", "error": err.Error()})
		return
	}
	subs := parseSubQueries(planRaw, q)
	if len(subs) > 5 {
		subs = subs[:5]
	}
	// Iter 257/275: per-sub-query expansion via retrieveWithExpansion.
	// Iter 365: per-sub-query retriever dispatch (bm25 / dense / hybrid).
	// Iter 366: read retriever + wantRerank early so the plan event can
	// surface the full retriever label, not just expand.
	expandMode := r.URL.Query().Get("expand")
	retrieverParam := r.URL.Query().Get("retriever")
	wantRerank := r.URL.Query().Get("rerank") == "true" && s.reranker != nil
	denseReady := s.hnsw != nil && s.embedder != nil
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, expandMode != "", wantRerank)

	// Iter 284: surface the active expansion strategy on the plan event so
	// UIs can render 'using paraphrase' or 'using HyDE' as soon as the plan
	// arrives, before per-sub-query expansion fires.
	planEvent := map[string]any{"type": "plan", "query": q, "plan": subs, "model": sc.Model()}
	if mode := normalizeExpandMode(expandMode); mode != "" {
		planEvent["expand"] = mode
	}
	if retrieverLabel != "" {
		planEvent["retriever"] = retrieverLabel
	}
	sse(planEvent)

	type ranked struct {
		score float64
		hit   index.Hit
	}
	best := make(map[string]ranked, k*len(subs))
	perSub := k * 2
	if perSub > 40 {
		perSub = 40
	}
	for _, sq := range subs {
		hits, _, err := s.retrieve(r.Context(), sq, perSub, retrieverParam, expandMode)
		if err != nil {
			log.Printf("pebble-serve: /research sub-query %q failed: %v", sq, err) // iter 302
			continue
		}
		for _, h := range hits {
			if prev, ok := best[h.URL]; !ok || h.Score > prev.score {
				best[h.URL] = ranked{score: h.Score, hit: h}
			}
		}
	}
	pooled := make([]ranked, 0, len(best))
	for _, v := range best {
		pooled = append(pooled, v)
	}
	sort.Slice(pooled, func(i, j int) bool { return pooled[i].score > pooled[j].score })
	// Iter 250: same rerank wiring as /research sync. Pool widens to
	// rerankCandK, rerank reorders, then truncate to k before SSE 'sources'
	// fires so the client sees the final rank-ordered list. wantRerank was
	// hoisted to the top of streamResearch in iter 366 for the plan label.
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}
	if len(pooled) > keepCap {
		pooled = pooled[:keepCap]
	}

	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
	}
	cands := make([]cand, 0, len(pooled))
	for _, p := range pooled {
		doc, derr := s.store.GetDocByURL(r.Context(), p.hit.URL)
		if derr != nil || doc == nil {
			continue
		}
		if !filt.allow(doc.URL, doc.PublishedAt) {
			continue
		}
		excerpt := textExcerpt(doc.Text, 1200)
		src := answerSource{URL: doc.URL, Title: doc.Title, Excerpt: excerpt, Author: doc.Author}
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt
			src.PublishedAt = &t
		}
		if includeText {
			src.Text = doc.Text
		}
		c := cand{src: src, excerpt: excerpt}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
	}
	if wantRerank && len(cands) > 1 {
		rc := make([]rerank.Candidate, len(cands))
		for i := range cands {
			rc[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: cands[i].rerankText}
		}
		if order, rerr := s.doRerank(r.Context(), q, rc); rerr == nil && len(order) > 0 {
			reordered := make([]cand, 0, len(cands))
			seen := make(map[int]bool, len(cands))
			for _, id := range order {
				if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(cands) && !seen[n] {
					reordered = append(reordered, cands[n])
					seen[n] = true
				}
			}
			for i, c := range cands {
				if !seen[i] {
					reordered = append(reordered, c)
				}
			}
			cands = reordered
		}
	}
	// Iter 385: MMR diversification on /research stream — same pattern as sync.
	// Updates retrieverLabel so the iter-366 retriever field on the sources
	// SSE event reflects what actually fired (planEvent's label, emitted
	// earlier, says what was requested — usually identical when MMR succeeds).
	mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr"))
	if mmrSet && len(cands) > 1 {
		urls := make([]string, len(cands))
		for i := range cands {
			urls[i] = cands[i].src.URL
		}
		if order := s.applyMMRPermutation(r.Context(), urls, q, mmrLambda); order != nil {
			reordered := make([]cand, len(order))
			for i, idx := range order {
				reordered[i] = cands[idx]
			}
			cands = reordered
			retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	sources := make([]answerSource, 0, len(cands))
	var promptSources strings.Builder
	for i, c := range cands {
		// Iter 341: stamp ID = i+1 so the JSON response matches the [N]
		// citation tokens we emit in the synth prompt below.
		c.src.ID = i + 1
		sources = append(sources, c.src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
	}
	srcEvt := map[string]any{"type": "sources", "sources": sources, "total_candidates": len(best)} // iter 317
	if retrieverLabel != "" {
		srcEvt["retriever"] = retrieverLabel // iter 366/385
	}
	sse(srcEvt)
	if len(sources) == 0 {
		sse(map[string]any{"type": "done", "took": time.Since(start).String(), "empty": true})
		return
	}

	_, err = s.doChatStream(r.Context(), sc, []embed.ChatMsg{
		{Role: "system", Content: researchSynthPrompt},
		{Role: "user", Content: "Sources:\n\n" + promptSources.String() + "Original question: " + q},
	}, func(delta string) {
		sse(map[string]any{"type": "answer_chunk", "text": delta})
	})
	if err != nil {
		sse(map[string]any{"type": "error", "phase": "synth", "error": err.Error()})
		return
	}
	sse(map[string]any{"type": "done", "took": time.Since(start).String()})
}

// retrievalFilters bundles the four post-retrieval predicates /search,
// /answer, /find_similar, and /research all share. Centralized in iter 246
// so /research's sync + stream paths stay in lockstep.
type retrievalFilters struct {
	include    []string
	exclude    []string
	since      time.Time
	until      time.Time
	dateActive bool
}

func parseRetrievalFilters(r *http.Request) (retrievalFilters, error) {
	f := retrievalFilters{
		include: splitDomainsCSV(r.URL.Query().Get("include_domains")),
		exclude: splitDomainsCSV(r.URL.Query().Get("exclude_domains")),
	}
	since, err := parseDateBound(r.URL.Query().Get("since"))
	if err != nil {
		return f, fmt.Errorf("since: %w", err)
	}
	until, err := parseDateBound(r.URL.Query().Get("until"))
	if err != nil {
		return f, fmt.Errorf("until: %w", err)
	}
	f.since = since
	f.until = until
	f.dateActive = !since.IsZero() || !until.IsZero()
	return f, nil
}

// allow reports whether (url, publishedAt) clears the filter. publishedAt
// can be the zero Time when the caller doesn't have it loaded; zero is
// treated as "unknown" and dropped under any date filter.
func (f retrievalFilters) allow(rawURL string, publishedAt time.Time) bool {
	if len(f.include) > 0 || len(f.exclude) > 0 {
		host := hostOf(rawURL)
		if len(f.include) > 0 && !matchesAnyDomain(host, f.include) {
			return false
		}
		if len(f.exclude) > 0 && matchesAnyDomain(host, f.exclude) {
			return false
		}
	}
	if f.dateActive {
		if publishedAt.IsZero() {
			return false
		}
		if !f.since.IsZero() && publishedAt.Before(f.since) {
			return false
		}
		if !f.until.IsZero() && publishedAt.After(f.until) {
			return false
		}
	}
	return true
}

func parseSubQueries(raw, fallback string) []string {
	raw = strings.TrimSpace(raw)
	for _, fence := range []string{"```json", "```"} {
		raw = strings.TrimPrefix(raw, fence)
		raw = strings.TrimSuffix(raw, "```")
	}
	raw = strings.TrimSpace(raw)
	startIdx := strings.Index(raw, "[")
	endIdx := strings.LastIndex(raw, "]")
	if startIdx >= 0 && endIdx > startIdx {
		var arr []string
		if err := json.Unmarshal([]byte(raw[startIdx:endIdx+1]), &arr); err == nil && len(arr) > 0 {
			return arr
		}
	}
	return []string{fallback}
}

func splitDomainsCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.ToLower(strings.TrimSpace(p))
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func matchesAnyDomain(host string, patterns []string) bool {
	host = strings.ToLower(host)
	for _, p := range patterns {
		if host == p || strings.HasSuffix(host, "."+p) {
			return true
		}
	}
	return false
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// parseDateBound accepts the same forms as the SQLite-side server: empty
// (zero time), RFC3339 ("2026-01-15T00:00:00Z"), or a bare date
// ("2026-01-15", treated as UTC midnight). Iter 234.
func parseDateBound(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("expected YYYY-MM-DD or RFC3339, got %q", s)
}

// textExcerpt returns a body-prefix excerpt no longer than maxBytes, cut at a
// word boundary if one is available within the trailing 16 bytes. Iter 233.
// Static prefix (not query-aware) — matches the SQLite-side DocMeta.Excerpt
// shape; a query-aware passage extractor is the iter-199 path on the index
// hot path and lives there, not in the HTTP handler.
func textExcerpt(text string, maxBytes int) string {
	if len(text) <= maxBytes {
		return text
	}
	cut := text[:maxBytes]
	for i := len(cut) - 1; i >= len(cut)-16 && i > 0; i-- {
		if cut[i] == ' ' || cut[i] == '\n' || cut[i] == '\t' {
			cut = cut[:i]
			break
		}
	}
	return strings.TrimSpace(cut) + "…"
}

func (s *pebbleHTTP) handleContents(w http.ResponseWriter, r *http.Request) {
	rawURL := r.URL.Query().Get("url")
	if rawURL == "" {
		writeProblem(w, http.StatusBadRequest, "missing url parameter")
		return
	}
	decoded, err := url.QueryUnescape(rawURL)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "url parameter is not URL-encoded")
		return
	}
	doc, err := s.store.GetDocByURL(r.Context(), decoded)
	if errors.Is(err, store.ErrNotFound) {
		writeProblem(w, http.StatusNotFound, "url not in index")
		return
	}
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"url":          doc.URL,
		"title":        doc.Title,
		"text":         doc.Text,
		"author":       doc.Author,
		"published_at": doc.PublishedAt,
		"fetched_at":   doc.FetchedAt,
	})
}

// Iter 254/255: POST /contents — batch URL → document. Up to 100 URLs in
// one round-trip. URLs not in the index emit a {url, found:false, error}
// stub in the same slot so the caller can correlate positionally. Wire
// shape (results+took, per-item found+cached+lang) mirrors the SQLite-side
// /contents POST so `cosift contents <url> <url>` works against either
// backend without code changes.
type contentsBatchReq struct {
	URLs []string `json:"urls"`
}

type contentsBatchItem struct {
	URL         string    `json:"url"`
	Found       bool      `json:"found"`
	Title       string    `json:"title,omitempty"`
	Text        string    `json:"text,omitempty"`
	Lang        string    `json:"lang,omitempty"`
	Cached      bool      `json:"cached"`
	Author      string    `json:"author,omitempty"`
	PublishedAt time.Time `json:"published_at,omitempty"`
	FetchedAt   time.Time `json:"fetched_at,omitempty"`
	Error       string    `json:"error,omitempty"`
}

func (s *pebbleHTTP) handleContentsBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req contentsBatchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if len(req.URLs) == 0 {
		writeProblem(w, http.StatusBadRequest, "urls: must be a non-empty array")
		return
	}
	if len(req.URLs) > 100 {
		writeProblem(w, http.StatusBadRequest, "urls: at most 100 per request")
		return
	}
	results := make([]contentsBatchItem, 0, len(req.URLs))
	for _, u := range req.URLs {
		item := contentsBatchItem{URL: u}
		doc, err := s.store.GetDocByURL(r.Context(), u)
		switch {
		case errors.Is(err, store.ErrNotFound):
			item.Error = "not in index"
		case err != nil:
			item.Error = err.Error()
		case doc == nil:
			item.Error = "not in index"
		default:
			item.Found = true
			item.Cached = true
			item.Title = doc.Title
			item.Text = doc.Text
			item.Lang = doc.Lang
			item.Author = doc.Author
			item.PublishedAt = doc.PublishedAt
			item.FetchedAt = doc.FetchedAt
		}
		results = append(results, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"results": results,
		"took":    time.Since(start).String(),
	})
}

// runPebbleInfo prints Pebble's built-in metrics for the store at -dir.
// Iter 325: openPebbleOrFriendlyErr wraps store.OpenPebble with a more
// helpful error message when the underlying failure is lock contention (a
// pebble-serve / live crawl is holding the single-writer lock). Callers
// using offline CLI subcommands (pebble-info, verify) hit this when run
// during an active deployment.
func openPebbleOrFriendlyErr(d string) (*store.PebbleStore, error) {
	ps, err := store.OpenPebble(d)
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "lock") || strings.Contains(msg, "resource temporarily unavailable") {
			return nil, fmt.Errorf("open pebble at %s: writer lock is held by another process (pebble-serve / crawl in flight); stop the running service first", d)
		}
		return nil, fmt.Errorf("open pebble at %s: %w", d, err)
	}
	return ps, nil
}

// Iter 331: runVerifyViaServer GETs pebble-serve's /verify endpoint and
// renders the JSON body in the same shape runVerifyPebble's local path emits.
// Lets `cosift verify -server URL` work against a running pebble-serve while
// the writer lock is held by the crawl / serve process.
func runVerifyViaServer(ctx context.Context, serverURL string, asJSON bool) error {
	endpoint := strings.TrimRight(serverURL, "/") + "/verify"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if asJSON {
		fmt.Println(string(body))
	} else {
		var d struct {
			OK                 bool  `json:"ok"`
			IndexedDocsCounter int64 `json:"indexed_docs_counter"`
			IndexedDocsScan    int64 `json:"indexed_docs_scan"`
			IndexedDocsDrift   int64 `json:"indexed_docs_drift"`
			SumDocLenCounter   int64 `json:"sum_doc_len_counter"`
			SumDocLenScan      int64 `json:"sum_doc_len_scan"`
			SumDocLenDrift     int64 `json:"sum_doc_len_drift"`
		}
		if err := json.Unmarshal(body, &d); err != nil {
			return fmt.Errorf("decode %s: %w", endpoint, err)
		}
		fmt.Printf("pebble-serve: %s\n\n", serverURL)
		fmt.Printf("  indexed_docs (counter): %d\n", d.IndexedDocsCounter)
		fmt.Printf("  indexed_docs (scan):    %d\n", d.IndexedDocsScan)
		fmt.Printf("  sum_doc_len  (counter): %d\n", d.SumDocLenCounter)
		fmt.Printf("  sum_doc_len  (scan):    %d\n", d.SumDocLenScan)
		if d.OK {
			fmt.Println("\nOK: counters match the 'l' family scan.")
		} else {
			fmt.Printf("\nDRIFT: indexed_docs Δ=%+d, sum_doc_len Δ=%+d\n", d.IndexedDocsDrift, d.SumDocLenDrift)
		}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("counter drift detected (server returned %d)", resp.StatusCode)
	}
	return nil
}

// pebbleInfoJSON shapes the -json output of `cosift pebble-info`. Extracted
// in iter 381 so the shape can be unit-tested without spawning a subprocess.
// Mirrors the /stats response (iter 375 retrievers list, iter 358 vector
// meta) — same jq filters compose against either source.
func pebbleInfoJSON(path string, st store.Stats, sumLen, indexedDocs int64, meta index.HNSWMeta, hnswOK bool) map[string]any {
	retrievers := []string{"bm25", "bm25-mlt"}
	if hnswOK {
		retrievers = append(retrievers, "dense", "hybrid")
	}
	out := map[string]any{
		"path":         path,
		"documents":    st.Documents,
		"indexed_docs": indexedDocs,
		"sum_doc_len":  sumLen,
		"hnsw_loaded":  hnswOK,
		"retrievers":   retrievers,
	}
	if indexedDocs > 0 {
		out["avg_doc_len"] = float64(sumLen) / float64(indexedDocs)
	}
	if hnswOK {
		out["vector_nodes"] = meta.NodeCount
		out["vector_dim"] = meta.Dim
	}
	return out
}

// Iter 217 — operator visibility into LSM levels, WAL state, on-disk
// size, and compaction queue, surfaced via pebble.Metrics().String().
func runPebbleInfo(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("pebble-info", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (defaults to <cfg.DataDir>/pebble)")
	// Iter 380: machine-readable output for tooling / dashboards. Mirrors
	// the shape /stats returns (iter 375's retrievers list, iter 358's
	// vector meta) so the same jq filters compose against either source.
	asJSON := fs.Bool("json", false, "emit JSON instead of human-readable text (no pebble.Metrics dump)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	d := *dir
	if d == "" {
		d = filepath.Join(cfg.DataDir, "pebble")
	}
	ps, err := openPebbleOrFriendlyErr(d)
	if err != nil {
		return err
	}
	defer ps.Close()

	st, err := ps.Stats(ctx)
	if err != nil {
		return err
	}
	sumLen, indexedDocs, _ := ps.CorpusStats(ctx)
	meta, hnswOK, _ := index.LoadHNSWMeta(ctx, ps)

	if *asJSON {
		out := pebbleInfoJSON(d, st, sumLen, indexedDocs, meta, hnswOK)
		blob, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(blob))
		return nil
	}

	fmt.Printf("PebbleStore: %s\n\n", d)
	fmt.Printf("  documents:    %d\n", st.Documents)
	// Iter 285: surface iter-207 counters here too. pebble-info already needs
	// to read the store; reading them costs O(1) and gives operators 'is this
	// store healthy' without round-tripping through pebble-serve /stats.
	if indexedDocs > 0 {
		fmt.Printf("  indexed_docs: %d\n", indexedDocs)
		fmt.Printf("  sum_doc_len:  %d\n", sumLen)
		fmt.Printf("  avg_doc_len:  %.2f\n", float64(sumLen)/float64(indexedDocs))
	}
	// Iter 360: when an HNSW meta blob is persisted, surface dim+nodes here
	// too. Same cheap 20-byte read pebble-serve does at startup (iter 358).
	// Operators running pebble-info to inspect an offline store get the dense
	// shape without opening pebble-serve.
	// Iter 377: also print the retrievers this store can power. bm25/bm25-mlt
	// always. dense/hybrid need the graph (meta present) AND, for non-URL
	// modes, a runtime embedder — offline we can only check the persisted
	// shape, so we report 'available with embedder' rather than asserting.
	if hnswOK {
		fmt.Printf("  vector_nodes: %d\n", meta.NodeCount)
		fmt.Printf("  vector_dim:   %d\n", meta.Dim)
	}
	fmt.Println()
	fmt.Println("--- retrievers ---")
	fmt.Println("  bm25, bm25-mlt: always (BM25 index)")
	if hnswOK {
		fmt.Println("  dense:          available — /find_similar?url= works without embedder;")
		fmt.Println("                  /search /answer /research need an embedder configured at server start")
		fmt.Println("  hybrid:         available (same requirements as dense)")
	} else {
		fmt.Println("  dense, hybrid:  unavailable — no HNSW graph persisted in this store")
		fmt.Println("                  (build embeddings during crawl with cfg.Embeddings.Model set)")
	}
	fmt.Println()
	fmt.Println("--- pebble.Metrics ---")
	fmt.Println(ps.Metrics().String())
	return nil
}

// Iter 228: verify the iter-207 running counters ('m'+indexed_docs,
// 'm'+sum_doc_len) against an authoritative scan of the 'l' family.
// Drift would mean a crash or bug left the counter inconsistent —
// flagged here so it can be re-derived from the scan.
func runVerifyPebble(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (defaults to <cfg.DataDir>/pebble)")
	// Iter 318: machine-readable output for CI integration.
	asJSON := fs.Bool("json", false, "emit JSON report instead of human text (suitable for jq / CI)")
	// Iter 331: -server URL routes the check through a running pebble-serve's
	// /verify endpoint instead of opening the store directly. Useful when a
	// crawl or pebble-serve is holding the writer lock.
	serverURL := fs.String("server", "", "pebble-serve URL — when set, GETs /verify instead of opening the store")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *serverURL != "" {
		return runVerifyViaServer(ctx, *serverURL, *asJSON)
	}
	d := *dir
	if d == "" {
		d = filepath.Join(cfg.DataDir, "pebble")
	}
	ps, err := openPebbleOrFriendlyErr(d)
	if err != nil {
		return err
	}
	defer ps.Close()

	counterSum, counterCount, err := ps.CorpusStats(ctx)
	if err != nil {
		return fmt.Errorf("read counters: %w", err)
	}
	scanSum, scanCount, err := ps.SumDocLengths(ctx)
	if err != nil {
		return fmt.Errorf("scan 'l' family: %w", err)
	}

	driftCount := counterCount - scanCount
	driftSum := counterSum - scanSum
	ok := driftCount == 0 && driftSum == 0

	if *asJSON {
		out := map[string]any{
			"path":                 d,
			"ok":                   ok,
			"indexed_docs_counter": counterCount,
			"indexed_docs_scan":    scanCount,
			"indexed_docs_drift":   driftCount,
			"sum_doc_len_counter":  counterSum,
			"sum_doc_len_scan":     scanSum,
			"sum_doc_len_drift":    driftSum,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
		if !ok {
			return fmt.Errorf("counter drift detected")
		}
		return nil
	}

	fmt.Printf("PebbleStore: %s\n\n", d)
	fmt.Printf("  indexed_docs (counter): %d\n", counterCount)
	fmt.Printf("  indexed_docs (scan):    %d\n", scanCount)
	fmt.Printf("  sum_doc_len  (counter): %d\n", counterSum)
	fmt.Printf("  sum_doc_len  (scan):    %d\n", scanSum)
	if ok {
		fmt.Println("\nOK: counters match the 'l' family scan.")
		return nil
	}
	fmt.Printf("\nDRIFT: indexed_docs Δ=%+d, sum_doc_len Δ=%+d\n", driftCount, driftSum)
	return fmt.Errorf("counter drift detected")
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeProblem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":  http.StatusText(status),
		"status": status,
		"detail": detail,
	})
}
