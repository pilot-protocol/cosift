package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"

	"github.com/pilot-protocol/cosift/internal/answercache"
	"github.com/pilot-protocol/cosift/internal/authority"
	"github.com/pilot-protocol/cosift/internal/chatgate"
	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/rerank"
	"github.com/pilot-protocol/cosift/internal/sla"
	"github.com/pilot-protocol/cosift/internal/store"
)

func runPebbleServe(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("pebble-serve", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (required; the SQLite cfg.DataDir is ignored)")
	addr := fs.String("addr", cfg.Server.Addr, "listen address (defaults to server.addr from cosift.json)")
	// -crawl-seeds-file lets pebble-serve run an in-process crawler
	// against the same PebbleStore. Eliminates the writer-lock dance between
	// cosift-serve and cosift-crawl — search + crawl + index growth all in one
	// process, one binary, one lock. The crawler runs as a goroutine for the
	// lifetime of the server.
	crawlSeeds := fs.String("crawl-seeds-file", "", "if set, run a continuous in-process crawler against these seeds while serving")
	crawlCheckpoint := fs.Duration("crawl-checkpoint", 60*time.Second, "HNSW persist cadence for the in-serve crawler")
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

	// log corpus size at startup so operators see whether the
	// store opened with the expected doc count (silent open would force them
	// to curl /stats just to confirm a restart loaded the right data dir).
	if _, indexedDocs, err := ps.CorpusStats(ctx); err == nil && indexedDocs > 0 {
		log.Printf("pebble-serve: opened store with %d indexed docs", indexedDocs)
	}

	// Meta is 20 bytes (dim+nodeCount); first-entry probe is the fallback
	// when meta is absent but vector entries exist (partial persist).
	// We never load the full graph here — gigabytes of RAM at 10M-vector scale.
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
	// Loading is
	// gigabytes of RAM at production scale (10M vectors × 1536 dim ≈ 60GB),
	// so it's opt-in. When the env var isn't set we just keep the cheap meta
	// snapshot from and operators see has_vectors=true.
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
			// Default 50 from NewHNSW
			// underfits big graphs grown via AddPassage; raising to ~200
			// restored Recall@10 from 0.47 to ~0.85 on the 800K-vector
			// production corpus without a graph rebuild.
			if v := os.Getenv("COSIFT_HNSW_EF_SEARCH"); v != "" {
				if n, err := strconv.Atoi(v); err == nil && n > 0 {
					g.SetEfSearch(n)
					log.Printf("pebble-serve: HNSW efSearch override = %d", n)
				}
			}
			// if a PQ codebook + codes exist in this store, wire
			// them so /search uses asymmetric PQ distance (much faster on
			// large graphs). When absent, search falls back to raw vectors.
			//
			// bench-pq exposed that PQ on this dim=768 corpus drops
			// Recall@10 from ~0.89 to ~0.60 — the 32× compression has a
			// recall cost that may exceed the speed benefit. Operators can
			// keep the codes on disk but disable the runtime path by setting
			// COSIFT_DISABLE_PQ=true.
			if os.Getenv("COSIFT_DISABLE_PQ") == "true" {
				log.Printf("pebble-serve: PQ disabled via COSIFT_DISABLE_PQ — using raw vectors for search")
			} else if cb, cbOK, _ := index.LoadPQCodebook(ctx, ps); cbOK {
				codes := make([][]uint16, g.Len())
				loaded := 0
				if err := ps.IteratePQCodes(ctx, func(nodeID uint64, blob []byte) bool {
					if int(nodeID) >= len(codes) {
						return true
					}
					// codebook-aware decode handles both byte-packed
					// (new, K≤256) and uint16 LE (legacy) blob shapes.
					code, err := cb.DecodeCodeBlob(blob)
					if err != nil || len(code) != cb.M {
						return true
					}
					codes[int(nodeID)] = code
					loaded++
					return true
				}); err != nil {
					log.Printf("pebble-serve: PQ codes iterate failed: %v — falling back to raw vectors", err)
				} else {
					g.UsePQ(cb, codes)
					log.Printf("pebble-serve: PQ search enabled (codebook: dim=%d M=%d K=%d, codes=%d/%d)",
						cb.Dim, cb.M, cb.K, loaded, g.Len())
				}
			}
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

	// configurable HyDE + paraphrase cache caps (defaults 256).
	hydeCap := 256
	if v := os.Getenv("COSIFT_HYDE_CACHE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			hydeCap = n
			log.Printf("pebble-serve: HyDE cache size override = %d", n)
		} else {
			// surface unparseable env vars as warnings at startup
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
	// Wikipedia / kernel.org / arxiv float
	// above spam directories in the long tail. Optional Tranco /
	// Majestic CSVs at COSIFT_TRANCO_CSV / COSIFT_MAJESTIC_CSV improve
	// the score; with no CSVs the embedded whitelist + TLD heuristics
	// still produce useful signals.
	scorer := authority.New()
	if v := os.Getenv("COSIFT_AUTHORITY_ALPHA"); v != "" {
		if a, err := strconv.ParseFloat(v, 64); err == nil && a >= 0 {
			scorer = scorer.WithAlpha(a)
		}
	}
	if path := os.Getenv("COSIFT_TRANCO_CSV"); path != "" {
		if f, err := os.Open(path); err == nil {
			n, _ := scorer.LoadTranco(f)
			_ = f.Close()
			log.Printf("pebble-serve: authority Tranco loaded (%d entries from %s)", n, path)
		} else {
			log.Printf("pebble-serve: WARN COSIFT_TRANCO_CSV=%q: %v", path, err)
		}
	}
	if path := os.Getenv("COSIFT_MAJESTIC_CSV"); path != "" {
		if f, err := os.Open(path); err == nil {
			n, _ := scorer.LoadMajestic(f)
			_ = f.Close()
			log.Printf("pebble-serve: authority Majestic loaded (%d entries from %s)", n, path)
		} else {
			log.Printf("pebble-serve: WARN COSIFT_MAJESTIC_CSV=%q: %v", path, err)
		}
	}
	idx = idx.WithAuthority(scorer)
	log.Printf("pebble-serve: authority scoring enabled (alpha=%.2f, trusted=%d, tranco=%d, majestic=%d)",
		scorer.Alpha(), scorer.Stats().TrustedHosts, scorer.Stats().TrancoEntries, scorer.Stats().MajesticEntries)
	// COSIFT_BM25_K1 / COSIFT_BM25_B override per instance.
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
		authority:    scorer,
		// capture doc count at startup so /stats can compute
		// per-minute crawl rate without long-term counters.
		startupDocs: vectorNodes, // placeholder; overwritten below
	}
	// Read actual doc count at startup for crawl-rate baseline. Uses
	// CorpusStats (the atomic-mirror indexed_docs counter) — NOT
	// Stats.Documents which scans the raw 'd' family. The two diverge
	// in two ways:
	//
	//   - Stats.Documents may include partially-upserted rows that the
	//     indexed_docs counter hasn't seen yet, leaving startupDocs >
	//     indexedDocs and freezing docs_added_since_start at 0 forever.
	//   - On a cold corpus, Stats does a 12s scan that returns the
	//     count AT END-OF-SCAN, by which time the crawler has already
	//     added 50+ more docs, pushing startupDocs above the post-boot
	//     indexed_docs atomic.
	//
	// CorpusStats reads the atomic directly (or bootstraps it from the
	// indexed_docs meta key on first call), giving an exact match with
	// the indexedDocs value the rate calc uses below.
	if _, count, err := ps.CorpusStats(ctx); err == nil && count > 0 {
		srv.startupDocs = int(count)
	} else {
		srv.startupDocs = 0
	}
	// stamp cluster config onto srv early so /search's
	// gateway-mode check fires even on nodes that don't run a crawler.
	if err := cfg.Cluster.Validate(); err != nil {
		log.Printf("pebble-serve: cluster config invalid (%v) — running single-node", err)
	} else {
		srv.cluster = cfg.Cluster
		if cfg.Cluster.IsClustered() {
			role := "leaf"
			if cfg.Cluster.GatewayMode {
				role = "gateway+leaf"
			}
			log.Printf("pebble-serve: cluster mode (role=%s, shard=%d/%d, peers=%d)",
				role, cfg.Cluster.MyShardID, cfg.Cluster.NumShards, len(cfg.Cluster.Peers))
		}
	}
	// Uses the same OpenAI-compatible chat
	// client the SQLite-side server uses; works against OpenAI, Together,
	// Azure, llama.cpp, vLLM, Ollama, anything speaking /v1/chat/completions.
	// anonymous auth when cfg.Chat.URL is set (self-hosted, no key
	// needed). Required only for hosted defaults (empty URL = api.openai.com).
	// Shared between answer + rerank pools so an
	// /answer burst can't also exhaust the rerank pool's goroutines.
	srv.chatGate = chatgate.New(
		envIntDefault("COSIFT_LLM_GATE_RERANK", 8),
		envIntDefault("COSIFT_LLM_GATE_ANSWER", 4),
	)
	// /answer response cache. Default TTL 60s + 1024-entry cap is a
	// reasonable starting point for a workload with a few hundred
	// recurring queries; raise via COSIFT_ANSWER_CACHE_TTL_SEC for
	// stable corpora, disable with =0 if every query is unique.
	srv.answerCache = answercache.New(
		time.Duration(envIntDefault("COSIFT_ANSWER_CACHE_TTL_SEC", 60))*time.Second,
		envIntDefault("COSIFT_ANSWER_CACHE_CAP", 1024),
	)
	srv.researchCache = answercache.New(
		time.Duration(envIntDefault("COSIFT_RESEARCH_CACHE_TTL_SEC", 600))*time.Second,
		envIntDefault("COSIFT_RESEARCH_CACHE_CAP", 256),
	)
	// Query log (observability). COSIFT_QUERY_LOG=/path enables JSONL append
	// of every query-endpoint request. Disabled if unset.
	if qp := os.Getenv("COSIFT_QUERY_LOG"); qp != "" {
		if f, err := os.OpenFile(qp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
			srv.qlogFile = f
			log.Printf("pebble-serve: query log enabled → %s", qp)
		} else {
			log.Printf("pebble-serve: query log open failed (%s): %v", qp, err)
		}
	}
	// Feedback rate limiter — always on (stricter than global). Per-client via
	// XFF. Override COSIFT_FEEDBACK_RPM / _BURST.
	srv.fbRL = &rateLimiter{
		rpm:       float64(envIntDefault("COSIFT_FEEDBACK_RPM", 20)),
		burst:     float64(envIntDefault("COSIFT_FEEDBACK_BURST", 5)),
		whitelist: map[string]bool{},
	}
	// Feedback log. COSIFT_FEEDBACK_LOG=/path, or defaults beside the query log.
	if fp := feedbackLogPath(); fp != "" {
		if f, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
			srv.fbFile = f
			log.Printf("pebble-serve: feedback log enabled → %s", fp)
		} else {
			log.Printf("pebble-serve: feedback log open failed (%s): %v", fp, err)
		}
	}
	// Tries the vLLM /metrics endpoint at the chat origin (works for
	// self-hosted vLLM, no-op against OpenAI). Threshold default 8 — that
	// matches the answer-pool cap so when the chat backend is already
	// saturated we stop trying to add more LLM work.
	if cfg.Chat.URL != "" {
		probeURL := chatgate.VLLMMetricsURLFrom(cfg.Chat.URL)
		threshold := envIntDefault("COSIFT_LLM_DEGRADE_QUEUE", 8)
		interval := envDurationMsDefault("COSIFT_LLM_PROBE_MS", 2*time.Second)
		srv.llmProbe = chatgate.NewLoadProbe(probeURL, threshold, interval)
		go srv.llmProbe.Run(ctx)
		log.Printf("pebble-serve: vLLM load probe started (url=%s, threshold=%d, interval=%s)", probeURL, threshold, interval)
	}
	// Defaults match the
	// June-3 baseline measurements; ops can override per env. Violation
	// log defaults under cfg.DataDir so it co-locates with the rest of
	// the operational state.
	slaLogPath := os.Getenv("COSIFT_SLA_LOG_PATH")
	if slaLogPath == "" && cfg.DataDir != "" {
		slaLogPath = filepath.Join(cfg.DataDir, "sla-violations.jsonl")
	}
	slaTargets := []sla.Target{
		{Endpoint: "/search", P95: envDurationMsDefault("COSIFT_SLA_SEARCH_P95_MS", 1500*time.Millisecond), P99: envDurationMsDefault("COSIFT_SLA_SEARCH_P99_MS", 4*time.Second), MaxErrorRate: 0.02},
		{Endpoint: "/answer", P95: envDurationMsDefault("COSIFT_SLA_ANSWER_P95_MS", 8*time.Second), P99: envDurationMsDefault("COSIFT_SLA_ANSWER_P99_MS", 20*time.Second), MaxErrorRate: 0.05},
		{Endpoint: "/research", P95: envDurationMsDefault("COSIFT_SLA_RESEARCH_P95_MS", 30*time.Second), P99: envDurationMsDefault("COSIFT_SLA_RESEARCH_P99_MS", 60*time.Second), MaxErrorRate: 0.05},
		{Endpoint: "/healthz", P95: 50 * time.Millisecond, P99: 200 * time.Millisecond, MaxErrorRate: 0.01},
		{Endpoint: "/stats", P95: 200 * time.Millisecond, P99: time.Second, MaxErrorRate: 0.01},
	}
	slaWindow := envDurationMsDefault("COSIFT_SLA_WINDOW_MS", 5*time.Minute)
	slaEvalInterval := envDurationMsDefault("COSIFT_SLA_EVAL_MS", 30*time.Second)
	if m, err := sla.New(slaTargets, slaWindow, slaLogPath); err == nil {
		srv.sla = m
		go srv.sla.Run(ctx.Done(), slaEvalInterval)
		log.Printf("pebble-serve: SLA monitor started (window=%s, eval=%s, log=%s)", slaWindow, slaEvalInterval, slaLogPath)
	} else {
		log.Printf("pebble-serve: SLA monitor disabled: %v", err)
	}
	// Holds the unwrapped OpenAIChatClient so the rerank
	// wrapper below can sit directly on top of the inner client (single
	// gate acquisition per call) instead of stacking on srv.chatSafe.
	var rawChatForRerank embed.ChatClient
	if cfg.Chat.Model != "" {
		apiKey := resolveAPIKey("chat")
		rawChat := embed.NewOpenAIChat(apiKey, cfg.Chat.URL, cfg.Chat.Model)
		rawChatForRerank = rawChat
		srv.chatSafe = chatgate.NewSafeChat(rawChat, chatgate.Options{
			Gate:          srv.chatGate,
			Kind:          chatgate.KindAnswer,
			StageDeadline: envDurationMsDefault("COSIFT_LLM_DEADLINE_ANSWER_MS", 30*time.Second),
			MaxRetries:    envIntDefault("COSIFT_LLM_RETRIES", 1),
		})
		srv.chat = srv.chatSafe
		auth := "anonymous"
		if apiKey != "" {
			auth = "bearer-token"
		}
		st := srv.chatGate.Stats()
		log.Printf("pebble-serve: /answer enabled (chat model=%s, auth=%s, gate=answer:%d/rerank:%d)",
			cfg.Chat.Model, auth, st.AnswerCap, st.RerankCap)
	}
	// Same anonymous-when-URL
	// semantics as chat above.
	if cfg.Embeddings.Model != "" {
		apiKey := resolveAPIKey("embed")
		// when cfg.Embeddings.URLs is set, build one client per
		// URL and wrap them in a RoundRobinEmbedder so cosift fans embed
		// requests across multiple backends. Falls back to single-URL
		// behavior when URLs is empty.
		urls := cfg.Embeddings.URLs
		if len(urls) == 0 {
			urls = []string{cfg.Embeddings.URL}
		}
		clients := make([]embed.Embedder, 0, len(urls))
		for _, u := range urls {
			clients = append(clients, embed.NewOpenAIClient(apiKey, u, cfg.Embeddings.Model, cfg.Embeddings.Dim))
		}
		var base embed.Embedder = embed.NewRoundRobinEmbedder(clients)
		// Both search and
		// crawler embed paths share the cache layer — same text returns
		// instantly on re-fetch / re-query, no ollama call.
		cacheNote := ""
		if cfg.Embeddings.CacheDir != "" {
			base = embed.NewCachedEmbedder(base, cfg.Embeddings.CacheDir)
			cacheNote = fmt.Sprintf(", cache=%s", cfg.Embeddings.CacheDir)
		}
		srv.embedder = base
		auth := "anonymous"
		if apiKey != "" {
			auth = "bearer-token"
		}
		log.Printf("pebble-serve: embedder configured (model=%s, dim=%d, auth=%s, backends=%d%s)",
			cfg.Embeddings.Model, cfg.Embeddings.Dim, auth, len(clients), cacheNote)
	}
	// wire rerank.Reranker when cfg.Rerank is configured. Two paths
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
		// when cfg.Rerank.Model is set and differs from cfg.Chat.Model,
		// build a separate chat client for rerank pointing at the same /v1 URL
		// but with the alternate model. Lets operators dedicate a small/fast
		// model (e.g. qwen3.5:0.8b) to rerank so it doesn't queue behind chat
		// generations on the same model. Falls back to the main chat client
		// when Rerank.Model is empty or equal to Chat.Model.
		var rerankInner embed.ChatClient = rawChatForRerank
		if cfg.Rerank.Model != "" && cfg.Rerank.Model != cfg.Chat.Model {
			apiKey := resolveAPIKey("chat")
			rerankInner = embed.NewOpenAIChat(apiKey, cfg.Chat.URL, cfg.Rerank.Model)
			log.Printf("pebble-serve: rerank using dedicated chat (model=%s, url=%s)", cfg.Rerank.Model, cfg.Chat.URL)
		}
		srv.rerankSafe = chatgate.NewSafeChat(rerankInner, chatgate.Options{
			Gate:          srv.chatGate,
			Kind:          chatgate.KindRerank,
			StageDeadline: envDurationMsDefault("COSIFT_LLM_DEADLINE_RERANK_MS", 5*time.Second),
			MaxRetries:    envIntDefault("COSIFT_LLM_RETRIES", 1),
		})
		srv.reranker = rerank.NewLLMReranker(srv.rerankSafe)
		log.Printf("pebble-serve: rerank enabled (llm: %s, gated)", srv.rerankSafe.Model())
	}
	srv.rerankCandK = cfg.Rerank.CandidateK
	if srv.rerankCandK <= 0 {
		srv.rerankCandK = 20
	}
	srv.hostBoosts = cfg.Defaults.HostBoosts

	mux := http.NewServeMux()
	// Built once from env;
	// nil when disabled (COSIFT_RATELIMIT_RPM unset or 0). Wraps every route
	// below — including /healthz so monitoring hits are budgeted too;
	// operators wanting unlimited probes should set
	// COSIFT_RATELIMIT_WHITELIST to include their monitoring source.
	srv.rl = newRateLimiterFromEnv()
	if srv.rl != nil {
		log.Printf("pebble-serve: rate limit active (rpm=%.0f burst=%.0f whitelist=%v)", srv.rl.rpm, srv.rl.burst, srv.rl.whitelistList())
	}
	wrap := func(h http.HandlerFunc) http.HandlerFunc { return srv.count(srv.rateLimit(h)) }
	// qwrap adds query logging (innermost, so it sees the real status+bytes) for
	// the user-facing query endpoints — the observability substrate we lacked.
	qwrap := func(h http.HandlerFunc) http.HandlerFunc { return srv.count(srv.rateLimit(srv.qlog(h))) }
	// awrap = wrap + admin auth. All /admin/* routes go through this so the
	// peer-token gate is enforced at the mux level (belt-and-suspenders with any
	// per-handler check), closing gaps where a handler forgets to inline it.
	awrap := func(h http.HandlerFunc) http.HandlerFunc { return srv.count(srv.rateLimit(srv.requireAdmin(h))) }
	// landing page at / and OpenAPI 3.1 spec at /openapi.json.
	// Both embedded into the binary at build time — operators get a single
	// self-contained executable, no separate static-asset deployment.
	mux.HandleFunc("GET /", wrap(srv.handleLanding))
	// Streams /answer or /research SSE
	// into a multi-turn conversation view with citation rendering.
	mux.HandleFunc("GET /chat", wrap(srv.handleChat))
	mux.HandleFunc("GET /openapi.json", wrap(srv.handleOpenAPI))
	// Swagger UI bundled into the binary. /docs renders the
	// interactive spec; /docs/swagger-ui.css and /docs/swagger-ui-bundle.js
	// serve the dist assets locally so the page works air-gapped (no CDN).
	mux.HandleFunc("GET /docs", wrap(srv.handleSwaggerUI))
	mux.HandleFunc("GET /docs/{file...}", wrap(srv.handleSwaggerAsset))
	mux.HandleFunc("GET /healthz", wrap(srv.handleHealthz))
	mux.HandleFunc("GET /find", qwrap(srv.handleFind))
	mux.HandleFunc("GET /stats", wrap(srv.handleStats))
	mux.HandleFunc("GET /domains", wrap(srv.handleDomains))
	// frontier queue visibility — counts by status + top-N
	// hosts in queue. Distinct from /domains which is over INDEXED docs.
	mux.HandleFunc("GET /queue", wrap(srv.handleQueue))
	// Single-node deployments still expose
	// this; it's just unused. Authenticated by cfg.Cluster.PeerAuthToken
	// (Bearer); when token is empty, requests from any source are accepted.
	mux.HandleFunc("POST /admin/crawl-enqueue", awrap(srv.handleCrawlEnqueue))
	mux.HandleFunc("POST /admin/allow-domain", awrap(srv.handleAllowDomain))
	mux.HandleFunc("POST /admin/frontier-purge-host", awrap(srv.handleFrontierPurgeHost))
	mux.HandleFunc("POST /admin/frontier-clear", awrap(srv.handleFrontierClear))
	mux.HandleFunc("POST /admin/frontier-demote-host", awrap(srv.handleFrontierDemoteHost))
	mux.HandleFunc("POST /admin/frontier-purge-stale-inflight", awrap(srv.handleFrontierPurgeStaleInFlight))
	mux.HandleFunc("POST /admin/rss-import", awrap(srv.handleRSSImport))
	mux.HandleFunc("POST /admin/crawl-now", awrap(srv.handleCrawlNow))
	mux.HandleFunc("POST /admin/wet-import", awrap(srv.handleWETImport))
	mux.HandleFunc("POST /admin/wet-import-bulk", awrap(srv.handleWETImportBulk))
	mux.HandleFunc("POST /admin/site-pack", awrap(srv.handleSitePack))
	mux.HandleFunc("POST /admin/site-submit", awrap(srv.handleSiteSubmit))
	mux.HandleFunc("POST /admin/embed-backfill", awrap(srv.handleEmbedBackfill))
	mux.HandleFunc("POST /admin/host-backfill", awrap(srv.handleHostBackfill))
	mux.HandleFunc("GET /admin/eval-quick", awrap(srv.handleEvalQuick))
	mux.HandleFunc("POST /admin/hnsw-compact", awrap(srv.handleHNSWCompact))
	mux.HandleFunc("GET /query", qwrap(srv.handleQuery))
	mux.HandleFunc("POST /query", qwrap(srv.handleQuery))
	// import a sitemap.xml (or sitemap-index) and push every
	// listed URL into the live frontier.
	mux.HandleFunc("POST /admin/sitemap-import", awrap(srv.handleSitemapImport))
	mux.HandleFunc("POST /admin/recrawl-sitemap", awrap(srv.handleRecrawlSitemap))
	// PQ training admin endpoint. Same auth as crawl-enqueue
	// (Bearer cfg.Cluster.PeerAuthToken). Runs synchronously — for the
	// 224K-vec corpus we have today it takes ~minutes; operator-only.
	mux.HandleFunc("POST /admin/pq-train", awrap(srv.handlePQTrain))
	// backfill-only — re-encode every node that doesn't have a
	// code yet against the existing codebook. No retrain. Fast.
	mux.HandleFunc("POST /admin/pq-encode", awrap(srv.handlePQEncode))
	// Creates a hard-linked, consistent
	// snapshot dir that's safe to tar without racing background compactions.
	mux.HandleFunc("POST /admin/checkpoint", awrap(srv.handleCheckpoint))
	mux.HandleFunc("GET /search", qwrap(srv.handleSearch))
	mux.HandleFunc("POST /search", qwrap(srv.handleSearchPOST))
	mux.HandleFunc("GET /contents", wrap(srv.handleContents))
	mux.HandleFunc("POST /contents", wrap(srv.handleContentsBatch))
	mux.HandleFunc("GET /verify", wrap(srv.handleVerify))
	mux.HandleFunc("GET /metrics", wrap(srv.handleMetrics))
	mux.HandleFunc("GET /admin/query-log", awrap(srv.handleQueryLog))
	mux.HandleFunc("POST /feedback", wrap(srv.handleFeedback))
	mux.HandleFunc("GET /admin/feedback", awrap(srv.handleFeedbackList))
	mux.HandleFunc("GET /sla", wrap(srv.handleSLA))
	mux.HandleFunc("GET /admin/domains-audit", awrap(srv.handleDomainsAudit))
	mux.HandleFunc("GET /find_similar", qwrap(srv.handleFindSimilar))
	mux.HandleFunc("POST /find_similar", qwrap(srv.handleFindSimilarPOST))
	mux.HandleFunc("GET /answer", qwrap(srv.handleAnswer))
	mux.HandleFunc("POST /answer", qwrap(srv.handleAnswerPOST))
	mux.HandleFunc("GET /research", qwrap(srv.handleResearch))
	mux.HandleFunc("POST /research", qwrap(srv.handleResearchPOST))

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MB
	}

	log.Printf("pebble-serve: listening on %s (PebbleStore at %s)", *addr, *dir)
	// Production state observed on GH200: after a restart series that
	// healed HNSW zombies, the meta counters showed 0 while the 'l'
	// family had 10.87M entries — degraded BM25 length normalization.
	// Off the hot path so HNSW load + first request aren't blocked.
	go func() {
		// Brief settle so an active IndexDocument can populate counters
		// the cheap way before we kick the O(N) scan.
		time.Sleep(10 * time.Second)
		sumLen, count, recomputed, err := ps.BootstrapCorpusStats(ctx)
		if err != nil {
			log.Printf("pebble-serve: corpus stats bootstrap failed: %v", err)
			return
		}
		if recomputed {
			log.Printf("pebble-serve: corpus stats bootstrapped (indexed_docs=%d, sum_doc_len=%d)", count, sumLen)
		}
	}()
	// Without this the
	// subdomain-density penalty in authority.Score never fires — we
	// saw 6.68M .cfd/.sbs hosts on GH200 sitting at score 0.20 instead
	// of dropping to "block." Single-pass iteration is ~18s at 9M hosts;
	// runs off the hot path so HNSW load isn't blocked. Re-runs every
	// 6h to track ongoing crawl growth without operator intervention.
	go func() {
		// Initial settle so the rest of startup completes first.
		time.Sleep(15 * time.Second)
		refresh := func() {
			counts := map[string]int{}
			err := ps.IterateDomains(ctx, func(host string, _ int) bool {
				// Use the Scorer's eTLD+1 logic so the bucket keys we
				// produce here match what Score() looks up — without this
				// the publicsuffix-aware Scorer keys on "bbc.co.uk" while
				// the bootstrap keys on "co.uk" and the lookup misses.
				e := authority.ETLD1(host)
				if e == "" {
					return true
				}
				counts[e]++
				return true
			})
			if err != nil {
				log.Printf("pebble-serve: subdomain-count refresh: %v", err)
				return
			}
			srv.authority.SetSubdomainCounts(counts)
			log.Printf("pebble-serve: authority subdomain counts refreshed (%d eTLD+1 buckets)", len(counts))
		}
		refresh()
		// Re-scan periodically to absorb ongoing crawl growth.
		t := time.NewTicker(6 * time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				refresh()
			}
		}
	}()
	// Only
	// listens on loopback so it can never be exposed publicly through the
	// reverse proxy. Set COSIFT_PPROF_ADDR=127.0.0.1:6060 to enable.
	if pprofAddr := os.Getenv("COSIFT_PPROF_ADDR"); pprofAddr != "" {
		// optional mutex / block sampling so the
		// dense-search-latency investigation has data. Off by default
		// because each sample is a couple-µs overhead per contended
		// lock event. Set rate ≥1 to enable; 1000 means "sample 1 in
		// 1000 lock waits" which is plenty for diagnosis.
		if v := os.Getenv("COSIFT_MUTEX_PROFILE_FRACTION"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				runtime.SetMutexProfileFraction(n)
				log.Printf("pebble-serve: mutex profile fraction = %d", n)
			}
		}
		if v := os.Getenv("COSIFT_BLOCK_PROFILE_RATE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				runtime.SetBlockProfileRate(n)
				log.Printf("pebble-serve: block profile rate = %d", n)
			}
		}
		pprofMux := http.NewServeMux()
		pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
		pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		go func() {
			log.Printf("pebble-serve: pprof listening on %s", pprofAddr)
			// Bare ListenAndServe uses zero-value Server with no timeouts —
			// a slow-reader on the pprof port can pin goroutines indefinitely.
			// WriteTimeout is generous because CPU profile and trace endpoints
			// stream for 30s+ by design.
			srv := &http.Server{
				Addr:              pprofAddr,
				Handler:           pprofMux,
				ReadHeaderTimeout: 10 * time.Second,
				ReadTimeout:       30 * time.Second,
				WriteTimeout:      5 * time.Minute,
				IdleTimeout:       120 * time.Second,
				MaxHeaderBytes:    1 << 20,
			}
			if err := srv.ListenAndServe(); err != nil {
				log.Printf("pebble-serve: pprof exited: %v", err)
			}
		}()
	}
	go func() {
		<-ctx.Done()
		log.Printf("pebble-serve: shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
	}()

	// Reuses ps + srv.hnsw + srv.embedder. The
	// crawler.Crawler uses concurrency from cfg.Crawler.MaxConcurrent. Vectors
	// land into the SAME HNSW pointer that /search reads from, so freshly
	// crawled content is searchable as soon as AddPassage returns.
	// Checkpoint goroutine persists the graph every crawlCheckpoint.
	// collect a WaitGroup of crawler goroutines so the final
	// HNSW persist runs BEFORE ps.Close() — fixes a 'pebble: closed' panic.
	var crawlWG sync.WaitGroup
	if *crawlSeeds != "" {
		if err := srv.startInProcessCrawl(ctx, ps, *crawlSeeds, *crawlCheckpoint, cfg, &crawlWG); err != nil {
			log.Printf("pebble-serve: in-process crawler not started: %v", err)
		}
	}

	servErr := httpSrv.ListenAndServe()
	// wait for crawler + checkpoint goroutines to finish their
	// final persist before the deferred ps.Close() runs. Otherwise Persist
	// can race ps.Close() and panic.
	crawlWG.Wait()
	if servErr != nil && servErr != http.ErrServerClosed {
		return servErr
	}
	return nil
}

// startInProcessCrawl wires the crawler-inside-serve flow. Bumps
// srv.hnsw to a fresh empty graph if none was loaded (so dense retrieval is
// live as soon as the first passage lands), then runs the crawler in a
// goroutine for the server's lifetime.
func (s *pebbleHTTP) startInProcessCrawl(ctx context.Context, ps *store.PebbleStore, seedsFile string, ckpEvery time.Duration, cfg *config.Config, wg *sync.WaitGroup) error {
	if s.embedder == nil {
		return errors.New("crawl requires embedder configuration (cfg.Embeddings.Model)")
	}
	buf, err := os.ReadFile(seedsFile)
	if err != nil {
		return fmt.Errorf("read seeds file: %w", err)
	}
	var seeds []string
	for _, line := range strings.Split(string(buf), "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		seeds = append(seeds, t)
	}
	if len(seeds) == 0 {
		return errors.New("seeds file empty after parsing")
	}

	// Ensure HNSW is non-nil so the bridge can call AddPassage. /search reads
	// the same pointer, so growth is immediately searchable.
	if s.hnsw == nil {
		s.hnsw = index.NewHNSW(s.embedder.Dim())
		log.Printf("in-serve crawler: created fresh HNSW (dim=%d)", s.embedder.Dim())
	}

	c := crawler.NewWithBackend(cfg.Crawler, ps, index.NewPebbleBM25(ps))
	// crawler embedder is wrapped in a semaphore-throttled
	// wrapper so the bulk crawl can never queue enough requests at
	// ollama to starve interactive /search?retriever=dense calls.
	// Default cap = 8 (well under OLLAMA_NUM_PARALLEL=32); operator
	// override via COSIFT_CRAWL_EMBED_CONCURRENCY.
	const crawlEmbedDefault = 8
	crawlEmbedCap := crawlEmbedDefault
	if v := os.Getenv("COSIFT_CRAWL_EMBED_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			crawlEmbedCap = n
		}
	}
	// The throttle exists specifically so bulk-crawl embeds can't starve
	// interactive /search. Overriding it far above the safe range silently
	// defeats that protection, so warn loudly — but never clamp, since an
	// operator on a large box may legitimately want more. Safe ceiling tracks
	// OLLAMA_NUM_PARALLEL when set, else 4x the default.
	safeCrawlEmbed := 4 * crawlEmbedDefault
	if v := os.Getenv("OLLAMA_NUM_PARALLEL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			safeCrawlEmbed = n
		}
	}
	if crawlEmbedCap > safeCrawlEmbed {
		log.Printf("WARNING: COSIFT_CRAWL_EMBED_CONCURRENCY=%d is far above the safe ceiling (%d); bulk-crawl embeds at this concurrency can starve interactive /search — lower it if search latency spikes", crawlEmbedCap, safeCrawlEmbed)
	}
	// Coalesces many
	// per-doc embed calls into one larger inner.Embed per ~20 ms window
	// (up to maxBatch=128 texts). Search bypasses the batcher (uses
	// s.embedder directly) so interactive latency stays sub-100 ms.
	// Operator overrides:
	//   COSIFT_EMBED_BATCH=128       — texts per inner call
	//   COSIFT_EMBED_BATCH_WAIT_MS=20 — drain timer in ms
	batchMax := 128
	if v := os.Getenv("COSIFT_EMBED_BATCH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			batchMax = n
		}
	}
	batchWait := 20 * time.Millisecond
	if v := os.Getenv("COSIFT_EMBED_BATCH_WAIT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			batchWait = time.Duration(n) * time.Millisecond
		}
	}
	batchedEmbedder := embed.NewBatchingEmbedder(s.embedder, batchMax, batchWait)
	crawlEmbedder := embed.NewThrottledEmbedder(batchedEmbedder, crawlEmbedCap)
	log.Printf("pebble-serve: crawler embed = throttle(%d) → batch(max=%d, wait=%s) → round-robin → backends; search bypasses batcher",
		crawlEmbedCap, batchMax, batchWait)
	c = c.WithEmbedder(crawlEmbedder)
	c = c.WithPassageWriter(&hnswPassageWriter{ps: ps, hnsw: s.hnsw})
	// Single-node config
	// (NumShards <= 1) makes route fn a no-op (every URL ownsLocally).
	// s.cluster was stamped at server-init time so search-only nodes also
	// see it; we just need the router here.
	if cfg.Cluster.IsClustered() {
		c = c.WithRouter(
			func(url string) (bool, string) {
				if cfg.Cluster.OwnsURL(url) {
					return true, ""
				}
				return false, cfg.Cluster.PeerForURL(url)
			},
			s.forwardURLToPeer,
		)
		log.Printf("in-serve crawler: cluster mode (shard=%d/%d, peers=%d)",
			cfg.Cluster.MyShardID, cfg.Cluster.NumShards, len(cfg.Cluster.Peers))
	}
	// Expose Seed so /admin/crawl-enqueue can hand off forwarded URLs.
	s.crawlSeed = c.Seed
	// expose SeedSitemap so /admin/sitemap-import can push
	// sitemap-discovered URLs into the live frontier.
	s.crawlSeedSitemap = c.SeedSitemap
	// expose SeedSitemapLane so /admin/site-submit can push a whole site's
	// URLs into a chosen priority lane (default: submitted/priority).
	s.crawlSeedSitemapLane = c.SeedSitemapLane
	// expose Recrawl so /admin/recrawl-sitemap can reset done/errored URLs.
	s.crawlRecrawl = c.Recrawl
	s.crawlSeedRSS = c.SeedRSS
	s.crawlFetchNow = c.FetchAndIndexNow
	s.crawlSeedWET = c.SeedWET
	s.crawlAllowDomain = c.AddAllowedDomain
	for _, u := range seeds {
		// Only seed locally-owned URLs in cluster mode; the rest get forwarded.
		if cfg.Cluster.IsClustered() && !cfg.Cluster.OwnsURL(u) {
			if err := s.forwardURLToPeer(ctx, u, cfg.Cluster.PeerForURL(u)); err != nil {
				log.Printf("in-serve crawler: forward initial seed %s: %v", u, err)
			}
			continue
		}
		_ = c.Seed(u)
	}
	log.Printf("in-serve crawler: %d seeds queued (concurrency=%d, depth=%d, checkpoint=%s)",
		len(seeds), cfg.Crawler.MaxConcurrent, cfg.Crawler.MaxDepth, ckpEvery)
	s.crawlActive = true

	// Checkpoint goroutine: incremental persist via PersistFrom — each
	// tick only writes nodes [lastN, n). Shutdown does a full persist
	// from 0 so any backlinks added to older nodes get refreshed.
	// Seeding lastN from the loaded graph means the first checkpoint
	// after restart only writes nodes added during this run; the prior
	// N are already on disk.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(ckpEvery)
		defer t.Stop()
		lastN := s.hnsw.Len()
		if lastN > 0 {
			log.Printf("in-serve crawler: checkpoint baseline = %d nodes (loaded from disk)", lastN)
		}
		for {
			select {
			case <-ctx.Done():
				n := s.hnsw.Len()
				if n > 0 {
					t0 := time.Now()
					log.Printf("in-serve crawler: final HNSW persist at shutdown (%d nodes, full)", n)
					if err := s.hnsw.Persist(context.Background(), ps); err != nil {
						log.Printf("in-serve crawler: final HNSW persist failed: %v", err)
					} else {
						log.Printf("in-serve crawler: final HNSW persist complete in %s", time.Since(t0))
					}
				}
				return
			case <-t.C:
				n := s.hnsw.Len()
				if n == 0 || n == lastN {
					continue
				}
				// graph can shrink (e.g., /admin/hnsw-compact rewrites
				// indices and writes a smaller meta). When that happens, lastN
				// from before the compaction is stale and > n; PersistFrom(lastN)
				// would be a no-op forever, stranding any new AddPassages until
				// shutdown. The compact handler does its own full Persist so disk
				// is already in sync; we just need to resync lastN here.
				if n < lastN {
					lastN = n
					continue
				}
				t0 := time.Now()
				if err := s.hnsw.PersistFrom(context.Background(), ps, lastN); err != nil {
					log.Printf("in-serve crawler: HNSW persist (incremental from %d) failed: %v", lastN, err)
					continue
				}
				// alongside HNSW node writes, persist any newly-
				// encoded PQ codes for nodes [lastN, n). Skipped silently
				// when no codebook is loaded.
				pqWritten := 0
				if s.hnsw.HasPQ() {
					if w, err := s.hnsw.PersistPQCodesFrom(context.Background(), ps, lastN); err != nil {
						log.Printf("in-serve crawler: PQ codes persist failed: %v", err)
					} else {
						pqWritten = w
					}
				}
				if pqWritten > 0 {
					log.Printf("in-serve crawler: HNSW checkpoint at %d nodes (+%d incremental, +%d PQ codes, took %s)",
						n, n-lastN, pqWritten, time.Since(t0))
				} else {
					log.Printf("in-serve crawler: HNSW checkpoint at %d nodes (+%d incremental, took %s)",
						n, n-lastN, time.Since(t0))
				}
				lastN = n
			}
		}
	}()

	// Crawler goroutine — restarts automatically when new items appear in the
	// frontier after a drain. This ensures that site-submit / recrawl-sitemap
	// URLs submitted while the crawler is idle are picked up without a service
	// restart. Poll every 5s after drain; resume as soon as queued > 0.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			log.Printf("in-serve crawler: starting")
			if err := c.Run(ctx); err != nil {
				log.Printf("in-serve crawler: exited with error: %v", err)
				return
			}
			log.Printf("in-serve crawler: frontier drained, waiting for new work…")
			// Poll until the parent context is cancelled or new items appear.
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
				fs, err := s.store.GetFrontierStats(ctx)
				if err != nil {
					continue
				}
				if fs.Queued > 0 {
					log.Printf("in-serve crawler: %d new frontier items, restarting", fs.Queued)
					break
				}
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

// pebbleHTTP holds the read-side handles. Stays small on purpose — the
// SQLite-side Server struct accumulated a lot of config knobs over many iters;
// the Pebble surface starts minimal and grows feature-by-feature.
type pebbleHTTP struct {
	store       *store.PebbleStore
	idx         *index.PebbleBM25
	chat        embed.ChatClient // nil when cfg.Chat.Model is unset; /answer returns 501
	reranker    rerank.Reranker  // nil when no rerank is configured; ?rerank=true is a no-op then
	rerankCandK int              // candidates pulled for rerank; default 20

	// Bounded LLM concurrency + circuit breaker. chatGate is shared
	// between the answer and rerank pools so a burst on one side can't
	// also pin the goroutines of the other (the auto-sitemap-style leak
	// that took the box down). chatSafe wraps the answer-class client
	// (used for /answer, planner, HyDE, paraphrase) with KindAnswer;
	// rerankSafe wraps the per-passage scoring client with KindRerank.
	// Nil = uninitialized (tests / mocks).
	chatGate   *chatgate.Gate
	chatSafe   *chatgate.SafeChat
	rerankSafe *chatgate.SafeChat

	// TTL'd response cache + singleflight for /answer. A popular query
	// during a load spike costs one hybrid + rerank + LLM call; the
	// next 100 callers hit the cache in microseconds OR join the
	// in-flight call (sub-second). Disabled when ttl=0.
	answerCache *answercache.Cache

	// /research SSE stream cache. Captures and replays the full SSE byte
	// stream so repeat research queries skip retrieval + LLM entirely.
	researchCache *answercache.Cache

	// Query log: appends one JSON line per query-endpoint request so we have
	// real observability into what users ask, what returns empty, and latency.
	// nil file = disabled (COSIFT_QUERY_LOG unset). See querylog.go.
	qlogFile *os.File
	qlogMu   sync.Mutex

	// Feedback log: appends one JSON line per /feedback rating, correlated to a
	// query by qid. The real usefulness signal (penalize/reward). See feedback.go.
	fbFile *os.File
	fbMu   sync.Mutex
	// fbRL rate-limits /feedback per real client (XFF) — always on, stricter than
	// the global limiter, since feedback is public/unauthed and abusable.
	fbRL *rateLimiter

	// Per-site docID boost map cache. key = canonical site string (e.g.
	// "pilotprotocol.network"). Populated lazily on first site= query;
	// subsequent calls are sub-microsecond map lookups.
	siteBoostCache sync.Map // map[string]map[int64]float64

	// Per-site page-title cache for planner context. key = canonical site
	// string (same scheme as siteBoostCache). value = []string (<=10 titles).
	// Populated lazily on first site= research query.
	siteTitleCache sync.Map // map[string][]string

	// Polls vLLM /metrics. When num_requests_waiting exceeds
	// COSIFT_LLM_DEGRADE_QUEUE, optional LLM stages (rerank, HyDE,
	// paraphrase) are silently skipped — graceful degradation rather
	// than queue-then-timeout.
	llmProbe *chatgate.LoadProbe

	// Per-host authority Scorer; multiplies BM25 / hybrid scores by
	// (1 + alpha * Score(host)). Wikipedia / kernel.org / arxiv float
	// over the spam directories in the long tail of the crawl.
	authority *authority.Scorer

	// Per-endpoint latency + error-rate tracking against
	// operator-configured targets. Violations are appended to disk
	// (COSIFT_SLA_LOG_PATH) and surfaced on /sla.
	sla *sla.Monitor

	// hostBoosts is the operator-configured host-suffix →
	// multiplier map, applied to fused retrieval scores by /query.
	// Empty / nil = no boosts (fast path).
	hostBoosts map[string]float64

	// Nil = disabled.
	rl *rateLimiter

	// Empty cluster cfg = single-node, no-ops below.
	cluster config.Cluster
	// crawlSeed is set after startInProcessCrawl runs so /admin/crawl-enqueue
	// can hand off forwarded URLs into the in-process frontier. Nil when no
	// in-serve crawler is wired.
	crawlSeed func(url string) error
	// crawlSeedSitemap wraps Crawler.SeedSitemap so the /admin/
	// sitemap-import endpoint can push sitemap URLs into the live frontier.
	crawlSeedSitemap func(ctx context.Context, url string) (int, error)
	// crawlSeedSitemapLane is like crawlSeedSitemap but lets the caller pick
	// the frontier lane — used by /admin/site-submit to land a site's URLs
	// in the high-priority submitted lane.
	crawlSeedSitemapLane func(ctx context.Context, url string, lane byte) (int, error)
	crawlSeedRSS         func(ctx context.Context, url string) (int, error)
	crawlFetchNow        func(ctx context.Context, url string) error
	crawlSeedWET         func(ctx context.Context, url string, dedupeFresh, lexicalOnly bool) (int, error)
	// crawlAllowDomain promotes a domain into the crawler's runtime dynamic
	// allowlist (used by /admin/allow-domain for organic HN/Reddit growth).
	crawlAllowDomain func(domain string) error
	crawlRecrawl     func(ctx context.Context, url string) error

	// doc count at startup so /stats can report crawl rate
	// without persistent counter tables. docs_added = current - startup,
	// rate = docs_added / uptime.
	startupDocs int
	// crawlActive = true when in-serve crawler is running.
	crawlActive bool

	// /research?expand=true issues a
	// chat call PER sub-query, so a sticky workload (repeated queries, slow
	// sub-query rephrasings from the planner) hits the chat provider many
	// times for the same passage. Cap at 256 entries; on overflow drop one
	// arbitrary entry (Go map iteration order is randomized, good enough).
	hydeMu       sync.RWMutex
	hydeCache    map[string]string
	hydeCacheCap int // env-configurable via COSIFT_HYDE_CACHE_SIZE

	// PQStatus's lock contended
	// hard with the crawler's AddPassage writers — observed /stats
	// latency varied 0.2 s to 5.5 s. 5 s TTL keeps the cache effective
	// for the landing page (polls every ~30 s) without serving very
	// stale numbers.
	//
	// After TTL, return stale body
	// immediately and refresh async; only the first-ever call pays the
	// cold-cache cost. statsRefreshing is a single-flight guard so we
	// don't fan out N background refreshes when a burst arrives stale.
	statsBodyMu     sync.Mutex
	statsBodyBlob   []byte
	statsBodyAt     time.Time
	statsRefreshing atomic.Bool

	// /research?expand=paraphrase fans
	// out 3 paraphrases × N sub-queries — same hot path as HyDE but each
	// miss is 3x larger by output volume. Keyed on q only (fixed n=3 today).
	paraMu       sync.RWMutex
	paraCache    map[string][]string
	paraCacheCap int // env-configurable via COSIFT_PARA_CACHE_SIZE
	paraHits     atomic.Int64
	paraMisses   atomic.Int64

	// atomic counters surfaced on /metrics so operators can size the
	// HyDE cache against a real workload — if misses dominate, raise the cap
	// or move to an L2 store; if hits dominate at a tight working set, the
	// cap is sufficient and you can mostly forget about it.
	hydeHits   atomic.Int64
	hydeMisses atomic.Int64

	// snapshot at startup of whether persisted HNSW vectors exist
	// in the 'v' family. Cheap peek (first-hit short-circuit). Surfaced on
	// /stats so operators know whether the store is ready for the future
	// ?retriever=dense path without needing to grep pebble-info.
	hasVectors bool
	// when an HNSW meta blob is persisted (the normal case),
	// surface dim + node count too. Cheap 20-byte read at startup.
	vectorDim   int
	vectorNodes int
	// Loaded
	// only when COSIFT_LOAD_HNSW=true at startup (gigabytes RAM at scale).
	// Nil = graph not loaded; /search?retriever=dense returns a warning.
	hnsw *index.HNSW
	// Required by
	// /search?retriever=dense alongside s.hnsw. Built at startup when
	// cfg.Embeddings.Model is set. Nil → ?retriever=dense warns + falls back.
	embedder embed.Embedder

	// Rerank failures fall back
	// to BM25 order silently — that's the right reliability move, but without
	// a counter operators can't tell whether their LLM/HTTP reranker is
	// healthy or quietly broken.
	rerankAttempts atomic.Int64
	rerankFailures atomic.Int64

	// chat call attempt + failure counters across HyDE expansion,
	// /answer synth (sync + SSE), and /research plan + synth (sync + SSE).
	// A spike in failures (provider 429s, network blips) is the clearest
	// early signal that synth endpoints are degraded.
	chatAttempts atomic.Int64
	chatFailures atomic.Int64

	// count responses that carried at least one warning.
	// Operators alerting on this catch 'a deploy started sending malformed
	// requests' without having to parse response bodies.
	warningsEmitted atomic.Int64

	// /metrics divides this by
	// chatAttempts to give mean chat latency, separated from the
	// per-endpoint duration. Diagnoses 'where did the seconds go' on a
	// slow /research stream: chat-side or retrieval-side?
	chatDurationNanos atomic.Int64

	// per-endpoint request counters + duration sums via a
	// counting middleware wrapping every mux entry. sync.Map keeps the hot
	// path lock-free; /metrics reads via Range. Path is the label so a
	// misrouted call (404) doesn't get counted under an existing endpoint.
	// rate(sum)/rate(count) over the duration sum gives mean latency in PromQL.
	requestCounts sync.Map // map[string]*endpointMetrics

	started time.Time
}

type endpointMetrics struct {
	count    atomic.Int64
	sumNanos atomic.Int64
}

// count is the request-counting middleware. Bumps a per-path
// {count, sumNanos} struct, lazily created on first request to a path. Hot
// path is sync.Map.Load + two atomic Adds — no contention even under high
// RPS. Duration is sampled after the handler returns so streaming endpoints
// account for their full open connection time.
// envIntDefault reads name as an int; falls back to def when unset or
// unparseable. Used by gate / deadline knobs so operators can tune via
// env without a config-file roundtrip.
func envIntDefault(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// envDurationMsDefault reads name as a millisecond integer; returns def
// on missing/invalid. Chose ms over Go's "2s" parse to keep operator
// muscle memory consistent with the other COSIFT_*_MS knobs.
func envDurationMsDefault(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return def
}

// rateLimiter is a per-IP token-bucket limiter. Active when
// COSIFT_RATELIMIT_RPM > 0; nil = disabled. Whitelisted IPs bypass entirely.
type rateLimiter struct {
	rpm       float64
	burst     float64
	whitelist map[string]bool
	buckets   sync.Map // string IP → *rateLimitBucket
}

type rateLimitBucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// newRateLimiterFromEnv reads COSIFT_RATELIMIT_RPM / _BURST / _WHITELIST.
// Returns nil when RPM is unset or non-positive — limiting is off by default
// so the no-config self-host story stays simple.
func newRateLimiterFromEnv() *rateLimiter {
	rpmStr := os.Getenv("COSIFT_RATELIMIT_RPM")
	if rpmStr == "" {
		return nil
	}
	rpm, err := strconv.ParseFloat(rpmStr, 64)
	if err != nil || rpm <= 0 {
		return nil
	}
	burst := 10.0
	if v := os.Getenv("COSIFT_RATELIMIT_BURST"); v != "" {
		if b, err := strconv.ParseFloat(v, 64); err == nil && b > 0 {
			burst = b
		}
	}
	wl := map[string]bool{}
	for _, ip := range strings.Split(os.Getenv("COSIFT_RATELIMIT_WHITELIST"), ",") {
		ip = strings.TrimSpace(ip)
		if ip != "" {
			wl[ip] = true
		}
	}
	return &rateLimiter{rpm: rpm, burst: burst, whitelist: wl}
}

// whitelistList returns the whitelisted IPs as a slice (for logging only).
func (rl *rateLimiter) whitelistList() []string {
	if rl == nil {
		return nil
	}
	out := make([]string, 0, len(rl.whitelist))
	for ip := range rl.whitelist {
		out = append(out, ip)
	}
	sort.Strings(out)
	return out
}

// allow returns whether the request from ip may proceed. Side-effects: drains
// one token from the IP's bucket on success.
func (rl *rateLimiter) allow(ip string) bool {
	if rl == nil {
		return true
	}
	if rl.whitelist[ip] {
		return true
	}
	bv, _ := rl.buckets.LoadOrStore(ip, &rateLimitBucket{tokens: rl.burst, last: time.Now()})
	b := bv.(*rateLimitBucket)
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * rl.rpm / 60.0
	if b.tokens > rl.burst {
		b.tokens = rl.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// stripPort drops ":port" from a "host:port" or "[v6]:port" RemoteAddr. Falls
// back to the input on parse failure (so we still get SOME per-client key).
func stripPort(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

// rateLimit is the HTTP middleware that gates each request through the per-IP
// limiter. Returns 429 with a JSON problem doc + Retry-After hint when the
// bucket is empty. No-op when s.rl is nil.
//
// requireAdmin gates a handler on the peer auth token (cfg.Cluster.PeerAuthToken,
// sent as "Authorization: Bearer <token>"). When the token is empty — the
// single-node default — the check is skipped and any caller is accepted. Applied
// at the mux level via awrap to every /admin/* route so no admin handler can ship
// without auth, even if it forgets the per-handler check.
func (s *pebbleHTTP) requireAdmin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if want := s.cluster.PeerAuthToken; want != "" {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if got != want {
				writeProblem(w, http.StatusUnauthorized, "missing or invalid peer token")
				return
			}
		}
		h(w, r)
	}
}

// X-Forwarded-For is honored ONLY when the request came from a configured
// trusted proxy; otherwise clients could spoof their IP by setting the header.
// For self-host with cosift directly on the public network, leave
// cfg.Server.TrustedProxies empty (default) and the RemoteAddr is used.
func (s *pebbleHTTP) rateLimit(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := stripPort(r.RemoteAddr)
		if !s.rl.allow(ip) {
			w.Header().Set("Retry-After", "60")
			writeProblem(w, http.StatusTooManyRequests, "rate limit exceeded for ip="+ip)
			return
		}
		h(w, r)
	}
}

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
		// Wrap the writer so the SLA monitor knows whether the call
		// succeeded; default status to 200 because handlers that just
		// w.Write() never call WriteHeader explicitly.
		sw := &statusCapturingWriter{ResponseWriter: w, status: 200}
		h(sw, r)
		dur := time.Since(start)
		m.sumNanos.Add(dur.Nanoseconds())
		if s.sla != nil {
			s.sla.Observe(key, dur, sw.status < 500)
		}
	}
}

// statusCapturingWriter lets the count middleware record the response
// status for the SLA observer. The middleware treats 5xx as failures
// for SLA purposes; 4xx are client errors and don't count against us.
type statusCapturingWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	bytes       int64 // response body bytes — used by the query logger as an empty-result proxy
}

func (s *statusCapturingWriter) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusCapturingWriter) Write(p []byte) (int, error) {
	if !s.wroteHeader {
		s.wroteHeader = true
	}
	n, err := s.ResponseWriter.Write(p)
	s.bytes += int64(n)
	return n, err
}

// Flush passes through to support SSE / streaming handlers.
func (s *statusCapturingWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

//go:embed assets/landing.html
var landingHTML []byte

//go:embed assets/chat.html
var chatHTML []byte

//go:embed assets/openapi.json
var openapiJSON []byte

//go:embed assets/swagger/index.html
var swaggerIndexHTML []byte

//go:embed assets/swagger/swagger-ui.css
var swaggerUICSS []byte

//go:embed assets/swagger/swagger-ui-bundle.js
var swaggerUIJS []byte

// handleLanding serves the self-host dashboard. Only matches exact
// "/" — Go 1.22's ServeMux routes sub-paths to the longest match, so this
// won't accidentally swallow /search etc.
func (s *pebbleHTTP) handleLanding(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.Write(landingHTML)
}

// handleChat serves the multi-turn chat UI. JS calls /answer
// or /research with stream=true and renders SSE events incrementally.
func (s *pebbleHTTP) handleChat(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60")
	_, _ = w.Write(chatHTML)
}

func (s *pebbleHTTP) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.Write(openapiJSON)
}

func (s *pebbleHTTP) handleSwaggerUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	w.Write(swaggerIndexHTML)
}

func (s *pebbleHTTP) handleSwaggerAsset(w http.ResponseWriter, r *http.Request) {
	file := r.PathValue("file")
	switch file {
	case "swagger-ui.css":
		w.Header().Set("Content-Type", "text/css")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(swaggerUICSS)
	case "swagger-ui-bundle.js":
		w.Header().Set("Content-Type", "application/javascript")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.Write(swaggerUIJS)
	default:
		http.NotFound(w, r)
	}
}
