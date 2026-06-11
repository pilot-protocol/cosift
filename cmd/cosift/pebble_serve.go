// minimal HTTP server backed by PebbleStore + PebbleBM25.
//
// Parallel to the SQLite-backed `cosift serve`. Read-only endpoints only:
// /healthz, /stats, /search, /contents. No crawler, no admin, no /answer
// or /research yet — those need the SQLite-side server's chat-client +
// admin-token plumbing, which is the work.
//
// Purpose: proves the path-2 storage rework works end-to-end through HTTP,
// and gives a clean benchmark surface against the existing SQLite server.
// Operators evaluating cosift's billion-scale capability run this against
// a Pebble store and compare /search latency + index size to the SQLite
// equivalent.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crypto/sha256"
	"encoding/hex"

	"github.com/pilot-protocol/cosift/internal/crawler"

	"github.com/pilot-protocol/cosift/internal/answercache"
	"github.com/pilot-protocol/cosift/internal/authority"
	"github.com/pilot-protocol/cosift/internal/chatgate"
	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/judge"
	"github.com/pilot-protocol/cosift/internal/qexpand"
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
	// Read actual doc count at startup for crawl-rate baseline.
	if st, _ := ps.Stats(ctx); st.Documents > 0 {
		srv.startupDocs = int(st.Documents)
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
	mux.HandleFunc("GET /stats", wrap(srv.handleStats))
	mux.HandleFunc("GET /domains", wrap(srv.handleDomains))
	// frontier queue visibility — counts by status + top-N
	// hosts in queue. Distinct from /domains which is over INDEXED docs.
	mux.HandleFunc("GET /queue", wrap(srv.handleQueue))
	// Single-node deployments still expose
	// this; it's just unused. Authenticated by cfg.Cluster.PeerAuthToken
	// (Bearer); when token is empty, requests from any source are accepted.
	mux.HandleFunc("POST /admin/crawl-enqueue", wrap(srv.handleCrawlEnqueue))
	mux.HandleFunc("POST /admin/frontier-purge-host", wrap(srv.handleFrontierPurgeHost))
	mux.HandleFunc("POST /admin/frontier-clear", wrap(srv.handleFrontierClear))
	mux.HandleFunc("POST /admin/rss-import", wrap(srv.handleRSSImport))
	mux.HandleFunc("POST /admin/crawl-now", wrap(srv.handleCrawlNow))
	mux.HandleFunc("POST /admin/wet-import", wrap(srv.handleWETImport))
	mux.HandleFunc("POST /admin/wet-import-bulk", wrap(srv.handleWETImportBulk))
	mux.HandleFunc("POST /admin/site-pack", wrap(srv.handleSitePack))
	mux.HandleFunc("POST /admin/embed-backfill", wrap(srv.handleEmbedBackfill))
	mux.HandleFunc("GET /admin/eval-quick", wrap(srv.handleEvalQuick))
	mux.HandleFunc("POST /admin/hnsw-compact", wrap(srv.handleHNSWCompact))
	mux.HandleFunc("GET /query", wrap(srv.handleQuery))
	mux.HandleFunc("POST /query", wrap(srv.handleQuery))
	// import a sitemap.xml (or sitemap-index) and push every
	// listed URL into the live frontier.
	mux.HandleFunc("POST /admin/sitemap-import", wrap(srv.handleSitemapImport))
	// PQ training admin endpoint. Same auth as crawl-enqueue
	// (Bearer cfg.Cluster.PeerAuthToken). Runs synchronously — for the
	// 224K-vec corpus we have today it takes ~minutes; operator-only.
	mux.HandleFunc("POST /admin/pq-train", wrap(srv.handlePQTrain))
	// backfill-only — re-encode every node that doesn't have a
	// code yet against the existing codebook. No retrain. Fast.
	mux.HandleFunc("POST /admin/pq-encode", wrap(srv.handlePQEncode))
	// Creates a hard-linked, consistent
	// snapshot dir that's safe to tar without racing background compactions.
	mux.HandleFunc("POST /admin/checkpoint", wrap(srv.handleCheckpoint))
	mux.HandleFunc("GET /search", wrap(srv.handleSearch))
	mux.HandleFunc("POST /search", wrap(srv.handleSearchPOST))
	mux.HandleFunc("GET /contents", wrap(srv.handleContents))
	mux.HandleFunc("POST /contents", wrap(srv.handleContentsBatch))
	mux.HandleFunc("GET /verify", wrap(srv.handleVerify))
	mux.HandleFunc("GET /metrics", wrap(srv.handleMetrics))
	mux.HandleFunc("GET /sla", wrap(srv.handleSLA))
	mux.HandleFunc("GET /admin/domains-audit", wrap(srv.handleDomainsAudit))
	mux.HandleFunc("GET /find_similar", wrap(srv.handleFindSimilar))
	mux.HandleFunc("POST /find_similar", wrap(srv.handleFindSimilarPOST))
	mux.HandleFunc("GET /answer", wrap(srv.handleAnswer))
	mux.HandleFunc("POST /answer", wrap(srv.handleAnswerPOST))
	mux.HandleFunc("GET /research", wrap(srv.handleResearch))
	mux.HandleFunc("POST /research", wrap(srv.handleResearchPOST))

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
	crawlEmbedCap := 8
	if v := os.Getenv("COSIFT_CRAWL_EMBED_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			crawlEmbedCap = n
		}
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
	s.crawlSeedRSS = c.SeedRSS
	s.crawlFetchNow = c.FetchAndIndexNow
	s.crawlSeedWET = c.SeedWET
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

	// Crawler goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("in-serve crawler: starting")
		if err := c.Run(ctx); err != nil {
			log.Printf("in-serve crawler: exited with error: %v", err)
			return
		}
		log.Printf("in-serve crawler: frontier drained")
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
	crawlSeedRSS     func(ctx context.Context, url string) (int, error)
	crawlFetchNow    func(ctx context.Context, url string) error
	crawlSeedWET     func(ctx context.Context, url string, dedupeFresh, lexicalOnly bool) (int, error)

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
	return s.ResponseWriter.Write(p)
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

func (s *pebbleHTTP) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

// forwardURLToPeer POSTs a URL to peer's /admin/crawl-enqueue. Best-effort:
// caller logs failures and the URL is dropped — no retry or persistent queue.
var forwardHTTP = &http.Client{Timeout: 10 * time.Second}

func (s *pebbleHTTP) forwardURLToPeer(ctx context.Context, rawURL, peerAddr string) error {
	body, _ := json.Marshal(crawlEnqueueReq{URL: rawURL})
	// peerAddr is host:port; assume http inside the cluster (mTLS / VPN
	// would be a wrapper concern). Switch to https://... if peers expose TLS.
	endpoint := "http://" + peerAddr + "/admin/crawl-enqueue"
	if strings.HasPrefix(peerAddr, "http://") || strings.HasPrefix(peerAddr, "https://") {
		endpoint = strings.TrimRight(peerAddr, "/") + "/admin/crawl-enqueue"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if t := s.cluster.PeerAuthToken; t != "" {
		req.Header.Set("Authorization", "Bearer "+t)
	}
	resp, err := forwardHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("peer %s returned %d: %s", peerAddr, resp.StatusCode, b)
	}
	return nil
}

// scatterSearch fans /search out to every peer with the given params,
// collects per-peer hit lists, RRF-merges them, and returns the merged
// top-k. Used by /search, /answer, /research gateway modes.
// includeText forces text inlining on the scatter so callers that need
// passages (synth endpoints) get them in one round trip.
func (s *pebbleHTTP) scatterSearch(ctx context.Context, q string, k int, perPeerK int, params url.Values, includeText bool) (hits []searchHit, warns []string, totalCandidates int) {
	srcQ := url.Values{}
	for kk, vv := range params {
		srcQ[kk] = vv
	}
	srcQ.Set("q", q)
	srcQ.Set("k", strconv.Itoa(perPeerK))
	srcQ.Set("cluster_local", "1")
	if includeText {
		srcQ.Set("include_text", "true")
	}
	queryStr := srcQ.Encode()

	type peerResult struct {
		peerIdx int
		hits    []searchHit
		err     error
	}
	resCh := make(chan peerResult, len(s.cluster.Peers))
	httpClient := &http.Client{Timeout: 15 * time.Second}
	for i, peer := range s.cluster.Peers {
		if peer == "" {
			continue
		}
		i, peer := i, peer
		go func() {
			endpoint := "http://" + peer + "/search?" + queryStr
			if strings.HasPrefix(peer, "http://") || strings.HasPrefix(peer, "https://") {
				endpoint = strings.TrimRight(peer, "/") + "/search?" + queryStr
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
			if t := s.cluster.PeerAuthToken; t != "" {
				req.Header.Set("Authorization", "Bearer "+t)
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				resCh <- peerResult{peerIdx: i, err: err}
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode >= 300 {
				resCh <- peerResult{peerIdx: i, err: fmt.Errorf("peer %s returned %d", peer, resp.StatusCode)}
				return
			}
			var sr struct {
				Hits []searchHit `json:"hits"`
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
			if err := json.Unmarshal(body, &sr); err != nil {
				resCh <- peerResult{peerIdx: i, err: err}
				return
			}
			resCh <- peerResult{peerIdx: i, hits: sr.Hits}
		}()
	}
	lists := [][]index.Hit{}
	hitFull := map[string]searchHit{}
gather:
	for i := 0; i < len(s.cluster.Peers); i++ {
		if s.cluster.Peers[i] == "" {
			continue
		}
		select {
		case res := <-resCh:
			if res.err != nil {
				warns = append(warns, fmt.Sprintf("peer %d (%s) failed: %v", res.peerIdx, s.cluster.Peers[res.peerIdx], res.err))
				continue
			}
			totalCandidates += len(res.hits)
			peerList := make([]index.Hit, len(res.hits))
			for j, h := range res.hits {
				peerList[j] = index.Hit{URL: h.URL, Title: h.Title, Score: h.Score}
				if _, ok := hitFull[h.URL]; !ok {
					hitFull[h.URL] = h
				}
			}
			lists = append(lists, peerList)
		case <-ctx.Done():
			warns = append(warns, "client context cancelled before all peers responded")
			break gather
		}
	}
	fused := rrfFuse(lists)
	if len(fused) > k {
		fused = fused[:k]
	}
	hits = make([]searchHit, 0, len(fused))
	for _, f := range fused {
		full, ok := hitFull[f.URL]
		if !ok {
			full = searchHit{URL: f.URL, Title: f.Title}
		}
		full.Score = f.Score
		hits = append(hits, full)
	}
	return hits, warns, totalCandidates
}

// handleSearchGateway is the scatter-gather entry. It fans out the
// search to every peer's /search (including its own shard via the peers[]
// table), each peer over-fetches k*2 candidates locally, gateway RRF-merges
// the per-peer lists, and returns the top-k. Slow / failing peers are
// included in a warnings[] entry but don't block the response.
func (s *pebbleHTTP) handleSearchGateway(w http.ResponseWriter, r *http.Request) {
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
	perPeerK := k * 2
	if perPeerK < 20 {
		perPeerK = 20
	}
	// delegated to shared scatterSearch helper.
	hits, warns, total := s.scatterSearch(r.Context(), q, k, perPeerK, r.URL.Query(), r.URL.Query().Get("include_text") == "true")
	resp := searchResponse{
		Query:           q,
		Retriever:       fmt.Sprintf("gateway:rrf(%d-shard)", numNonEmpty(s.cluster.Peers)-len(warns)),
		Hits:            hits,
		TotalCandidates: total,
		Warnings:        append(warns, s.warningsFor(r)...),
		Took:            time.Since(start).String(),
	}
	writeJSON(w, http.StatusOK, resp)
}

// numNonEmpty counts non-empty entries in a peer list (peers[i]="" means
// "skip", typically MyShardID).
func numNonEmpty(ps []string) int {
	n := 0
	for _, p := range ps {
		if p != "" {
			n++
		}
	}
	return n
}

// handleFindSimilarGateway scatters /find_similar to every shard. The
// owning shard (URL-mode) or every shard (text-mode) produces real
// neighbors; others return empty. Gateway RRF-merges.
func (s *pebbleHTTP) handleFindSimilarGateway(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	k := 10
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			k = n
		}
	}
	perPeerK := k * 2
	if perPeerK < 20 {
		perPeerK = 20
	}
	srcQ := url.Values{}
	for kk, vv := range r.URL.Query() {
		srcQ[kk] = vv
	}
	srcQ.Set("k", strconv.Itoa(perPeerK))
	srcQ.Set("cluster_local", "1")
	queryStr := srcQ.Encode()

	type peerResult struct {
		peerIdx int
		hits    []searchHit
		err     error
	}
	resCh := make(chan peerResult, len(s.cluster.Peers))
	httpClient := &http.Client{Timeout: 15 * time.Second}
	for i, peer := range s.cluster.Peers {
		if peer == "" {
			continue
		}
		i, peer := i, peer
		go func() {
			endpoint := "http://" + peer + "/find_similar?" + queryStr
			if strings.HasPrefix(peer, "http://") || strings.HasPrefix(peer, "https://") {
				endpoint = strings.TrimRight(peer, "/") + "/find_similar?" + queryStr
			}
			req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint, http.NoBody)
			if t := s.cluster.PeerAuthToken; t != "" {
				req.Header.Set("Authorization", "Bearer "+t)
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				resCh <- peerResult{peerIdx: i, err: err}
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode >= 300 {
				resCh <- peerResult{peerIdx: i, err: fmt.Errorf("peer %s returned %d", peer, resp.StatusCode)}
				return
			}
			var sr struct {
				Hits []searchHit `json:"hits"`
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
			if err := json.Unmarshal(body, &sr); err != nil {
				resCh <- peerResult{peerIdx: i, err: err}
				return
			}
			resCh <- peerResult{peerIdx: i, hits: sr.Hits}
		}()
	}
	lists := [][]index.Hit{}
	hitFull := map[string]searchHit{}
	warns := []string{}
	total := 0
gather:
	for i := 0; i < len(s.cluster.Peers); i++ {
		if s.cluster.Peers[i] == "" {
			continue
		}
		select {
		case res := <-resCh:
			if res.err != nil {
				warns = append(warns, fmt.Sprintf("peer %d (%s) failed: %v", res.peerIdx, s.cluster.Peers[res.peerIdx], res.err))
				continue
			}
			total += len(res.hits)
			peerList := make([]index.Hit, len(res.hits))
			for j, h := range res.hits {
				peerList[j] = index.Hit{URL: h.URL, Title: h.Title, Score: h.Score}
				if _, ok := hitFull[h.URL]; !ok {
					hitFull[h.URL] = h
				}
			}
			lists = append(lists, peerList)
		case <-r.Context().Done():
			warns = append(warns, "client context cancelled")
			break gather
		}
	}
	fused := rrfFuse(lists)
	if len(fused) > k {
		fused = fused[:k]
	}
	out := make([]searchHit, 0, len(fused))
	for _, f := range fused {
		full, ok := hitFull[f.URL]
		if !ok {
			full = searchHit{URL: f.URL, Title: f.Title}
		}
		full.Score = f.Score
		out = append(out, full)
	}
	writeJSON(w, http.StatusOK, searchResponse{
		Query:           r.URL.Query().Get("url"),
		Retriever:       fmt.Sprintf("gateway:find_similar:rrf(%d-shard)", numNonEmpty(s.cluster.Peers)-len(warns)),
		Hits:            out,
		TotalCandidates: total,
		Warnings:        append(warns, s.warningsFor(r)...),
		Took:            time.Since(start).String(),
	})
}

// handleResearchGateway runs the planner on the gateway, scatters
// each sub-query to peers (collecting RRF-merged hits with text), then
// runs a single research synth.
func (s *pebbleHTTP) handleResearchGateway(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented, "/research requires cfg.Chat.Model on the gateway")
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
	// 1. Plan via chat.
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
	// 2. Scatter per sub-query. Dedup by URL, keep best fused score.
	type ranked struct {
		score float64
		hit   searchHit
	}
	best := make(map[string]ranked, k*len(subs))
	allWarns := []string{}
	perSub := k * 2
	for _, sq := range subs {
		hits, warns, _ := s.scatterSearch(r.Context(), sq, perSub, perSub, r.URL.Query(), true)
		allWarns = append(allWarns, warns...)
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
	if len(pooled) > k {
		pooled = pooled[:k]
	}
	// 3. Build prompt.
	sources := make([]answerSource, 0, len(pooled))
	var promptSources strings.Builder
	for i, p := range pooled {
		src := answerSource{ID: i + 1, URL: p.hit.URL, Title: p.hit.Title, Excerpt: p.hit.Excerpt, Author: p.hit.Author, PublishedAt: p.hit.PublishedAt}
		text := p.hit.Text
		if text == "" {
			text = p.hit.Excerpt
		}
		sources = append(sources, src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, p.hit.Title, p.hit.URL, text)
	}
	if len(sources) == 0 {
		writeJSON(w, http.StatusOK, researchResponse{
			Query: q, Plan: subs, Answer: "No matching sources across the cluster.",
			Sources: sources, Model: s.chat.Model(),
			Warnings: allWarns, Took: time.Since(start).String(),
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
		Query: q, Plan: subs, Answer: answer, Sources: sources,
		Model:     s.chat.Model(),
		Retriever: fmt.Sprintf("gateway:rrf(%d-shard,%d-sub)", numNonEmpty(s.cluster.Peers), len(subs)),
		Warnings:  allWarns, Took: time.Since(start).String(),
	})
}

// handleAnswerGateway scatters /search across peers with include_text, runs
// a SINGLE chat synth on the gateway.
func (s *pebbleHTTP) handleAnswerGateway(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented, "/answer requires cfg.Chat.Model on the gateway")
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
	hits, warns, total := s.scatterSearch(r.Context(), q, k, k*2, r.URL.Query(), true)
	if len(hits) == 0 {
		writeJSON(w, http.StatusOK, answerResponse{
			Query: q, Answer: "No matching sources across the cluster.",
			Sources: []answerSource{}, Model: s.chat.Model(),
			TotalCandidates: total, Warnings: warns, Took: time.Since(start).String(),
		})
		return
	}
	sources := make([]answerSource, 0, len(hits))
	var promptSources strings.Builder
	for i, h := range hits {
		src := answerSource{ID: i + 1, URL: h.URL, Title: h.Title, Excerpt: h.Excerpt, Author: h.Author, PublishedAt: h.PublishedAt}
		text := h.Text
		if text == "" {
			text = h.Excerpt
		}
		sources = append(sources, src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, h.Title, h.URL, text)
	}
	answer, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
		{Role: "system", Content: answerSystemPrompt},
		{Role: "user", Content: "Sources:\n\n" + promptSources.String() + "Question: " + q},
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "synth: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, answerResponse{
		Query: q, Answer: answer, Sources: sources, Model: s.chat.Model(),
		Retriever:       fmt.Sprintf("gateway:rrf(%d-shard)", numNonEmpty(s.cluster.Peers)-len(warns)),
		TotalCandidates: total, Warnings: warns, Took: time.Since(start).String(),
	})
}

// handlePQEncode backfills PQ codes for every node currently missing one,
// against the codebook already loaded on this serve. Order-of-magnitude
// faster than handlePQTrain because it skips k-means; just encode loop.
func (s *pebbleHTTP) handlePQEncode(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.hnsw == nil {
		writeProblem(w, http.StatusBadRequest, "pq-encode: no HNSW loaded")
		return
	}
	if !s.hnsw.HasPQ() {
		writeProblem(w, http.StatusBadRequest, "pq-encode: no codebook loaded — run /admin/pq-train first")
		return
	}
	t0 := time.Now()
	ids, codes, err := s.hnsw.EncodeMissing()
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "encode: "+err.Error())
		return
	}
	encodeElapsed := time.Since(t0)
	if len(ids) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{
			"encoded":       0,
			"total":         s.hnsw.Len(),
			"already_full":  true,
			"total_elapsed": encodeElapsed.String(),
		})
		return
	}
	// Persist freshly-encoded codes in a single batch.
	pq := s.hnsw.PQStatus()
	cb := &index.PQCodebook{Dim: pq.Dim, M: pq.M, K: pq.K, SubDim: pq.Dim / pq.M}
	entries := make([]store.PQCodeEntry, len(ids))
	for i := range ids {
		entries[i] = store.PQCodeEntry{ID: ids[i], Blob: cb.EncodeCodeBlob(codes[i])}
	}
	persistT0 := time.Now()
	if err := s.store.PutPQCodesBatch(r.Context(), entries); err != nil {
		writeProblem(w, http.StatusInternalServerError, "persist codes: "+err.Error())
		return
	}
	persistElapsed := time.Since(persistT0)
	total := s.hnsw.Len()
	coverage := 100.0 * float64(pq.NodesWithCode+len(ids)) / float64(total)
	totalElapsed := time.Since(t0)
	log.Printf("pq-encode: backfilled %d codes in %s (persist %s, total %s) → coverage %.1f%%",
		len(ids), encodeElapsed-persistElapsed, persistElapsed, totalElapsed, coverage)
	writeJSON(w, http.StatusOK, map[string]any{
		"encoded":         len(ids),
		"total":           total,
		"coverage_pct":    coverage,
		"encode_elapsed":  (encodeElapsed - persistElapsed).String(),
		"persist_elapsed": persistElapsed.String(),
		"total_elapsed":   totalElapsed.String(),
	})
}

// handleCheckpoint creates a Pebble checkpoint (hard-linked, point-in-time)
// at a server-chosen path under COSIFT_CHECKPOINT_DIR (default /tmp). The
// returned path is safe to tar — Pebble's compactor cannot mutate hard-linked
// SSTs. Caller is responsible for deleting the dir after consuming it.
func (s *pebbleHTTP) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	base := os.Getenv("COSIFT_CHECKPOINT_DIR")
	if base == "" {
		base = "/tmp"
	}
	dest := filepath.Join(base, fmt.Sprintf("cosift-ckpt-%d", time.Now().UnixNano()))
	t0 := time.Now()
	if err := s.store.Checkpoint(dest); err != nil {
		writeProblem(w, http.StatusInternalServerError, "checkpoint: "+err.Error())
		return
	}
	elapsed := time.Since(t0)
	log.Printf("checkpoint: created %q in %s", dest, elapsed)
	writeJSON(w, http.StatusOK, map[string]any{
		"path":    dest,
		"elapsed": elapsed.String(),
	})
}

// handlePQTrain trains a PQ codebook on a sample of the current HNSW
// graph, then encodes ALL nodes and persists codes + codebook to Pebble.
// Synchronous; returns when training completes.
type pqTrainReq struct {
	SampleSize int `json:"sample_size"` // default 50000
	M          int `json:"m"`           // default dim/8 (subspaces of size 8)
	K          int `json:"k"`           // default 256
	Iters      int `json:"iters"`       // default 15
	Parallel   int `json:"parallel"`    // default GOMAXPROCS
}

func (s *pebbleHTTP) handlePQTrain(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.hnsw == nil || s.hnsw.Len() == 0 {
		writeProblem(w, http.StatusBadRequest, "pq-train requires an in-memory HNSW with nodes")
		return
	}
	var req pqTrainReq
	if r.ContentLength > 0 {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 4<<10))
		_ = json.Unmarshal(body, &req)
	}
	if req.SampleSize <= 0 {
		req.SampleSize = 50000
	}
	if req.K <= 0 {
		req.K = 256
	}
	if req.Iters <= 0 {
		req.Iters = 15
	}
	if req.Parallel <= 0 {
		req.Parallel = 8
	}
	if s.embedder == nil {
		writeProblem(w, http.StatusBadRequest, "pq-train needs an embedder to know the vector dim")
		return
	}
	dim := s.embedder.Dim()
	M := req.M
	if M <= 0 {
		// Default: 1 subspace per 8 dims. For 768d → 96 subspaces.
		M = dim / 8
		if M <= 0 {
			M = 1
		}
	}
	if dim%M != 0 {
		writeProblem(w, http.StatusBadRequest, fmt.Sprintf("dim %d not divisible by M %d", dim, M))
		return
	}
	t0 := time.Now()
	log.Printf("pq-train: sampling %d / %d vectors (dim=%d, M=%d, K=%d, iters=%d, parallel=%d)",
		req.SampleSize, s.hnsw.Len(), dim, M, req.K, req.Iters, req.Parallel)
	sample := s.hnsw.SampleVectors(req.SampleSize, time.Now().UnixNano())
	sampledN := len(sample)
	log.Printf("pq-train: training codebook on %d samples...", sampledN)
	cb, err := index.TrainPQCodebookParallel(sample, dim, M, req.K, req.Iters, req.Parallel, time.Now().UnixNano())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "train: "+err.Error())
		return
	}
	trainElapsed := time.Since(t0)
	log.Printf("pq-train: codebook trained in %s; persisting...", trainElapsed)
	if err := cb.Persist(r.Context(), s.store); err != nil {
		writeProblem(w, http.StatusInternalServerError, "persist codebook: "+err.Error())
		return
	}
	encodeT0 := time.Now()
	log.Printf("pq-train: encoding %d nodes...", s.hnsw.Len())
	ids, codes, err := s.hnsw.EncodeAll(cb)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "encode: "+err.Error())
		return
	}
	entries := make([]store.PQCodeEntry, len(ids))
	for i := range ids {
		// K≤256 → byte-packed for 2x disk savings.
		entries[i] = store.PQCodeEntry{ID: ids[i], Blob: cb.EncodeCodeBlob(codes[i])}
	}
	if err := s.store.PutPQCodesBatch(r.Context(), entries); err != nil {
		writeProblem(w, http.StatusInternalServerError, "put codes: "+err.Error())
		return
	}
	encodeElapsed := time.Since(encodeT0)
	totalElapsed := time.Since(t0)
	log.Printf("pq-train: done in %s (train=%s, encode+persist=%s, %d codes written)",
		totalElapsed, trainElapsed, encodeElapsed, len(ids))
	writeJSON(w, http.StatusOK, map[string]any{
		"sample_size":    sampledN,
		"dim":            dim,
		"M":              M,
		"K":              req.K,
		"iters":          req.Iters,
		"nodes_encoded":  len(ids),
		"train_elapsed":  trainElapsed.String(),
		"encode_elapsed": encodeElapsed.String(),
		"total_elapsed":  totalElapsed.String(),
	})
}

// handleCrawlEnqueue accepts a single URL forwarded from a peer shard and
// pushes it onto the local crawler's frontier. Auth via cfg.Cluster.
// PeerAuthToken Bearer header.
type crawlEnqueueReq struct {
	URL string `json:"url"`
}

func (s *pebbleHTTP) handleCrawlEnqueue(w http.ResponseWriter, r *http.Request) {
	// Auth
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid peer token")
			return
		}
	}
	if s.crawlSeed == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req crawlEnqueueReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.URL == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"url\": \"...\"}")
		return
	}
	if err := s.crawlSeed(req.URL); err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queued": req.URL})
}

// handleFrontierClear wipes the entire frontier in a single Pebble
// DeleteRange — every queued URL, primary + secondary index. Used when
// the frontier is so polluted by spam-discovery crawls that
// claim-time exclude rules can't drain it in reasonable time. Operator
// re-seeds via seeds-file restart or /admin/sitemap-import calls.
//
// Sees the same PeerAuthToken as the other admin endpoints. Returns the
// approximate count of deleted entries (DeleteRange is O(tombstones),
// not O(N), so the count is reported as -1 to signal "swept range").
func (s *pebbleHTTP) handleFrontierClear(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid peer token")
			return
		}
	}
	if err := s.store.ClearFrontier(r.Context()); err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cleared": true})
}

// handleFrontierPurgeHost deletes every queued frontier entry whose host
// matches the supplied value. Used to drain blacklisted hosts that the
// round-robin cursor would take days to chew through one-by-one (the
// cursor visits each host once per cycle; a 5M-entry host needs 5M
// cycles to drain, days of cycle time at typical claim rates).
type frontierPurgeReq struct {
	Host string `json:"host"`
}

func (s *pebbleHTTP) handleFrontierPurgeHost(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid peer token")
			return
		}
	}
	var req frontierPurgeReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.Host == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"host\": \"...\"}")
		return
	}
	n, err := s.store.PurgeFrontierByHost(r.Context(), req.Host)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"purged": n, "host": req.Host})
}

// handleSitemapImport fetches a sitemap.xml (or sitemap-index, one level
// of recursion) and pushes every <loc> entry to the live frontier. Same
// auth as crawl-enqueue. Synchronous — returns when all URLs are queued.
// For very large sitemaps (>50K URLs) the call may take seconds.
type sitemapImportReq struct {
	URL string `json:"url"`
}

func (s *pebbleHTTP) handleSitemapImport(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.crawlSeedSitemap == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req sitemapImportReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.URL == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"url\": \"https://.../sitemap.xml\"}")
		return
	}
	t0 := time.Now()
	n, err := s.crawlSeedSitemap(r.Context(), req.URL)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("sitemap-import: queued %d URLs from %s in %s", n, req.URL, time.Since(t0).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, map[string]any{
		"sitemap": req.URL,
		"queued":  n,
		"elapsed": time.Since(t0).String(),
	})
}

// handleCrawlNow runs the fetch-parse-index pipeline synchronously for one
// or more URLs, bypassing the persistent frontier entirely. Use this when
// the round-robin cursor in a large frontier (10M+) would take hours to
// reach an explicitly-seeded URL — direct fetch + UpsertDocument + chunk +
// embed all happen in this request. Returns per-URL outcomes.
type crawlNowReq struct {
	URLs []string `json:"urls"`
	URL  string   `json:"url,omitempty"`
}

func (s *pebbleHTTP) handleCrawlNow(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.crawlFetchNow == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req crawlNowReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err := json.Unmarshal(body, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "expected {\"urls\":[...]} or {\"url\":\"...\"}")
		return
	}
	urls := req.URLs
	if req.URL != "" {
		urls = append(urls, req.URL)
	}
	if len(urls) == 0 {
		writeProblem(w, http.StatusBadRequest, "no URLs in body")
		return
	}
	results := make([]map[string]any, 0, len(urls))
	t0 := time.Now()
	for _, u := range urls {
		err := s.crawlFetchNow(r.Context(), u)
		out := map[string]any{"url": u}
		if err != nil {
			out["error"] = err.Error()
		} else {
			out["ok"] = true
		}
		results = append(results, out)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"results": results,
		"elapsed": time.Since(t0).String(),
	})
}

// evalQuickQueries is the canned smoke-eval set. Chosen for breadth:
// 3 definition + 3 technical-howto + 2 comparison + 2 findability. Tracks
// the same answered/no-info/error verdicts the external 60-Q harness uses,
// so a regression here predicts a regression in the bigger eval. Hardcoded
// (no new deps) so any operator can hit /admin/eval-quick and get an
// answer rate without setting up Python or external test infra.
var evalQuickQueries = []string{
	"what is BM25 ranking function",
	"what is HNSW algorithm",
	"what is reciprocal rank fusion",
	"how does retrieval augmented generation work",
	"how does Go goroutines scheduling work",
	"explain CAP theorem in distributed systems",
	"compare BM25 vs dense vector retrieval",
	"compare REST vs GraphQL APIs",
	"what is Pilot Protocol overlay network",
	"what is sentence embedding",
}

// handleEvalQuick runs the 10-query smoke eval against the running cosift
// and returns answered-rate + per-query verdicts. Each query hits /answer
// in-process (re-uses the same chat/retrieval stack as a real /answer call),
// so the rate that comes back matches what users see.
func (s *pebbleHTTP) handleEvalQuick(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented, "eval-quick requires cfg.Chat.Model")
		return
	}
	// the server-wide WriteTimeout (60s) was killing this handler
	// before 10 sequential /answer calls finished — connection closed mid-write,
	// curl saw "empty reply from server" (exit 52). Disable the deadline for
	// this long-running admin endpoint via ResponseController. Pairs with
	// bounded-parallel dispatch below so the 10-query batch finishes in
	// ~one chat-LLM round-trip instead of ten.
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
	type queryResult struct {
		Query     string `json:"query"`
		Verdict   string `json:"verdict"` // answered | no_info | empty | error
		LatencyMs int    `json:"latency_ms"`
		Sources   int    `json:"sources"`
		Suggest   string `json:"suggest_escalation,omitempty"`
	}
	results := make([]queryResult, len(evalQuickQueries))
	t0 := time.Now()

	// Bounded-parallel dispatch. vLLM batches concurrent requests so 4-wide
	// fan-out finishes in roughly one chat round-trip rather than 10.
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for i, q := range evalQuickQueries {
		wg.Add(1)
		go func(i int, q string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			qStart := time.Now()
			subReq, _ := http.NewRequestWithContext(r.Context(), http.MethodGet,
				"/answer?q="+url.QueryEscape(q)+"&stream=false&retriever=hybrid", nil)
			rec := newResponseRecorder()
			s.handleAnswer(rec, subReq)
			latencyMs := int(time.Since(qStart) / time.Millisecond)

			var ar answerResponse
			if jerr := json.Unmarshal(rec.body.Bytes(), &ar); jerr != nil || rec.code >= 400 {
				results[i] = queryResult{Query: q, Verdict: "error", LatencyMs: latencyMs}
				return
			}
			var verdict string
			switch {
			case len(ar.Answer) < 30:
				verdict = "empty"
			case answerLooksLikeNoInfo(ar.Answer):
				verdict = "no_info"
			default:
				verdict = "answered"
			}
			results[i] = queryResult{
				Query:     q,
				Verdict:   verdict,
				LatencyMs: latencyMs,
				Sources:   len(ar.Sources),
				Suggest:   ar.SuggestEscalation,
			}
		}(i, q)
	}
	wg.Wait()

	answered, noInfo, errCount, empty, totalMs := 0, 0, 0, 0, 0
	for _, r := range results {
		totalMs += r.LatencyMs
		switch r.Verdict {
		case "answered":
			answered++
		case "no_info":
			noInfo++
		case "empty":
			empty++
		default:
			errCount++
		}
	}
	n := len(evalQuickQueries)
	writeJSON(w, http.StatusOK, map[string]any{
		"queries":         n,
		"answered":        answered,
		"no_info":         noInfo,
		"empty":           empty,
		"errors":          errCount,
		"answer_rate_pct": 100 * answered / n,
		"avg_latency_ms":  totalMs / n,
		"total_elapsed":   time.Since(t0).String(),
		"chat_model":      s.chat.Model(),
		"results":         results,
	})
}

// handleHNSWCompact runs HNSW.Compact() in-place, then clears the persisted
// 'v' family and writes a fresh full snapshot so disk matches the compacted
// in-memory graph. Cheaper than the offline hnsw-rebuild subcommand: Compact
// keeps the existing topology among surviving nodes (O(N + edges)), whereas
// Rebuild re-inserts every node via HNSW search (multiple minutes per million
// passages). Operators run this when stats.zombie_nodes climbs above ~30% of
// nodes_total.
//
// Synchronous; holds the HNSW write lock during the compact step and the
// read lock during the persist step. Dense retrieval and AddPassage calls
// queue for the duration. The server-wide WriteTimeout is disabled here via
// ResponseController because compacting a multi-million-node graph routinely
// runs past 60s. Returns counters so operators can confirm progress.
func (s *pebbleHTTP) handleHNSWCompact(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.hnsw == nil {
		writeProblem(w, http.StatusNotImplemented, "hnsw-compact requires a loaded HNSW graph")
		return
	}
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
	skipPersist := r.URL.Query().Get("skip_persist") == "1"

	before := s.hnsw.Len()
	t0 := time.Now()
	removed := s.hnsw.Compact()
	compactDur := time.Since(t0)
	after := s.hnsw.Len()

	resp := map[string]any{
		"nodes_before": before,
		"nodes_after":  after,
		"removed":      removed,
		"compact_ms":   compactDur.Milliseconds(),
		"persisted":    false,
	}

	if removed == 0 || skipPersist {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	// Compact remapped node indices; the persisted 'v' family now points at
	// stale slots. Wipe and full-rewrite. PQ codes follow node indices too,
	// so clear 'q' as well — operators must re-run /admin/pq-train if PQ was
	// in use.
	persistT0 := time.Now()
	ctx := r.Context()
	if err := s.store.ClearVectorFamily(ctx); err != nil {
		resp["persist_error"] = "clear vector family: " + err.Error()
		writeJSON(w, http.StatusInternalServerError, resp)
		return
	}
	if err := s.store.ClearPQFamily(ctx); err != nil {
		resp["persist_error"] = "clear pq family: " + err.Error()
		writeJSON(w, http.StatusInternalServerError, resp)
		return
	}
	if err := s.hnsw.Persist(ctx, s.store); err != nil {
		resp["persist_error"] = "persist: " + err.Error()
		writeJSON(w, http.StatusInternalServerError, resp)
		return
	}
	resp["persisted"] = true
	resp["persist_ms"] = time.Since(persistT0).Milliseconds()
	log.Printf("hnsw-compact: removed=%d (%.1f%% zombies) compact=%s persist=%s nodes %d→%d",
		removed, 100*float64(removed)/float64(before),
		compactDur.Round(time.Millisecond), time.Since(persistT0).Round(time.Millisecond),
		before, after)
	writeJSON(w, http.StatusOK, resp)
}

// responseRecorder captures an http.Handler's output for in-process
// dispatch. Minimal — only what handleEvalQuick needs.
type responseRecorder struct {
	hdr  http.Header
	body bytes.Buffer
	code int
}

func newResponseRecorder() *responseRecorder {
	return &responseRecorder{hdr: make(http.Header), code: 200}
}

func (r *responseRecorder) Header() http.Header         { return r.hdr }
func (r *responseRecorder) Write(b []byte) (int, error) { return r.body.Write(b) }
func (r *responseRecorder) WriteHeader(c int)           { r.code = c }

// handleEmbedBackfill walks famDocMeta, finds docs whose URL has zero
// HNSW vector nodes (lexical-only ingest from WET imports, or any docs
// indexed before an embedder was wired), and embeds them. Pairs with
// /admin/wet-import?lexical_only=true so bulk-loaded content gets dense
// retrieval after the fast lexical pass.
//
// Body: {"limit": 10000, "workers": 4}
//   - limit:   cap on docs processed this call (0 = unlimited)
//   - workers: concurrent embed pipelines (default 4)
//
// Idempotent against concurrent re-runs — re-embedding a doc that
// already has vectors is a no-op because UpsertPassage just adds more
// nodes pointing at the same URL (which the dedup-by-URL in
// HNSW.Search picks the best of anyway). Cleaner: zombie-reclaim kicks
// in to invalidate prior, then fresh vectors take over.
type embedBackfillReq struct {
	Limit   int `json:"limit,omitempty"`
	Workers int `json:"workers,omitempty"`
}

func (s *pebbleHTTP) handleEmbedBackfill(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.embedder == nil || s.hnsw == nil {
		writeProblem(w, http.StatusNotImplemented, "embed backfill requires both an embedder and HNSW graph to be configured")
		return
	}
	var req embedBackfillReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	_ = json.Unmarshal(body, &req)
	if req.Workers <= 0 {
		req.Workers = 4
	}
	if req.Workers > 32 {
		req.Workers = 32
	}

	// Pipeline: iter docs → filter (no-vector) → chunker → embed → write.
	// Bounded channel so we don't materialize the candidate list in memory.
	type cand struct {
		docID int64
		url   string
	}
	candCh := make(chan cand, req.Workers*4)

	t0 := time.Now()
	var scanned, missing, embedded atomic.Int64

	// Find candidates: docs with zero HNSW vectors. Producer.
	go func() {
		defer close(candCh)
		_ = s.store.IterDocsLite(r.Context(), func(docID int64, url string) error {
			scanned.Add(1)
			if req.Limit > 0 && missing.Load() >= int64(req.Limit) {
				return errors.New("limit reached") // sentinel — stops IterDocsLite
			}
			if _, ok := s.hnsw.LookupVectorByURL(url); ok {
				return nil // already has vectors
			}
			missing.Add(1)
			select {
			case candCh <- cand{docID: docID, url: url}:
			case <-r.Context().Done():
				return r.Context().Err()
			}
			return nil
		})
	}()

	// Worker pool: chunk + embed + write passages per doc.
	var wg sync.WaitGroup
	for i := 0; i < req.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range candCh {
				doc, err := s.store.GetDocByID(r.Context(), c.docID)
				if err != nil || doc == nil || doc.Text == "" {
					continue
				}
				// Use index defaults (NewChunker passes 0,0 = default 320/64).
				chunker := index.NewChunker()
				chunks := chunker.Chunk(doc.Text)
				if len(chunks) == 0 {
					continue
				}
				const tokenCap = 1500
				texts := make([]string, len(chunks))
				for j, ch := range chunks {
					texts[j] = truncateForEmbedLite(ch.Text, tokenCap)
				}
				vecs, eErr := s.embedder.Embed(r.Context(), texts)
				if eErr != nil || len(vecs) != len(chunks) {
					continue
				}
				for j, ch := range chunks {
					s.hnsw.AddPassage(doc.URL, doc.Title, ch.Offset, ch.Length, vecs[j])
					_ = j
				}
				embedded.Add(1)
			}
		}()
	}
	wg.Wait()

	log.Printf("embed-backfill: scanned=%d missing=%d embedded=%d in %s",
		scanned.Load(), missing.Load(), embedded.Load(), time.Since(t0).Round(time.Second))
	writeJSON(w, http.StatusOK, map[string]any{
		"scanned":  scanned.Load(),
		"missing":  missing.Load(),
		"embedded": embedded.Load(),
		"elapsed":  time.Since(t0).String(),
	})
}

// truncateForEmbedLite mirrors the crawler's helper without pulling the
// crawler package in. Same heuristic: cap by approximate token count.
func truncateForEmbedLite(s string, tokenCap int) string {
	// Rough: 1 token ≈ 4 chars for ASCII; cap at 4×tokenCap bytes as a
	// fast upper bound. Real chunker is at ~320 words = ~1200 tokens, so
	// 1500-token cap rarely triggers.
	maxBytes := tokenCap * 4
	if len(s) <= maxBytes {
		return s
	}
	return s[:maxBytes]
}

// handleSitePack discovers a site's sitemaps + RSS feeds and bulk-enqueues
// everything found. Targets the "I want to index THIS site" operator flow
// without editing cosift.json + restarting + waiting for the cursor.
// Discovery order:
//  1. GET https://<host>/robots.txt — look for `Sitemap:` directives
//  2. Fallback to canonical /sitemap.xml + /sitemap_index.xml
//  3. RSS at common paths: /feed, /feed.xml, /rss, /rss.xml, /atom.xml, /feed/atom
//
// Each found resource is run through the existing SeedSitemap / SeedRSS
// primitives. Returns per-resource counts.
type sitePackReq struct {
	Host string `json:"host"` // e.g. "example.com" or "blog.example.com" — no scheme
}

func (s *pebbleHTTP) handleSitePack(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.crawlSeedSitemap == nil || s.crawlSeedRSS == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req sitePackReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.Host == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"host\":\"example.com\"}")
		return
	}
	host := strings.TrimSpace(req.Host)
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimSuffix(host, "/")
	if host == "" || strings.Contains(host, "/") {
		writeProblem(w, http.StatusBadRequest, "host must be a bare hostname like example.com")
		return
	}
	base := "https://" + host
	hc := &http.Client{Timeout: 20 * time.Second}

	type result struct {
		Source  string `json:"source"` // "robots-sitemap" | "fallback-sitemap" | "rss"
		URL     string `json:"url"`
		Indexed int    `json:"indexed"`
		Error   string `json:"error,omitempty"`
	}
	results := make([]result, 0, 8)
	t0 := time.Now()

	// Step 1: robots.txt for Sitemap: directives.
	sitemapsFromRobots := []string{}
	if rresp, err := hc.Get(base + "/robots.txt"); err == nil && rresp.StatusCode < 400 {
		rbody, _ := io.ReadAll(io.LimitReader(rresp.Body, 2<<20))
		rresp.Body.Close()
		for _, line := range strings.Split(string(rbody), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(strings.ToLower(line), "sitemap:") {
				val := strings.TrimSpace(line[len("sitemap:"):])
				if val != "" {
					sitemapsFromRobots = append(sitemapsFromRobots, val)
				}
			}
		}
	}
	// Step 2: if robots.txt gave nothing, try canonical paths.
	candidateSitemaps := sitemapsFromRobots
	if len(candidateSitemaps) == 0 {
		// /sitemap.xml is the canonical
		// spec but many CMSes (WordPress, Yoast, Ghost, Hugo themes) ship
		// at non-canonical paths. Try a small ordered list before giving up.
		// Stops on first successful fetch — the order matters: /sitemap.xml
		// first (most common), then WordPress's /wp-sitemap.xml + Yoast's
		// per-content-type splits, then index variants.
		for _, p := range []string{
			"/sitemap.xml",
			"/wp-sitemap.xml",    // WordPress 5.5+
			"/sitemap_index.xml", // Yoast SEO
			"/post-sitemap.xml",  // Yoast posts
			"/page-sitemap.xml",  // Yoast pages
			"/sitemap-index.xml", // some CMSes hyphenate
			"/sitemap.xml.gz",    // gzipped variant (sitemap.go handles .gz)
		} {
			candidateSitemaps = append(candidateSitemaps, base+p)
		}
	}
	for _, su := range candidateSitemaps {
		n, err := s.crawlSeedSitemap(r.Context(), su)
		res := result{URL: su, Indexed: n}
		if len(sitemapsFromRobots) > 0 {
			res.Source = "robots-sitemap"
		} else {
			res.Source = "fallback-sitemap"
		}
		if err != nil {
			res.Error = err.Error()
		}
		results = append(results, res)
	}
	// Step 3: common RSS paths.
	for _, p := range []string{"/feed", "/feed.xml", "/rss", "/rss.xml", "/atom.xml", "/feed/atom"} {
		fu := base + p
		n, err := s.crawlSeedRSS(r.Context(), fu)
		// Silently skip RSS paths that 404 / don't parse — most sites have
		// only one of the listed paths.
		if err != nil && n == 0 {
			continue
		}
		results = append(results, result{Source: "rss", URL: fu, Indexed: n})
	}

	total := 0
	for _, r := range results {
		total += r.Indexed
	}
	log.Printf("site-pack: %s discovered %d resources, enqueued %d URLs in %s",
		host, len(results), total, time.Since(t0).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, map[string]any{
		"host":          host,
		"resources":     len(results),
		"total_indexed": total,
		"elapsed":       time.Since(t0).String(),
		"results":       results,
	})
}

// handleWETImportBulk fetches a CommonCrawl `wet.paths.gz` manifest,
// takes the first N entries (or skip+take), and runs `/admin/wet-import`
// against each one in parallel. Lets operators bulk-ingest a release with
// one call instead of repeatedly POSTing per file. Synchronous — blocks
// until all N finish, returns total docs indexed.
//
// Example body:
//
//	{"manifest_url":"https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-51/wet.paths.gz",
//	 "count":4, "skip":0, "concurrency":2, "lexical_only":true}
type wetImportBulkReq struct {
	ManifestURL string `json:"manifest_url"`
	Count       int    `json:"count"`
	Skip        int    `json:"skip,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	LexicalOnly bool   `json:"lexical_only,omitempty"`
	DedupeFresh bool   `json:"dedupe_fresh,omitempty"`
}

func (s *pebbleHTTP) handleWETImportBulk(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.crawlSeedWET == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req wetImportBulkReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.ManifestURL == "" || req.Count <= 0 {
		writeProblem(w, http.StatusBadRequest, "expected {\"manifest_url\":\"https://.../wet.paths.gz\",\"count\":N}")
		return
	}
	if req.Concurrency <= 0 {
		req.Concurrency = 2
	}
	if req.Concurrency > 8 {
		req.Concurrency = 8 // beyond this the pebble write lock dominates anyway
	}

	// Fetch the manifest. wet.paths.gz is a gzipped newline-delimited list
	// of relative paths under data.commoncrawl.org.
	mreq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, req.ManifestURL, http.NoBody)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "build manifest request: "+err.Error())
		return
	}
	mreq.Header.Set("User-Agent", "cosift-bulk-import")
	mresp, err := (&http.Client{Timeout: 60 * time.Second}).Do(mreq)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "fetch manifest: "+err.Error())
		return
	}
	defer mresp.Body.Close()
	if mresp.StatusCode >= 400 {
		writeProblem(w, http.StatusBadGateway, fmt.Sprintf("manifest http %d", mresp.StatusCode))
		return
	}
	zr, err := gzip.NewReader(mresp.Body)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "gunzip manifest: "+err.Error())
		return
	}
	defer zr.Close()
	mbody, err := io.ReadAll(io.LimitReader(zr, 32<<20))
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "read manifest: "+err.Error())
		return
	}
	// Each line is a path like "crawl-data/CC-MAIN-.../wet/...warc.wet.gz".
	// Prepend the data.commoncrawl.org base.
	all := strings.Split(strings.TrimSpace(string(mbody)), "\n")
	if req.Skip >= len(all) {
		writeProblem(w, http.StatusBadRequest, fmt.Sprintf("skip=%d exceeds manifest size %d", req.Skip, len(all)))
		return
	}
	end := req.Skip + req.Count
	if end > len(all) {
		end = len(all)
	}
	paths := all[req.Skip:end]

	// Run imports in parallel, bounded by concurrency.
	type result struct {
		URL     string `json:"url"`
		Indexed int    `json:"indexed"`
		Elapsed string `json:"elapsed"`
		Error   string `json:"error,omitempty"`
	}
	results := make([]result, len(paths))
	sem := make(chan struct{}, req.Concurrency)
	var wg sync.WaitGroup
	t0 := time.Now()
	for i, p := range paths {
		i, p := i, strings.TrimSpace(p)
		if p == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			full := "https://data.commoncrawl.org/" + p
			start := time.Now()
			n, err := s.crawlSeedWET(r.Context(), full, req.DedupeFresh, req.LexicalOnly)
			results[i].URL = full
			results[i].Indexed = n
			results[i].Elapsed = time.Since(start).Round(time.Second).String()
			if err != nil {
				results[i].Error = err.Error()
			}
		}()
	}
	wg.Wait()

	total := 0
	for _, r := range results {
		total += r.Indexed
	}
	log.Printf("wet-import-bulk: indexed %d docs from %d WET files in %s", total, len(paths), time.Since(t0).Round(time.Second))
	writeJSON(w, http.StatusOK, map[string]any{
		"manifest_url":  req.ManifestURL,
		"files":         len(paths),
		"total_indexed": total,
		"elapsed":       time.Since(t0).String(),
		"results":       results,
	})
}

// handleWETImport streams a CommonCrawl WET file (gzipped, pre-extracted
// plain text per URL) and runs each record through UpsertDocument + BM25
// indexing + chunk + embed. Bypasses the fetch-and-parse pipeline entirely
// because WET bodies are already extracted text — typically 50-100× faster
// than open-web crawling.
//
// Example: POST {"url":"https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-09/segments/.../wet/CC-MAIN-...wet.gz"}
type wetImportReq struct {
	URL         string `json:"url"`
	DedupeFresh bool   `json:"dedupe_fresh,omitempty"` // skip URLs already-fresh in famDoc
	LexicalOnly bool   `json:"lexical_only,omitempty"` // BM25-only ingest, defer dense embed to /admin/embed-backfill
}

func (s *pebbleHTTP) handleWETImport(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.crawlSeedWET == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req wetImportReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.URL == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"url\":\"https://.../...warc.wet.gz\"}")
		return
	}
	t0 := time.Now()
	n, err := s.crawlSeedWET(r.Context(), req.URL, req.DedupeFresh, req.LexicalOnly)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("wet-import: indexed %d docs from %s in %s", n, req.URL, time.Since(t0).Round(time.Second))
	writeJSON(w, http.StatusOK, map[string]any{
		"wet":     req.URL,
		"indexed": n,
		"elapsed": time.Since(t0).String(),
	})
}

// handleRSSImport fetches an RSS 2.0 or Atom feed and pushes every <item>/
// <entry> link to the live frontier. Same auth shape as sitemap-import.
// Designed to be cron-friendly: idempotent against the frontier (re-seeding
// the same feed only adds newly-listed items).
type rssImportReq struct {
	URL string `json:"url"`
}

func (s *pebbleHTTP) handleRSSImport(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got != want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	if s.crawlSeedRSS == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req rssImportReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.URL == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"url\": \"https://.../feed.xml\"}")
		return
	}
	t0 := time.Now()
	n, err := s.crawlSeedRSS(r.Context(), req.URL)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("rss-import: queued %d URLs from %s in %s", n, req.URL, time.Since(t0).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, map[string]any{
		"feed":    req.URL,
		"queued":  n,
		"elapsed": time.Since(t0).String(),
	})
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
	writeJSON(w, http.StatusOK, map[string]any{
		"queued":    fs.Queued,
		"in_flight": fs.InFlight,
		"done":      fs.Done,
		"errored":   fs.Errored,
		"top_hosts": hosts,
	})
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

// searchHit is the minimal hit shape returned by pebble-serve's /search.
// Intentionally narrower than the SQLite server's SearchHit — feature
// parity (highlight, excerpt, calibration, paragraph filters) grows as
// follow-up iters port each one through the Pebble side.
type searchHit struct {
	URL         string     `json:"url"`
	Title       string     `json:"title"`
	Score       float64    `json:"score"`
	Excerpt     string     `json:"excerpt,omitempty"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	Author      string     `json:"author,omitempty"`
	Text        string     `json:"text,omitempty"`
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

// POST /search with JSON body — for callers whose query lists,
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
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

// POST variants of /find_similar, /answer, /research. Same pattern
// as POST /search — re-encode JSON body as URL.Values, hand off to the GET
// handler. The GET handlers own the param semantics; POST is a wire-level
// alternative for callers whose payloads don't fit cleanly into a query string.

type findSimilarRequest struct {
	URL            string `json:"url,omitempty"`
	Text           string `json:"text,omitempty"`  // content-based MLT, no source URL needed
	Title          string `json:"title,omitempty"` // optional title-boost when using text mode
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	r.URL.RawQuery = req.toValues().Encode()
	s.handleAnswer(w, r)
}

func (s *pebbleHTTP) handleResearchPOST(w http.ResponseWriter, r *http.Request) {
	var req synthRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	r.URL.RawQuery = req.toValues().Encode()
	s.handleResearch(w, r)
}

func (s *pebbleHTTP) handleSearch(w http.ResponseWriter, r *http.Request) {
	// When this process is in cluster
	// gateway-mode AND the caller hasn't already set ?cluster_local=1
	// (which is how the gateway tells peers "just do your local search,
	// don't fan out again"), fan out to peers and RRF-merge their results.
	if s.cluster.GatewayMode && s.cluster.IsClustered() && r.URL.Query().Get("cluster_local") != "1" {
		s.handleSearchGateway(w, r)
		return
	}
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
	// include_domains / exclude_domains — mirrors the
	// SQLite-side server semantics (CSV, dot-boundary suffix match).
	// since / until — ISO-date filters on doc.PublishedAt.
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
	// rerank widens both the fetch and the keep-cap before filtering,
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
	// expansion dispatch (bare / HyDE / paraphrase+RRF).
	// Shared
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
	// enrich each surviving hit with Excerpt + PublishedAt + Author
	// via a single GetDocByURL per hit. Cost: k extra Gets — block-cache hot,
	// ~ms-scale at the k≤100 we accept. Opt out with ?enrich=false for callers
	// that only need scoring. The date filter forces a doc fetch
	// even when enrich=false, since PublishedAt lives on the gob.
	// ?include_text=true inlines doc.Text on each hit so research
	// pipelines avoid the N+1 round trip to /contents. Off by default —
	// payload size grows linearly with k and average doc length.
	enrich := r.URL.Query().Get("enrich") != "false"
	if wantRerank {
		enrich = true // rerank needs per-doc text — overrides enrich opt-out
	}
	// time-decay multiplier needs PublishedAt per hit, which lives
	// behind the enrich flag. Force enrich when decay is requested so the
	// signal is available downstream.
	decayHalfLife, decaySet := parseDecayHalfLife(r.URL.Query().Get("decay"))
	if decaySet {
		enrich = true
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
	// Runs BEFORE rerank so the rerank pool
	// reflects both quality and recency — when fetchK is over-fetched, the
	// reranker still sees the freshest-AND-most-relevant top-N. Hits without
	// PublishedAt are left alone (no signal, no penalty). The pool re-sort
	// re-aligns rerankTexts via a URL→text map so both knobs compose.
	if decaySet {
		var textByURL map[string]string
		if wantRerank && len(rerankTexts) == len(out) {
			textByURL = make(map[string]string, len(out))
			for i, h := range out {
				textByURL[h.URL] = rerankTexts[i]
			}
		}
		applyTimeDecay(out, decayHalfLife, time.Now())
		if textByURL != nil {
			rerankTexts = rerankTexts[:0]
			for _, h := range out {
				rerankTexts = append(rerankTexts, textByURL[h.URL])
			}
		}
	}
	// rerank now that we have keepCap candidates with text.
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
	// MMR diversification. ?mmr=<lambda> (0..1) reorders the
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
	// Applies to
	// the already-collected top-k pool; raise k to widen the pool before
	// re-sorting if you need more candidates for date ordering.
	switch r.URL.Query().Get("sort") {
	case "date_desc":
		sortHitsByDate(out, false)
	case "date_asc":
		sortHitsByDate(out, true)
	}
	// label centralized in buildRetrieverLabel so /answer
	// and /research report the same vocabulary as /search.
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, effectiveQuery != q, wantRerank)
	// mmr suffix when diversification actually fired (HNSW loaded
	// + embedder available). Same conditional as the apply site above —
	// the label tracks the real pipeline.
	if mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr")); mmrSet && s.hnsw != nil && s.embedder != nil && mmrLambda < 1.0 {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	// decay suffix when time-decay was applied.
	if decaySet {
		retrieverLabel += fmt.Sprintf("+decay:%gd", decayHalfLife)
	}
	resp := searchResponse{
		Query:     q,
		Expand:    normalizeExpandMode(expandMode),
		Retriever: retrieverLabel,
		Hits:      out,
		// total_candidates = BM25 candidates considered before
		// filter (capped at fetchK). Operators tuning over-fetch can see
		// whether their filter is dropping a lot — when out=k but
		// total_candidates is close to fetchK, the filter is restrictive
		// enough that you may want to raise k or relax filters.
		TotalCandidates: len(hits),
		Took:            time.Since(start).String(),
	}
	// surface the post-HyDE query when it actually changed, so callers
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
// gives oldest-first; asc=false gives newest-first.
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

// BM25-only "more like this". Mirrors EXA's /findSimilar shape but
// stays dependency-free: no embeddings required. Algorithm is Lucene's MLT —
// pick the source doc's top-N terms by tf·idf, build a query from them, run
// the existing BM25 search, drop the source URL itself. With dense vectors
// off the table on the Pebble path (HNSW indexing during crawl is the iter
// follow-up), this is the cheapest credible /find_similar we can ship.
func (s *pebbleHTTP) handleFindSimilar(w http.ResponseWriter, r *http.Request) {
	// /find_similar runs locally with a
	// known URL+vec, but in a cluster the URL's shard owns its vector and
	// only one shard has it. We fan-out to all shards anyway: the owning
	// shard does the real work; others either fall back to text-mode (if
	// text supplied) or return empty.
	if s.cluster.GatewayMode && s.cluster.IsClustered() && r.URL.Query().Get("cluster_local") != "1" {
		s.handleFindSimilarGateway(w, r)
		return
	}
	start := time.Now()
	// accept either ?url= (existing behavior) or ?text= (content-
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
		// previously referenced `decoded` (only in scope in the
		// url-mode branch) — broken since's text-mode addition.
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
	// optional ?q= augments the auto-derived MLT query so callers
	// can constrain "more like this URL" with an extra concept (e.g.
	// /find_similar?url=...&q=pricing). Appended verbatim — supports the
	// same quoted-phrase shape /search accepts.
	if extra := strings.TrimSpace(r.URL.Query().Get("q")); extra != "" {
		queryStr = queryStr + " " + extra
	}

	// scope MLT with the same retrieval filters /search and /answer
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
	// ?rerank=true closes /find_similar's parity gap with the other
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

	// /find_similar?retriever=dense reuses the source's
	// persisted vector (URL-mode) or embeds the user's text (text-mode) and
	// runs an HNSW cosine search instead of BM25-MLT. ?retriever=hybrid
	// runs BOTH BM25-MLT and dense, then RRF-fuses — the
	// strongest "find similar" signal: lexical precision + semantic recall.
	//
	// Requires COSIFT_LOAD_HNSW=true at server start (for the graph);
	// text-mode additionally needs a configured embedder. URL-mode dense
	// works without an embedder — the source vector is already in the graph
	// from indexing. Missing requirements fall through to BM25-MLT;
	// warningsFor() flags it (carves out the URL-mode-no-embedder
	// case so the warning isn't misleading).
	retrieverParam := r.URL.Query().Get("retriever")
	useDense := retrieverParam == "dense" && s.hnsw != nil
	useHybrid := retrieverParam == "hybrid" && s.hnsw != nil
	var (
		hits       []index.Hit
		denseFired bool
		bm25Fired  bool
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
			denseHits := s.applyAuthorityToDense(vhits)
			hits = rrfFuse([][]index.Hit{bm25Hits, denseHits})
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
	// Same pre-rerank placement as
	// /search — re-sort cands by adjusted score so rerank sees a freshness-
	// aware pool. Re-aligns rerankText too via URL→text map.
	decayHalfLife, decaySet := parseDecayHalfLife(r.URL.Query().Get("decay"))
	if decaySet && len(cands) > 0 {
		now := time.Now()
		for i := range cands {
			cands[i].hit.Score *= decayMultiplier(cands[i].hit.PublishedAt, now, decayHalfLife)
		}
		sort.SliceStable(cands, func(i, j int) bool { return cands[i].hit.Score > cands[j].hit.Score })
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
	// MMR diversification on /find_similar. Anchored at the
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
	// label tracks which retrievers actually fired.
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
	if decaySet {
		retrieverLabel += fmt.Sprintf("+decay:%gd", decayHalfLife)
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

// /answer — synthesis over BM25 retrieval. Mirrors the SQLite-side
// /answer in spirit (top-k sources → cited synthesis) but stays minimal: no
// streaming, no rerank, no query expansion. Those land in follow-up iters
// once this surface is exercised. Returns 501 when no chat model is
// configured so the absent capability fails loud instead of silent.
const answerSystemPrompt = `You are a research assistant. Answer the user's question using the provided sources.
- Cite sources by their numeric id in square brackets, e.g. [1] or [2,3]. Every factual claim needs a citation.
- Synthesize the answer from what the sources state, including reasonable inferences from adjacent context — sources rarely state every answer verbatim.
- Only fall back to "the sources do not cover X" if NONE of the sources have any relevant material at all. Do not refuse just because the exact formula, number, or phrase isn't quoted verbatim — extract whatever IS there and say so.
- IGNORE sources that are irrelevant to the question — login forms, page-not-found stubs, wiki edit/diff/admin pages, navigation menus, raw image directories, or content about a different topic. Do not cite them. Do not mention them in your answer. Pretend they weren't in the source list.
- Do not invent facts not supported by the sources.
- Keep the answer focused on what the sources actually say; do not pad.`

// HyDE-style query expansion prompt. Borrowed verbatim from
// internal/server/hyde.go so the SQLite and Pebble paths produce comparable
// expansions and operators don't have to learn two prompt shapes.
const hydeSystemPrompt = `Write a brief, factual passage (2-4 sentences) that would directly answer the user's question. Output ONLY the passage — no preamble, no commentary, no apology if you're uncertain. If the question is ambiguous, pick the most plausible interpretation and answer that. The passage doesn't need to be true; it needs to be the SHAPE of what a relevant document would say. Embedding this passage and searching by its vector will find documents that look like real answers, even if the user's original query was just a few keywords.`

// doChat wraps ChatClient.Chat with attempt/failure counters.
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

// doRerank wraps s.reranker.Rerank with attempt/failure counters.
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

// applyBM25EnvOverrides reads COSIFT_BM25_K1 / _B and applies them
// to idx. Shared by runPebbleServe and runQuery so both honor the same env.
// Returns which knobs landed so callers can log selectively.
type bm25EnvResult struct {
	k1Set, bSet bool
	k1Val, bVal float64
	k1Bad, bBad string // non-empty when env was set but unparseable
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

// paraphraseQuery returns up to n paraphrases of q via the chat
// client, parses the JSON array shape the SQLite-side paraphraser already
// uses, and falls back to nil on any failure (no chat client, empty / malformed
// reply, parse error). Caller decides what to do with an empty list — the
// downstream RRF strategy treats it as 'no expansion, fall back to single
// query'. No cache yet — keyed on (q, n) would need a different shape than
// the HyDE cache; revisit when workload justifies.
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

// rrfFuse implements Reciprocal Rank Fusion across N ranked lists.
// k=60 is the standard Cormack et al. constant; tweaking rarely changes top-k
// ordering meaningfully. Each list contributes 1/(k + rank+1) to a URL's
// fused score; URLs appearing in more lists at higher ranks rise. Returns
// synthesized Hits ordered by fused score (URL/Title from first encounter,
// Score = fused RRF score).
func rrfFuse(lists [][]index.Hit) []index.Hit {
	const fuseK = 60
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

// warningsFor surfaces silent no-ops that callers used to have to
// derive from absent effective_query / retriever fields. Each warning is one
// human-readable sentence; consumers programmatically inspect the slice.
func (s *pebbleHTTP) warningsFor(r *http.Request) []string {
	var w []string
	if mode := r.URL.Query().Get("expand"); mode != "" {
		if s.chat == nil {
			w = append(w, "expand="+mode+" requested but no chat client configured (set cfg.Chat.Model)")
		} else if normalizeExpandMode(mode) == "" {
			// unknown expand value used to be silently ignored.
			w = append(w, "expand="+mode+" is not a known strategy (try: hyde, paraphrase) — treated as no expansion")
		}
	}
	if r.URL.Query().Get("rerank") == "true" && s.reranker == nil {
		w = append(w, "rerank=true requested but no reranker configured (set cfg.Rerank.URL or cfg.Rerank.Enabled)")
	}
	// dense + hybrid retrievers need both a loaded HNSW graph
	// and an embedder. Missing either falls through to BM25 with a warning.
	// /find_similar?url=X URL-mode-dense reads the source vector
	// directly from the graph via LookupVectorByURL — no embed RPC. Skip the
	// embedder warning in that case (would otherwise be misleading: the
	// request succeeded without an embedder).
	// /find_similar falls back to bm25-mlt (not bm25), so the
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
	// MMR diversification needs HNSW + embedder. Bad values (non-
	// float, out of [0,1]) fall through silently — flag them. Missing
	// requirements also warn.
	// /find_similar?url=X reuses the source vector from the graph
	// for the MMR anchor — no embedder needed. Same carve-out as
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
	// invalid decay half-life flags loudly instead of silently
	// being ignored. Empty value is fine (no decay requested).
	if raw := r.URL.Query().Get("decay"); raw != "" {
		if _, ok := parseDecayHalfLife(raw); !ok {
			w = append(w, "decay="+raw+" is not a positive half-life in days (≤ 36500) — time-decay skipped")
		}
	}
	// catch unknown ?sort= values (silently treated as relevance).
	if sortVal := r.URL.Query().Get("sort"); sortVal != "" {
		switch sortVal {
		case "relevance", "date_desc", "date_asc":
			// valid
		default:
			w = append(w, "sort="+sortVal+" is not a known mode (try: relevance, date_desc, date_asc) — treated as relevance")
		}
	}
	// catch obviously-bad ?k= values that silently fell back to the
	// per-endpoint default. Upper-bound clamping varies per endpoint so we
	// flag only the universally-invalid cases (non-integer, zero, negative).
	if kVal := r.URL.Query().Get("k"); kVal != "" {
		if n, err := strconv.Atoi(kVal); err != nil || n <= 0 {
			w = append(w, "k="+kVal+" is not a positive integer — using server default")
		}
	}
	// Users sometimes
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
// no effect).
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

// parseDecayHalfLife parses ?decay= as a positive half-life in days. Returns
// (halfLife, true) when valid. Empty / non-positive → (0, false) so caller
// short-circuits. Bounded at 36500 (100 years) to avoid pathological values.
func parseDecayHalfLife(raw string) (float64, bool) {
	if raw == "" {
		// Half-life from COSIFT_DEFAULT_DECAY_DAYS
		// (default 180 days = 6 months). Docs older than 6 months get exponentially
		// less weight in the score; recent docs surface naturally without keyword
		// hacks like the JS regex. Set the env to 0 to disable globally.
		// Explicit ?decay=N still wins.
		def := 180.0
		if v := os.Getenv("COSIFT_DEFAULT_DECAY_DAYS"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
				def = f
			}
		}
		if def <= 0 {
			return 0, false
		}
		return def, true
	}
	// explicit decay=0 disables (even if env default is on).
	if raw == "0" {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || v > 36500 {
		return 0, false
	}
	return v, true
}

// decayMultiplier computes the time-decay weight for a single PublishedAt.
// Half-life H: doc from H days ago → 0.5x, 2H → 0.25x. Missing date or
// non-positive halfLife → 1.0 (no decay).
func decayMultiplier(publishedAt *time.Time, now time.Time, halfLifeDays float64) float64 {
	if halfLifeDays <= 0 || publishedAt == nil || publishedAt.IsZero() {
		return 1
	}
	ageDays := now.Sub(*publishedAt).Hours() / 24.0
	if ageDays < 0 {
		ageDays = 0 // future-dated → treated as fresh, never boosted
	}
	return math.Exp(-math.Ln2 * ageDays / halfLifeDays)
}

// applyTimeDecay multiplies each hit's Score by decayMultiplier and resorts.
func applyTimeDecay(hits []searchHit, halfLifeDays float64, now time.Time) {
	if halfLifeDays <= 0 || len(hits) == 0 {
		return
	}
	for i := range hits {
		hits[i].Score *= decayMultiplier(hits[i].PublishedAt, now, halfLifeDays)
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
}

// parseMMRLambda parses ?mmr= as a float in [0, 1]. Returns (lambda, true)
// when valid. Empty value or out-of-range returns (0, false) so the caller
// can short-circuit without firing MMR.
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
// vectors before calling.
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
// applies it to whatever URL-keyed slice it has.
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
// directly.
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
// and /research synth endpoints.
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

// buildRetrieverLabel produces the human-readable retriever string
// surfaced on /search, /answer, /research responses. Mirrors the
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

// applyLLMEndpointDefaults sets the retriever-choice and rerank defaults
// the LLM-synthesized endpoints (/answer, /research) want. BM25 alone is
// too slow at multi-million-doc corpora for synchronous LLM use, and the
// reranker is the proven quality recovery layer — so /answer and /research
// default to hybrid + rerank whenever the components are wired, while
// /search keeps its plain-BM25 default for low-latency operator use.
//
// Operators retain full control: ?retriever=bm25|dense|hybrid and
// ?rerank=true|false|1|0 override either default.
func (s *pebbleHTTP) applyLLMEndpointDefaults(retrieverParam, rerankParam string) (string, bool) {
	denseReady := s.hnsw != nil && s.embedder != nil
	if retrieverParam == "" && denseReady {
		retrieverParam = "hybrid"
	}
	wantRerank := s.reranker != nil
	switch rerankParam {
	case "false", "0":
		wantRerank = false
	case "true", "1":
		wantRerank = s.reranker != nil
	}
	// When the vLLM probe says the backend is queue-deep, silently
	// disable rerank. Operators can override by passing ?rerank=true
	// explicitly — but with COSIFT_LLM_DEGRADE_QUEUE breached, the
	// rerank call would just queue and time out anyway. Better to ship
	// hybrid scores fast than block on a doomed LLM call.
	if wantRerank && rerankParam == "" && s.llmProbe != nil && s.llmProbe.Loaded() {
		wantRerank = false
	}
	return retrieverParam, wantRerank
}

// retrieve dispatches retriever choice (bm25 / dense / hybrid) and
// then expansion (bare / HyDE / paraphrase+RRF) for BM25 paths. Shared by
// /search, /answer, /research so all three endpoints get the same retriever
// matrix. Dense / hybrid require both the loaded HNSW graph and an
// embedder; missing either falls through to BM25 — warningsFor()
// surfaces that to the client.
// applyAuthorityToDense converts HNSW VectorHits into index.Hits with
// the per-host authority multiplier applied to the score, AND re-sorts
// the result by the new score so the rank order reflects authority.
// RRF fusion downstream uses rank position, not raw score, so without
// the re-sort the multiplier was a no-op (observed: 'What is the C10K
// problem?' still cited 3 luanjiao.cfd subdomains after applying the
// multiplier alone). PebbleBM25.Search already multiplies AND sorts
// internally; this helper mirrors that contract for the dense path.
func (s *pebbleHTTP) applyAuthorityToDense(vhits []index.VectorHit) []index.Hit {
	out := make([]index.Hit, len(vhits))
	for i, vh := range vhits {
		score := vh.Score
		if s.authority != nil {
			score *= s.authority.Multiplier(hostFromURL(vh.URL))
		}
		out[i] = index.Hit{URL: vh.URL, Title: vh.Title, Score: score}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}

func (s *pebbleHTTP) retrieve(ctx context.Context, q string, fetchK int, retrieverParam, expandMode string) ([]index.Hit, string, error) {
	denseReady := s.hnsw != nil && s.embedder != nil
	switch {
	case retrieverParam == "dense" && denseReady:
		vecs, err := s.embedder.Embed(ctx, []string{q})
		if err != nil {
			return nil, "", fmt.Errorf("embedder: %w", err)
		}
		vhits := s.hnsw.Search(ctx, vecs[0], fetchK)
		hits := s.applyAuthorityToDense(vhits)
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
		denseHits := s.applyAuthorityToDense(denseV)
		hits := rrfFuse([][]index.Hit{bm25Hits, denseHits})
		if len(hits) > fetchK {
			hits = hits[:fetchK]
		}
		return hits, bm25Eff, nil
	default:
		return s.retrieveWithExpansion(ctx, q, fetchK, expandMode)
	}
}

// retrieveWithExpansion dispatches the BM25 call across the three
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
		hits := rrfFuse(lists)
		if len(hits) > fetchK {
			hits = hits[:fetchK]
		}
		return hits, q + " | " + strings.Join(paras, " | "), nil
	case "true", "hyde":
		eq := s.expandQuery(ctx, q)
		hits, err := s.idx.Search(ctx, eq, fetchK)
		return hits, eq, err
	case "entity":
		// Rule-based query rewrite for question-form entity lookups
		// ("who created X", "how tall is X", "what is the capital of
		// X"). Strips the question form, appends canonical-attribute
		// keywords to the original, single BM25 call. The earlier
		// RRF-fusion variant regressed legitimate passes because
		// equal-weight fusion across 4 queries pushed the original's
		// top hit out of the reranker's top-30. Concat preserves the
		// original signal while letting the new keywords add positive
		// scoring contributions where biographical pages exist.
		rewrites := qexpand.RewriteEntity(q)
		if len(rewrites) == 0 {
			hits, err := s.idx.Search(ctx, q, fetchK)
			return hits, q, err
		}
		expanded := q + " " + strings.Join(rewrites, " ")
		hits, err := s.idx.Search(ctx, expanded, fetchK)
		return hits, expanded, err
	default:
		// Bare path runs entity-expansion when the query matches a
		// question-form pattern. Earlier RRF-fuse experiment regressed
		// 'who created the World Wide Web' (passing → bail) because
		// equal-weight fusion across 4 queries pushed the
		// previously-#1 Berners-Lee bio out of the reranker's top-30.
		// Concat-into-single-query avoids the fusion-weight problem:
		// the rewrites only add positive scoring contributions for
		// pages that contain the canonical-attribute words ("creator,"
		// "inventor"), without diluting the original query's ranking
		// signal. Operators can disable with COSIFT_DISABLE_ENTITY_EXPAND=1.
		if os.Getenv("COSIFT_DISABLE_ENTITY_EXPAND") == "" {
			if rewrites := qexpand.RewriteEntity(q); len(rewrites) > 0 {
				expanded := q + " " + strings.Join(rewrites, " ")
				hits, err := s.idx.Search(ctx, expanded, fetchK)
				return hits, expanded, err
			}
		}
		hits, err := s.idx.Search(ctx, q, fetchK)
		return hits, q, err
	}
}

// expandQuery returns q + " " + a HyDE-generated passage when a chat client is
// configured. On any error (no chat client, chat call fails, empty passage),
// returns q unchanged so callers can compose safely without explicit guards.
// added a bounded in-memory cache to skip the chat call on
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
	// Matches the [N] tokens the synth prompt produces in the answer
	// text. SQLite-side AnswerSource has had this; pebble's was missing,
	// which caused the CLI to render every source as '[0]' before the
	// i+1 fallback landed.
	ID          int        `json:"id"`
	URL         string     `json:"url"`
	Title       string     `json:"title"`
	Excerpt     string     `json:"excerpt,omitempty"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	Author      string     `json:"author,omitempty"`
	Text        string     `json:"text,omitempty"`
}

type answerResponse struct {
	Query          string `json:"query"`
	EffectiveQuery string `json:"effective_query,omitempty"`
	Expand         string `json:"expand,omitempty"`
	// retriever label — same vocabulary as /search ("bm25",
	// "dense", "bm25+dense:rrf", "+hyde"/"+paraphrase", "+rerank:<name>").
	Retriever       string         `json:"retriever,omitempty"`
	Answer          string         `json:"answer"`
	Sources         []answerSource `json:"sources"`
	Model           string         `json:"model"`
	Warnings        []string       `json:"warnings,omitempty"`
	TotalCandidates int            `json:"total_candidates,omitempty"`
	Took            string         `json:"took"`
	// when the answer matches a "sources do not contain" pattern,
	// surface a hint URL pointing at /research?strategy=planner. The 60-Q
	// eval showed multi-hop decomposition rescues ~40% of findability +
	// factual-lookup misses. Purely additive — clients decide whether to
	// render a "Try research mode" affordance.
	SuggestEscalation string `json:"suggest_escalation,omitempty"`
}

// answerLooksLikeNoInfo returns true when the answer reads as "sources don't
// cover this" — the canonical /research-escalation trigger. Same phrase set
// the eval uses; kept in sync so eval results predict escalation rate.
func answerLooksLikeNoInfo(s string) bool {
	if len(s) > 800 {
		// Long answers with one disclaimer line aren't bails — only treat
		// short, mostly-disclaimer responses as escalation candidates.
		return false
	}
	low := strings.ToLower(s)
	for _, p := range []string{
		"do not contain", "do not provide", "sources don",
		"not contain the", "no information", "do not include",
		"do not have", "do not cover", "not mention", "no mention",
	} {
		if strings.Contains(low, p) {
			return true
		}
	}
	return false
}

func (s *pebbleHTTP) handleAnswer(w http.ResponseWriter, r *http.Request) {
	// When clustered + gateway mode +
	// not already serving as a leaf for someone else, fan-out retrieval
	// to peers (with include_text), then run a SINGLE synth here.
	if s.cluster.GatewayMode && s.cluster.IsClustered() && r.URL.Query().Get("cluster_local") != "1" {
		s.handleAnswerGateway(w, r)
		return
	}
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
	// Cache + singleflight on /answer. Key is the full query-string
	// (sorted) so different filter combos don't collide. We capture the
	// rendered body into a buffer; on success the buffer is the cache
	// value. Skip the cache for explicit no-cache requests and for
	// streaming clients — bufferedResponse can't flush, so SSE would
	// silently fall through to the sync JSON path.
	if s.answerCache != nil && r.Header.Get("Cache-Control") != "no-cache" && !wantsSSE(r) {
		key := answerCacheKey(r)
		body, err, shared := s.answerCache.Do(key, func() ([]byte, error) {
			rec := &bufferedResponse{header: http.Header{}, status: 200}
			s.handleAnswerInner(rec, r, start)
			if rec.status != http.StatusOK {
				return nil, errAnswerNotOK
			}
			return rec.buf.Bytes(), nil
		})
		if err == nil {
			if shared {
				w.Header().Set("X-Cache", "hit")
			} else {
				w.Header().Set("X-Cache", "miss")
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
			return
		}
		// On non-OK from the inner handler, fall through to the
		// uncached path so the real status code reaches the client.
	}
	s.handleAnswerInner(w, r, start)
}

// errAnswerNotOK signals that the inner handler wrote a non-2xx
// response — don't cache it, and let the caller re-run uncached so
// the proper status reaches the client.
var errAnswerNotOK = errors.New("answer: non-OK response, not cached")

// answerCacheKey is the deterministic cache key for a given /answer
// request. Sorts query parameters so URL-order doesn't fragment the
// cache; drops parameters that don't affect the response.
func answerCacheKey(r *http.Request) string {
	q := r.URL.Query()
	// Don't key on transport-only or debug-only flags.
	q.Del("cluster_local")
	q.Del("trace")
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		for _, v := range q[k] {
			sb.WriteString(k)
			sb.WriteByte('=')
			sb.WriteString(v)
			sb.WriteByte('\x00')
		}
	}
	sum := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(sum[:16])
}

// bufferedResponse captures a handler's writes into a buffer so we
// can store the rendered JSON in answerCache before flushing to the
// real response writer.
type bufferedResponse struct {
	buf    bytes.Buffer
	header http.Header
	status int
}

func (b *bufferedResponse) Header() http.Header        { return b.header }
func (b *bufferedResponse) WriteHeader(statusCode int) { b.status = statusCode }
func (b *bufferedResponse) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}
	return b.buf.Write(p)
}

// handleAnswerInner is the uncached body. Called both via the cache
// path (with a bufferedResponse) and directly when the cache is
// disabled or a non-OK response needs the real writer.
func (s *pebbleHTTP) handleAnswerInner(w http.ResponseWriter, r *http.Request, start time.Time) {
	q := r.URL.Query().Get("q")
	k := 5
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 20 {
			k = n
		}
	}
	// /answer respects the same retrieval filters /search does —
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
	// Default to hybrid + rerank on /answer — the reranker is the proven
	// quality recovery layer; ?retriever=bm25 and ?rerank=false opt out.
	retrieverParam, wantRerank := s.applyLLMEndpointDefaults(
		r.URL.Query().Get("retriever"),
		r.URL.Query().Get("rerank"),
	)
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
	// SSE opt-in. Open the stream BEFORE retrieve() so phase events can
	// surface backend pipeline progress (retrieve / rerank / judge / mmr /
	// synth_start) instead of just the synth-chunk tail. Opt in via
	// ?stream=true or Accept: text/event-stream. Caps requirement: chat
	// client must implement StreamingChatClient AND the ResponseWriter
	// must support http.Flusher; if either is missing, fall through to
	// sync (today's behavior).
	wantStream := wantsSSE(r)
	var sse *answerSSE
	var streamChat embed.StreamingChatClient
	if wantStream {
		if sc, ok := s.chat.(embed.StreamingChatClient); ok {
			if a := newAnswerSSE(w, start); a != nil {
				sse, streamChat = a, sc
				stopKA := sse.startKeepalive(r.Context(), 7*time.Second)
				defer stopKA()
				sse.warnings(s.warningsFor(r))
				sse.phase("parsed", map[string]any{
					"q": q, "k": k, "fetch_k": fetchK,
					"retriever": retrieverParam, "rerank": wantRerank,
				})
			}
		}
	}

	// expansion dispatch (bare / HyDE / paraphrase+RRF).
	// retriever dispatch (bm25 / dense / hybrid) via shared helper.
	expandMode := r.URL.Query().Get("expand")
	hits, effectiveQuery, err := s.retrieve(r.Context(), q, fetchK, retrieverParam, expandMode)
	if err != nil {
		if sse != nil {
			sse.errorEvt(err.Error())
			return
		}
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	if sse != nil {
		sse.phase("retrieve", map[string]any{
			"hits": len(hits), "effective_query": effectiveQuery,
		})
	}
	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
		score      float64 // retrieval score, used by time-decay
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
		c := cand{src: src, excerpt: excerpt, score: h.Score}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
		if len(cands) >= keepCap {
			break
		}
	}
	if sse != nil {
		sse.phase("filter", map[string]any{"candidates": len(cands)})
	}
	// time-decay re-weights then re-sorts BEFORE rerank, mirroring
	// the /search pre-rerank placement so the reranker sees a freshness-
	// aware pool.
	decayHalfLife, decaySet := parseDecayHalfLife(r.URL.Query().Get("decay"))
	if decaySet && len(cands) > 0 {
		now := time.Now()
		for i := range cands {
			cands[i].score *= decayMultiplier(cands[i].src.PublishedAt, now, decayHalfLife)
		}
		sort.SliceStable(cands, func(i, j int) bool { return cands[i].score > cands[j].score })
		if sse != nil {
			sse.phase("decay", map[string]any{"half_life_days": decayHalfLife})
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
		if sse != nil {
			sse.phase("rerank", map[string]any{"reranker": s.reranker.Name(), "candidates": len(cands)})
		}
	}
	// LLM relevance judge — STRICT gate. The reranker re-orders but
	// doesn't drop; without this gate, marginal content (Wikipedia
	// diff/admin pages, dead .edu PDFs, JS-only landing pages with
	// stub HTML) leaks into the synth context and produces "based on
	// the sources, here is some irrelevant trivia" answers.
	//
	// Strict means: if a candidate fails the relevance threshold, it
	// is DROPPED — we do not top up from the rejected pool to reach k.
	// Better to return 2 truly relevant sources than 5 sources where 3
	// are noise. If 0 candidates pass, downstream returns the "no
	// matching sources" response (already handled below).
	//
	// Skip when ?judge=false or no chat client (correctness-preserving).
	if r.URL.Query().Get("judge") != "false" && s.chat != nil && len(cands) > 1 {
		before := len(cands)
		jCands := make([]judge.Candidate, len(cands))
		for i, c := range cands {
			jCands[i] = judge.Candidate{ID: strconv.Itoa(i), Excerpt: c.rerankText}
		}
		verdicts := judge.Judge(r.Context(), s.chat, q, jCands, judge.Options{MinScore: 0.3})
		keep := make([]cand, 0, len(cands))
		for i, c := range cands {
			if i < len(verdicts) && verdicts[i].Keep {
				keep = append(keep, c)
			}
		}
		cands = keep
		if sse != nil {
			sse.phase("judge", map[string]any{
				"before": before, "kept": len(cands), "dropped": before - len(cands),
			})
		}
	}
	// MMR diversification on /answer — same pattern as /search.
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
		if sse != nil && mmrFired {
			sse.phase("mmr", map[string]any{"lambda": mmrLambda, "candidates": len(cands)})
		}
	}
	if len(cands) > k {
		cands = cands[:k]
	}
	sources := make([]answerSource, 0, len(cands))
	var promptSources strings.Builder
	for i, c := range cands {
		// stamp ID = i+1 so the JSON response matches the [N]
		// citation tokens we emit in the synth prompt below.
		c.src.ID = i + 1
		sources = append(sources, c.src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
	}
	// same retriever label vocabulary as /search.
	denseReady := s.hnsw != nil && s.embedder != nil
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, effectiveQuery != q, wantRerank)
	if mmrFired {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	if decaySet {
		retrieverLabel += fmt.Sprintf("+decay:%gd", decayHalfLife)
	}
	if len(sources) == 0 {
		// Distinguish the two failure modes so the client can tell
		// "we have nothing on this topic" (retrieval returned 0 hits)
		// from "we retrieved candidates but the relevance judge dropped
		// every one of them" (the corpus has noise, not the answer).
		msg := "No matching sources in the index."
		if len(hits) > 0 {
			msg = "Retrieval returned candidates but none were judged relevant to the question. The corpus may not cover this topic in usable detail."
		}
		if sse != nil {
			sse.sources(q, sources, streamChat.Model(), retrieverLabel, len(hits))
			sse.chunk(msg)
			sse.done()
			return
		}
		empty := answerResponse{
			Query: q, Expand: normalizeExpandMode(expandMode), Retriever: retrieverLabel,
			Answer:  msg,
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

	if sse != nil {
		sse.sources(q, sources, streamChat.Model(), retrieverLabel, len(hits))
		sse.phase("synth_start", map[string]any{"sources": len(sources), "model": streamChat.Model()})
		full, cerr := s.doChatStream(r.Context(), streamChat, msgs, sse.chunk)
		if cerr != nil {
			sse.errorEvt(cerr.Error())
			return
		}
		if answerLooksLikeNoInfo(full) {
			sse.suggestEscalation(q)
		}
		sse.done()
		return
	}

	answer, err := s.doChat(r.Context(), s.chat, msgs)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "chat: "+err.Error())
		return
	}
	resp := answerResponse{
		Query: q, Expand: normalizeExpandMode(expandMode), Retriever: retrieverLabel,
		Answer: answer, Sources: sources,
		Model: s.chat.Model(), TotalCandidates: len(hits), Took: time.Since(start).String(),
	}
	if effectiveQuery != q {
		resp.EffectiveQuery = effectiveQuery
	}
	resp.Warnings = s.warningsFor(r)
	// suggest /research escalation when the model bailed.
	if answerLooksLikeNoInfo(answer) {
		resp.SuggestEscalation = "/research?q=" + url.QueryEscape(q) + "&strategy=planner"
	}
	writeJSON(w, http.StatusOK, resp)
}

// wantsSSE reports whether the request opts into Server-Sent Events,
// either via ?stream=true or an Accept: text/event-stream header. Both
// /answer, /query, and /research check the same envelope.
func wantsSSE(r *http.Request) bool {
	return r.URL.Query().Get("stream") == "true" ||
		strings.Contains(r.Header.Get("Accept"), "text/event-stream")
}

// answerSSE wraps the SSE writer for /answer streaming. Methods emit the
// well-known event types (phase, sources, answer_chunk, warnings, error,
// suggest_escalation, done). Phase events surface pipeline progress
// (retrieve / rerank / judge / mmr / synth_start) so clients can render a
// timeline of what the backend is doing.
//
// Writes are mu-protected because the keepalive ticker can race with
// the main handler goroutine. The handler does the substantive event
// writes; the ticker emits SSE comment frames (": ka\n\n") to keep
// the HTTP/2 stream alive across long retrieval / rerank / chat calls
// — without them, browsers (Safari especially) close the stream when
// no bytes flow for 10–30 s and the request fails mid-pipeline.
type answerSSE struct {
	w       http.ResponseWriter
	flusher http.Flusher
	start   time.Time
	last    time.Time // last phase emit; used to compute per-phase duration
	mu      sync.Mutex
}

// newAnswerSSE opens an SSE stream. Returns nil if the writer can't flush.
// Caller is responsible for not calling writeProblem after this point —
// the response status (200) and headers are committed here.
//
// Disables the server-wide WriteTimeout (60s) via ResponseController.
// Multi-pass /research can run 90-180s with 3-5 passes against a slow
// chat model; the deadline would kill the connection mid-stream and
// the browser fetch would surface a generic "Load failed". SSE handlers
// flush their own progress, so a stuck handler still gets killed by
// ctx cancellation when the client disconnects.
func newAnswerSSE(w http.ResponseWriter, start time.Time) *answerSSE {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
	w.WriteHeader(http.StatusOK)
	return &answerSSE{w: w, flusher: flusher, start: start, last: start}
}

func (a *answerSSE) send(payload any) {
	buf, _ := json.Marshal(payload)
	a.mu.Lock()
	defer a.mu.Unlock()
	fmt.Fprintf(a.w, "data: %s\n\n", buf)
	a.flusher.Flush()
}

// startKeepalive launches a goroutine that emits SSE data frames every
// interval, keeping the HTTP/2 stream and any intermediate proxy alive
// across long retrieval / chat calls that emit no real data. Returns a
// stop function the caller defers; stopping is idempotent.
//
// We deliberately emit `data: {"type":"ka", ...}` frames rather than the
// spec's `: comment` form. Safari's CFNetwork stream stack tracks
// "no payload data received" separately from "no bytes received" — a
// long-running stream of nothing but comment frames still trips its
// internal timeout and the browser surfaces it as "Error: Load failed".
// JSON ka frames count as real data; the chat UI ignores `type:"ka"`.
func (a *answerSSE) startKeepalive(ctx context.Context, interval time.Duration) (stop func()) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				a.send(map[string]any{
					"type":       "ka",
					"elapsed_ms": time.Since(a.start).Milliseconds(),
				})
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// phase emits a pipeline-step event with elapsed-since-start and
// duration-since-last-phase, plus any caller-supplied detail fields.
func (a *answerSSE) phase(name string, details map[string]any) {
	now := time.Now()
	evt := map[string]any{
		"type":        "phase",
		"name":        name,
		"elapsed_ms":  now.Sub(a.start).Milliseconds(),
		"duration_ms": now.Sub(a.last).Milliseconds(),
	}
	for k, v := range details {
		evt[k] = v
	}
	a.last = now
	a.send(evt)
}

func (a *answerSSE) warnings(warns []string) {
	if len(warns) == 0 {
		return
	}
	a.send(map[string]any{"type": "warnings", "warnings": warns})
}

func (a *answerSSE) sources(q string, src []answerSource, model, retrieverLabel string, totalCandidates int) {
	evt := map[string]any{
		"type":             "sources",
		"query":            q,
		"sources":          src,
		"model":            model,
		"total_candidates": totalCandidates,
	}
	if retrieverLabel != "" {
		evt["retriever"] = retrieverLabel
	}
	a.send(evt)
}

func (a *answerSSE) chunk(delta string) {
	a.send(map[string]any{"type": "answer_chunk", "text": delta})
}

func (a *answerSSE) errorEvt(msg string) {
	a.send(map[string]any{"type": "error", "error": msg})
}

func (a *answerSSE) suggestEscalation(q string) {
	a.send(map[string]any{
		"type":               "suggest_escalation",
		"suggest_escalation": "/research?q=" + url.QueryEscape(q) + "&strategy=planner",
	})
}

func (a *answerSSE) done() {
	a.send(map[string]any{"type": "done", "took": time.Since(a.start).String()})
}

// /research — multi-step retrieval + synthesis. LLM decomposes the
// question into 2-3 sub-queries, each sub-query runs BM25, results are deduped
// by URL keeping the best score, top-k feed a cited synthesis. Mirrors the
// SQLite-side /research planner strategy. No streaming, no rerank, no
// paraphrase strategy yet — those follow once this surface is exercised.
const researchPlanPrompt = `Decompose the user's research question into 2-3 focused sub-queries that, taken together, would cover the answer. Output ONLY a JSON array of strings — no prose, no markdown. Example: ["sub-query 1", "sub-query 2"]`

// queryPlanPrompt is the LLM-orchestrator prompt. The model
// analyzes the user's natural-language query, classifies intent, and
// outputs a structured retrieval plan: expanded queries (3 variations
// covering different angles), recency window, retriever choice, and
// optionally a domain allowlist. The plan then drives a broader RRF-
// fused retrieval. JSON schema is strict; sanity-checked on receive.
const queryPlanPrompt = `You analyze a user's natural-language search query and output a JSON retrieval plan.

Output ONLY a JSON object (no markdown, no prose, no commentary) with this exact shape:
{
  "intent": "current_news" | "factual_lookup" | "comparison" | "research" | "findability",
  "queries": ["expanded query 1", "expanded query 2", "expanded query 3"],
  "since_days": null | <positive integer, days lookback from today>,
  "retriever": "bm25" | "dense" | "hybrid",
  "decay_days": <positive integer half-life> | null
}

Guidance:
- "queries": 3 paraphrases/expansions of the original. Include entity names, synonyms, related terms. e.g. "latest news from Romania" → ["Romania political news 2026", "Romanian government coalition PSD AUR", "Bucharest current events"]
- "since_days": only set when the query implies recency (latest/current/recent/today/news). null otherwise.
- "decay_days": half-life in days. For current-events queries use 60. For factual/research queries use null or 365.
- "retriever": hybrid is the safe default. Use "bm25" only for exact-term lookups (model names, error codes). Use "dense" only for purely conceptual queries with no keyword overlap.
- DO NOT invent include_domains. The corpus chooses sources, not you.

Examples:
Input: "latest news from Romania"
{"intent":"current_news","queries":["Romania political news 2026","Romanian PSD AUR coalition government","Bucharest economy and politics current"],"since_days":90,"retriever":"hybrid","decay_days":60}

Input: "what is BM25 ranking"
{"intent":"factual_lookup","queries":["BM25 Okapi ranking function formula","BM25 information retrieval probabilistic","term frequency inverse document frequency BM25"],"since_days":null,"retriever":"hybrid","decay_days":null}

Input: "compare HNSW vs IVF"
{"intent":"comparison","queries":["HNSW vs IVF approximate nearest neighbor","Hierarchical Navigable Small World versus Inverted File","ANN index tradeoffs HNSW IVF performance"],"since_days":null,"retriever":"hybrid","decay_days":730}`

// queryPlan captures the LLM's structured retrieval plan. Sanitized on
// parse — out-of-range values clipped to safe defaults.
type queryPlan struct {
	Intent    string   `json:"intent"`
	Queries   []string `json:"queries"`
	SinceDays *int     `json:"since_days"`
	Retriever string   `json:"retriever"`
	DecayDays *int     `json:"decay_days"`
}

// parseQueryPlan extracts the planner's JSON output and applies guard-rails.
// Returns a plan with sane defaults if the LLM produces garbage — never
// errors at the caller, so /query always succeeds.
func parseQueryPlan(raw, original string) queryPlan {
	defaultPlan := queryPlan{
		Intent:    "research",
		Queries:   []string{original},
		Retriever: "hybrid",
	}
	// Strip code fences if the LLM wrapped output despite the prompt.
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "```") {
		if i := strings.Index(raw, "\n"); i > 0 {
			raw = raw[i+1:]
		}
		raw = strings.TrimSuffix(raw, "```")
	}
	var p queryPlan
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return defaultPlan
	}
	if len(p.Queries) == 0 {
		p.Queries = []string{original}
	}
	if len(p.Queries) > 5 {
		p.Queries = p.Queries[:5]
	}
	switch p.Retriever {
	case "bm25", "dense", "hybrid":
		// ok
	default:
		p.Retriever = "hybrid"
	}
	if p.SinceDays != nil && (*p.SinceDays <= 0 || *p.SinceDays > 3650) {
		p.SinceDays = nil
	}
	if p.DecayDays != nil && (*p.DecayDays <= 0 || *p.DecayDays > 3650) {
		p.DecayDays = nil
	}
	if p.Intent == "" {
		p.Intent = "research"
	}
	return p
}

// handleQuery is the LLM-orchestrated search/research endpoint.
// Flow: (1) LLM planner analyzes intent + emits expanded queries, (2) each
// expansion runs through /search internals (hybrid + decay), (3) RRF-fuse
// the per-expansion result lists, (4) optional rerank, (5) synth with
// citations. Returns the plan in the response so callers can see what was
// done.
//
// Effectively: /research planner-style decomposition, but optimized for
// QUERY EXPANSION not sub-question DECOMPOSITION. The two are different —
// research splits multi-faceted questions ("compare X and Y" → "X facts",
// "Y facts", "X vs Y tradeoffs"); query expands a single intent into
// paraphrases that catch corpus phrasing variation.
func (s *pebbleHTTP) handleQuery(w http.ResponseWriter, r *http.Request) {
	if s.chat == nil {
		writeProblem(w, http.StatusNotImplemented, "/query requires cfg.Chat.Model")
		return
	}
	start := time.Now()
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing q parameter")
		return
	}

	// SSE opt-in. Stream phase events through the planner → expand → fuse
	// → synth pipeline so operators see the LLM's sub-queries, per-expansion
	// hit counts, fusion result, and synth tokens as they're produced.
	wantStream := wantsSSE(r)
	var sse *answerSSE
	var streamChat embed.StreamingChatClient
	if wantStream {
		if sc, ok := s.chat.(embed.StreamingChatClient); ok {
			if a := newAnswerSSE(w, start); a != nil {
				sse, streamChat = a, sc
				stopKA := sse.startKeepalive(r.Context(), 7*time.Second)
				defer stopKA()
				sse.phase("plan_start", map[string]any{"q": q, "model": s.chat.Model()})
			}
		}
	}

	// Step 1: LLM planner.
	planRaw, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
		{Role: "system", Content: queryPlanPrompt},
		{Role: "user", Content: q},
	})
	if err != nil {
		if sse != nil {
			sse.errorEvt("plan: " + err.Error())
			return
		}
		writeProblem(w, http.StatusBadGateway, "plan: "+err.Error())
		return
	}
	plan := parseQueryPlan(planRaw, q)
	if sse != nil {
		details := map[string]any{
			"intent":    plan.Intent,
			"retriever": plan.Retriever,
			"queries":   plan.Queries,
		}
		if plan.SinceDays != nil {
			details["since_days"] = *plan.SinceDays
		}
		if plan.DecayDays != nil {
			details["decay_days"] = *plan.DecayDays
		}
		sse.phase("plan", details)
	}

	// Step 2: run each expanded query through retrieval, dedupe by URL.
	type hitInfo struct {
		hit    index.Hit
		bestRR float64 // best reciprocal rank across expansions
	}
	byURL := make(map[string]*hitInfo, 64)
	fetchK := 30
	for _, sub := range plan.Queries {
		hits, _, err := s.retrieve(r.Context(), sub, fetchK, plan.Retriever, "")
		if err != nil {
			if sse != nil {
				sse.phase("expand", map[string]any{"query": sub, "error": err.Error()})
			}
			continue
		}
		for rank, h := range hits {
			rr := 1.0 / float64(60+rank) // RRF with k=60
			if existing, ok := byURL[h.URL]; ok {
				if rr > existing.bestRR {
					existing.bestRR = rr
				}
			} else {
				byURL[h.URL] = &hitInfo{hit: h, bestRR: rr}
			}
		}
		if sse != nil {
			sse.phase("expand", map[string]any{"query": sub, "hits": len(hits)})
		}
	}

	// Step 3: sort by fused RRF score with optional per-host boosts.
	fused := make([]index.Hit, 0, len(byURL))
	for _, v := range byURL {
		h := v.hit
		h.Score = v.bestRR
		if len(s.hostBoosts) > 0 {
			h.Score *= index.HostBoostFor(h.URL, s.hostBoosts)
		}
		fused = append(fused, h)
	}
	sort.Slice(fused, func(i, j int) bool { return fused[i].Score > fused[j].Score })
	if sse != nil {
		sse.phase("fuse", map[string]any{"unique_urls": len(fused)})
	}

	// Step 4: optional since filter + decay (decay handled by /answer-style
	// path when we materialize). Apply since filter here on PublishedAt.
	keep := 10
	if v := r.URL.Query().Get("k"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 20 {
			keep = n
		}
	}
	var since time.Time
	if plan.SinceDays != nil {
		since = time.Now().AddDate(0, 0, -(*plan.SinceDays))
	}

	// Step 5: enrich + filter by since.
	type cand struct {
		src     answerSource
		excerpt string
		text    string
	}
	cands := make([]cand, 0, keep)
	for _, h := range fused {
		if len(cands) >= keep {
			break
		}
		doc, err := s.store.GetDocByURL(r.Context(), h.URL)
		if err != nil || doc == nil {
			continue
		}
		if !since.IsZero() && (doc.PublishedAt.IsZero() || doc.PublishedAt.Before(since)) {
			continue
		}
		c := cand{
			src: answerSource{
				ID:    len(cands) + 1,
				URL:   h.URL,
				Title: h.Title,
			},
			excerpt: textExcerpt(doc.Text, 320),
			text:    doc.Text,
		}
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt
			c.src.PublishedAt = &t
		}
		cands = append(cands, c)
	}
	if sse != nil {
		sse.phase("materialize", map[string]any{"candidates": len(cands)})
	}

	// Step 6: synth via answerSystemPrompt.
	var promptSrcs strings.Builder
	sources := make([]answerSource, 0, len(cands))
	for i, c := range cands {
		c.src.ID = i + 1
		c.src.Excerpt = c.excerpt
		fmt.Fprintf(&promptSrcs, "[%d] %s\n%s\n\n", c.src.ID, c.src.URL, truncateForPromptLite(c.text, 1200))
		sources = append(sources, c.src)
	}

	if sse != nil {
		sse.sources(q, sources, s.chat.Model(), "query:planner+hybrid+rrf", len(fused))
		if len(cands) == 0 {
			sse.chunk("No sources matched the planner's expanded queries within the chosen time window.")
			sse.done()
			return
		}
		sse.phase("synth_start", map[string]any{"sources": len(sources), "model": streamChat.Model()})
		_, cerr := s.doChatStream(r.Context(), streamChat, []embed.ChatMsg{
			{Role: "system", Content: answerSystemPrompt},
			{Role: "user", Content: "Sources:\n\n" + promptSrcs.String() + "Question: " + q},
		}, sse.chunk)
		if cerr != nil {
			sse.errorEvt("synth: " + cerr.Error())
			return
		}
		sse.done()
		return
	}

	answerText := ""
	if len(cands) > 0 {
		answerText, err = s.doChat(r.Context(), s.chat, []embed.ChatMsg{
			{Role: "system", Content: answerSystemPrompt},
			{Role: "user", Content: "Sources:\n\n" + promptSrcs.String() + "Question: " + q},
		})
		if err != nil {
			writeProblem(w, http.StatusBadGateway, "synth: "+err.Error())
			return
		}
	} else {
		answerText = "No sources matched the planner's expanded queries within the chosen time window."
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"query":   q,
		"plan":    plan,
		"answer":  answerText,
		"sources": sources,
		"model":   s.chat.Model(),
		"took":    time.Since(start).String(),
	})
}

// truncateForPromptLite caps text by approximate char count for source
// prompts. Same heuristic the eval-quick path uses.
func truncateForPromptLite(s string, maxChars int) string {
	if len(s) <= maxChars {
		return s
	}
	return s[:maxChars] + "…"
}

const researchSynthPrompt = `You are a research assistant. Synthesize an answer to the original question using ONLY the provided sources.
- Cite sources by their numeric id, e.g. [1] or [2,3]. Every factual claim needs a citation.
- If the sources don't cover something, say so plainly — do not invent.
- Keep the answer focused on what the sources actually say.`

// researchRefineSynthPrompt is the system prompt for pass 2+ of a multi-pass
// /research call. The user message includes the prior draft answer alongside
// the expanded source pool. The instruction shape mirrors the standard synth
// prompt but explicitly directs the model to revise rather than start fresh.
const researchRefineSynthPrompt = `You are a research assistant. You have a draft answer from a prior research pass plus additional sources that were gathered to close gaps. Produce a REVISED answer using all the sources now available.
- Cite sources by their numeric id, e.g. [1] or [2,3]. Every factual claim needs a citation.
- Keep correct claims from the draft. Correct or extend where the new sources change the picture.
- If sources still don't cover something, say so plainly — do not invent.
- Output the FULL revised answer, not a delta.`

// selfEvalPrompt asks the model to judge whether its own draft answer is
// sufficient given the question and sources. JSON-only output keeps parsing
// deterministic. The "missing" + "refine_queries" fields drive the next
// research pass when sufficient=false.
const selfEvalPrompt = `You wrote a research answer. Judge whether it's sufficient given the question and sources.

Output ONLY a JSON object (no markdown, no prose):
{"sufficient": true|false, "missing": "<one-line gap, empty if sufficient>", "refine_queries": ["<query>", ...]}

Rules:
- "sufficient": true ONLY if the answer fully addresses the question with current sources.
- "missing": one short sentence describing what would strengthen the answer (empty string if sufficient).
- "refine_queries": 1-3 specific search queries that would close the gap (empty array if sufficient). Make them concrete — entity + concept + qualifier — not vague paraphrases.
- Be honest. Most first-pass answers benefit from one more pass on a specific gap.`

// selfEval is the parsed shape of the JSON the self-eval LLM call emits.
type selfEval struct {
	Sufficient    bool     `json:"sufficient"`
	Missing       string   `json:"missing"`
	RefineQueries []string `json:"refine_queries"`
}

// parseSelfEval extracts the {sufficient, missing, refine_queries} JSON from
// the LLM response. Tolerates leading/trailing prose and markdown fences. On
// any parse failure returns ok=false so the caller can treat as "stop and
// keep the current answer" instead of crashing the stream.
func parseSelfEval(s string) (selfEval, bool) {
	s = strings.TrimSpace(s)
	s = jsonFenceRE.ReplaceAllString(s, "$1")
	// Find the first '{' and last '}' to skip any preamble.
	i := strings.IndexByte(s, '{')
	j := strings.LastIndexByte(s, '}')
	if i < 0 || j <= i {
		return selfEval{}, false
	}
	var ev selfEval
	if err := json.Unmarshal([]byte(s[i:j+1]), &ev); err != nil {
		return selfEval{}, false
	}
	if len(ev.RefineQueries) > 3 {
		ev.RefineQueries = ev.RefineQueries[:3]
	}
	return ev, true
}

// jsonFenceRE strips ```json fences and bare ``` fences from LLM output. The
// judge package has its own copy with the same intent; not worth pulling into
// a shared package for two callers.
var jsonFenceRE = regexp.MustCompile("(?s)```(?:json)?\\s*(.*?)\\s*```")

// researchMaxPasses parses ?max_passes from the request. Default 3, valid
// range 1-10 (clamped). 1 disables the self-eval loop entirely (single-pass
// /research, today's behavior). The upper cap exists to bound LLM cost +
// wall-clock: each non-final pass costs 1 synth + 1 self-eval call on top
// of the per-sub-query retrieval, and the model can in theory loop forever
// emitting "not yet sufficient" verdicts.
func researchMaxPasses(r *http.Request) int {
	const defaultMax = 3
	const hardCap = 10
	v := r.URL.Query().Get("max_passes")
	if v == "" {
		return defaultMax
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return defaultMax
	}
	if n > hardCap {
		return hardCap
	}
	return n
}

type researchResponse struct {
	Query  string   `json:"query"`
	Plan   []string `json:"plan"`
	Expand string   `json:"expand,omitempty"`
	// retriever label, mirroring /search/answer vocabulary.
	Retriever       string         `json:"retriever,omitempty"`
	Answer          string         `json:"answer"`
	Sources         []answerSource `json:"sources"`
	Model           string         `json:"model"`
	Warnings        []string       `json:"warnings,omitempty"`
	TotalCandidates int            `json:"total_candidates,omitempty"`
	Took            string         `json:"took"`
}

func (s *pebbleHTTP) handleResearch(w http.ResponseWriter, r *http.Request) {
	// Planner runs once on gateway,
	// each sub-query scatters to peers, single synth here.
	if s.cluster.GatewayMode && s.cluster.IsClustered() && r.URL.Query().Get("cluster_local") != "1" {
		s.handleResearchGateway(w, r)
		return
	}
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
	// same retrieval filters /search/answer/find_similar accept.
	// Parsed once here so the streaming and sync paths share semantics.
	filt, err := parseRetrievalFilters(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	includeText := r.URL.Query().Get("include_text") == "true"
	// SSE streaming for /research. Same trigger as /answer.
	// Emits phase-aware events so the UI can render the plan and source list
	// before the synth call completes — /research often runs 10–30s
	// (2-3 plan→retrieve→synth chat rounds), so phase visibility matters.
	wantStream := wantsSSE(r)
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
	// each sub-query goes through retrieveWithExpansion, so
	// ?expand=hyde gives per-sub-query HyDE (was's behavior) and
	// ?expand=paraphrase fans out 3 paraphrases × N sub-queries → RRF per
	// sub-query → merge into best{}. The cross-sub-query merge stays
	// score-keep-best (not a second RRF) — that's what specified.
	// retriever dispatch (bm25 / dense / hybrid) applies per
	// sub-query — every sub-query runs through the same retriever.
	expandMode := r.URL.Query().Get("expand")
	// Default to hybrid + rerank on /research — same rationale as /answer.
	retrieverParam, wantRerank := s.applyLLMEndpointDefaults(
		r.URL.Query().Get("retriever"),
		r.URL.Query().Get("rerank"),
	)
	for _, sq := range subs {
		hits, _, err := s.retrieve(r.Context(), sq, perSub, retrieverParam, expandMode)
		if err != nil {
			// log the specific sub-query so operators can diagnose
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
	// same rerank wiring as /search and /answer — pool widens to
	// rerankCandK before materialization, rerank reorders the pool, truncate
	// to k after rerank so citation numbers track the final order.
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
		score      float64 // pooled score, used by time-decay
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
		c := cand{src: src, excerpt: excerpt, score: p.score}
		if wantRerank {
			c.rerankText = doc.Title + "\n" + doc.Text
		}
		cands = append(cands, c)
	}
	// time-decay on /research sync — re-weight pooled cands before
	// rerank. Sub-queries that hit recent docs bubble up first.
	decayHalfLife, decaySet := parseDecayHalfLife(r.URL.Query().Get("decay"))
	if decaySet && len(cands) > 0 {
		now := time.Now()
		for i := range cands {
			cands[i].score *= decayMultiplier(cands[i].src.PublishedAt, now, decayHalfLife)
		}
		sort.SliceStable(cands, func(i, j int) bool { return cands[i].score > cands[j].score })
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
	// MMR diversification on /research sync. /research aggregates
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
		// stamp ID = i+1 so the JSON response matches the [N]
		// citation tokens we emit in the synth prompt below.
		c.src.ID = i + 1
		sources = append(sources, c.src)
		fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
	}
	// Sub-queries don't yield a
	// single effectiveQuery, so "expansion fired" is approximated by intent:
	// chat is up and expandMode requested it. Matches what warningsFor() uses
	// to decide whether to flag a silent no-op.
	denseReady := s.hnsw != nil && s.embedder != nil
	expandFired := expandMode != "" && s.chat != nil
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, expandFired, wantRerank)
	if mmrFired {
		retrieverLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
	}
	if decaySet {
		retrieverLabel += fmt.Sprintf("+decay:%gd", decayHalfLife)
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
		Model: s.chat.Model(), Warnings: s.warningsFor(r),
		TotalCandidates: len(best), Took: time.Since(start).String(),
	})
}

// streamResearch is the SSE form of handleResearch. Event sequence:
//
//	plan    — sub-queries returned by the planner
//	sources — deduped + ranked pool fed to the synthesizer
//	chunk   — per chat delta during synth
//	done    — final {"took": "..."}
//	error   — terminal; either plan, retrieval, or synth failed
func (s *pebbleHTTP) streamResearch(w http.ResponseWriter, r *http.Request, sc embed.StreamingChatClient, q string, k int, filt retrievalFilters, start time.Time) {
	sse := newAnswerSSE(w, start)
	if sse == nil {
		writeProblem(w, http.StatusInternalServerError, "streaming requires http.Flusher")
		return
	}
	// /research can sit on a single slow retrieve or chat call for
	// 10–30 s. Without a keepalive, the browser's HTTP/2 stream times
	// out as inactive and the request fails mid-pipeline. 7 s gives
	// generous headroom under Safari's ~30 s idle limit.
	stopKA := sse.startKeepalive(r.Context(), 7*time.Second)
	defer stopKA()
	includeText := r.URL.Query().Get("include_text") == "true"

	// surface silent no-ops upfront so SSE clients can render them
	// while the plan call is still in flight. Fires before plan to give the
	// fastest visual feedback when a misconfigured request arrives.
	sse.warnings(s.warningsFor(r))
	sse.phase("plan_start", map[string]any{"q": q, "model": sc.Model()})

	planRaw, err := s.doChat(r.Context(), sc, []embed.ChatMsg{
		{Role: "system", Content: researchPlanPrompt},
		{Role: "user", Content: q},
	})
	if err != nil {
		sse.send(map[string]any{"type": "error", "phase": "plan", "error": err.Error()})
		return
	}
	subs := parseSubQueries(planRaw, q)
	if len(subs) > 5 {
		subs = subs[:5]
	}
	// per-sub-query expansion via retrieveWithExpansion.
	// per-sub-query retriever dispatch (bm25 / dense / hybrid).
	// read retriever + wantRerank early so the plan event can
	// surface the full retriever label, not just expand.
	// Default to hybrid + rerank on /research streaming — same rationale as /answer.
	expandMode := r.URL.Query().Get("expand")
	retrieverParam, wantRerank := s.applyLLMEndpointDefaults(
		r.URL.Query().Get("retriever"),
		r.URL.Query().Get("rerank"),
	)
	denseReady := s.hnsw != nil && s.embedder != nil
	retrieverLabel := s.buildRetrieverLabel(retrieverParam, expandMode, denseReady, expandMode != "", wantRerank)

	// surface the active expansion strategy on the plan event so
	// UIs can render 'using paraphrase' or 'using HyDE' as soon as the plan
	// arrives, before per-sub-query expansion fires.
	planEvent := map[string]any{"type": "plan", "query": q, "plan": subs, "model": sc.Model()}
	if mode := normalizeExpandMode(expandMode); mode != "" {
		planEvent["expand"] = mode
	}
	if retrieverLabel != "" {
		planEvent["retriever"] = retrieverLabel
	}
	sse.send(planEvent)
	// Phase event mirrors /query's shape so the chat UI's timeline strip
	// renders identical rows across all three synth endpoints. Co-exists
	// with the legacy `type: plan` event above, which the CLI client at
	// cmd/cosift/main.go keys on.
	planPhase := map[string]any{"queries": subs, "retriever": retrieverLabel}
	if mode := normalizeExpandMode(expandMode); mode != "" {
		planPhase["expand"] = mode
	}
	sse.phase("plan", planPhase)

	type ranked struct {
		score float64
		hit   index.Hit
	}
	type cand struct {
		src        answerSource
		excerpt    string
		rerankText string
		score      float64
	}

	// Multi-pass research loop. Each pass: retrieve for the current subs →
	// fuse → materialize → rerank → mmr → synth → self-eval. Cumulative URL
	// set across passes; new passes skip URLs already seen so retrieval
	// widens rather than re-fetching the same docs. After synth, the model
	// judges whether its draft is sufficient. If not, we use its
	// refine_queries to drive the next pass. Cap at maxPasses to bound
	// latency + LLM cost.
	maxPasses := researchMaxPasses(r)
	decayHalfLife, decaySet := parseDecayHalfLife(r.URL.Query().Get("decay"))
	mmrLambda, mmrSet := parseMMRLambda(r.URL.Query().Get("mmr"))
	perSub := k * 2
	if perSub > 40 {
		perSub = 40
	}
	keepCap := k
	if wantRerank {
		keepCap = s.rerankCandK
		if keepCap < k {
			keepCap = k
		}
	}

	seenURLs := make(map[string]bool)
	allCands := make([]cand, 0, k*maxPasses)
	totalCandidates := 0 // counts URL contributions across passes for the sources event
	var lastAnswer string
	var finalRetrieverLabel string

	for pass := 1; pass <= maxPasses; pass++ {
		sse.phase("pass_start", map[string]any{"pass": pass, "queries": subs})

		// Retrieval round for this pass. Skip URLs we've already promoted.
		best := make(map[string]ranked, k*len(subs))
		for _, sq := range subs {
			// Emit `retrieving` BEFORE the s.retrieve call so the SSE
			// stream sends a frame every loop iteration even when the
			// retrieve itself takes 10-15s on a cold corpus shard. Without
			// this, the browser/proxy can see 10+ seconds of silence
			// between expand events and close the HTTP/2 stream as
			// inactive — cosift then sees context-canceled mid-retrieve.
			sse.phase("retrieving", map[string]any{"pass": pass, "query": sq})
			hits, _, rerr := s.retrieve(r.Context(), sq, perSub, retrieverParam, expandMode)
			if rerr != nil {
				log.Printf("pebble-serve: /research sub-query %q failed: %v", sq, rerr)
				sse.phase("expand", map[string]any{"pass": pass, "query": sq, "error": rerr.Error()})
				continue
			}
			for _, h := range hits {
				if seenURLs[h.URL] {
					continue
				}
				if prev, ok := best[h.URL]; !ok || h.Score > prev.score {
					best[h.URL] = ranked{score: h.Score, hit: h}
				}
			}
			sse.phase("expand", map[string]any{"pass": pass, "query": sq, "hits": len(hits)})
		}
		totalCandidates += len(best)
		pooled := make([]ranked, 0, len(best))
		for _, v := range best {
			pooled = append(pooled, v)
		}
		sort.Slice(pooled, func(i, j int) bool { return pooled[i].score > pooled[j].score })
		sse.phase("fuse", map[string]any{"pass": pass, "unique_urls": len(pooled)})
		if len(pooled) > keepCap {
			pooled = pooled[:keepCap]
		}

		// Materialize this pass's new candidates (filt-aware).
		newCands := make([]cand, 0, len(pooled))
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
			c := cand{src: src, excerpt: excerpt, score: p.score}
			if wantRerank {
				c.rerankText = doc.Title + "\n" + doc.Text
			}
			newCands = append(newCands, c)
		}
		sse.phase("materialize", map[string]any{"pass": pass, "candidates": len(newCands)})

		// Optional time-decay on this pass's new candidates.
		passLabel := retrieverLabel
		if decaySet && len(newCands) > 0 {
			now := time.Now()
			for i := range newCands {
				newCands[i].score *= decayMultiplier(newCands[i].src.PublishedAt, now, decayHalfLife)
			}
			sort.SliceStable(newCands, func(i, j int) bool { return newCands[i].score > newCands[j].score })
			passLabel += fmt.Sprintf("+decay:%gd", decayHalfLife)
		}
		if wantRerank && len(newCands) > 1 {
			rc := make([]rerank.Candidate, len(newCands))
			for i := range newCands {
				rc[i] = rerank.Candidate{ID: strconv.Itoa(i), Text: newCands[i].rerankText}
			}
			if order, rerr := s.doRerank(r.Context(), q, rc); rerr == nil && len(order) > 0 {
				reordered := make([]cand, 0, len(newCands))
				seenIdx := make(map[int]bool, len(newCands))
				for _, id := range order {
					if n, perr := strconv.Atoi(id); perr == nil && n >= 0 && n < len(newCands) && !seenIdx[n] {
						reordered = append(reordered, newCands[n])
						seenIdx[n] = true
					}
				}
				for i, c := range newCands {
					if !seenIdx[i] {
						reordered = append(reordered, c)
					}
				}
				newCands = reordered
			}
			sse.phase("rerank", map[string]any{"pass": pass, "reranker": s.reranker.Name(), "candidates": len(newCands)})
		}
		if mmrSet && len(newCands) > 1 {
			urls := make([]string, len(newCands))
			for i := range newCands {
				urls[i] = newCands[i].src.URL
			}
			if order := s.applyMMRPermutation(r.Context(), urls, q, mmrLambda); order != nil {
				reordered := make([]cand, len(order))
				for i, idx := range order {
					reordered[i] = newCands[idx]
				}
				newCands = reordered
				passLabel += fmt.Sprintf("+mmr:%.2f", mmrLambda)
				sse.phase("mmr", map[string]any{"pass": pass, "lambda": mmrLambda, "candidates": len(newCands)})
			}
		}
		if len(newCands) > k {
			newCands = newCands[:k]
		}
		// Promote into the cumulative pool.
		for _, c := range newCands {
			seenURLs[c.src.URL] = true
			allCands = append(allCands, c)
		}
		finalRetrieverLabel = passLabel

		// First-pass-empty path mirrors today's behavior: if pass 1 finds
		// no usable sources at all, bail with empty=true.
		if pass == 1 && len(allCands) == 0 {
			sse.send(map[string]any{"type": "done", "took": time.Since(start).String(), "empty": true})
			return
		}
		// Later-pass empty (no new sources added): synth would just rewrite
		// the prior answer with no new signal. Stop and keep lastAnswer.
		if pass > 1 && len(newCands) == 0 {
			break
		}

		// Build the cumulative sources slice + prompt block, stamping IDs
		// 1..N in the order URLs were promoted.
		cumulativeSources := make([]answerSource, 0, len(allCands))
		var promptSources strings.Builder
		for i, c := range allCands {
			c.src.ID = i + 1
			cumulativeSources = append(cumulativeSources, c.src)
			fmt.Fprintf(&promptSources, "[%d] %s\n%s\n%s\n\n", i+1, c.src.Title, c.src.URL, c.excerpt)
		}
		srcEvt := map[string]any{
			"type": "sources", "sources": cumulativeSources,
			"total_candidates": totalCandidates, "pass": pass,
		}
		if finalRetrieverLabel != "" {
			srcEvt["retriever"] = finalRetrieverLabel
		}
		sse.send(srcEvt)

		// Synth. Pass 1 uses the standard prompt; pass 2+ uses the refine
		// prompt with the prior draft injected.
		sse.phase("synth_start", map[string]any{
			"pass": pass, "sources": len(cumulativeSources), "model": sc.Model(),
		})
		var synthMsgs []embed.ChatMsg
		userMsg := "Sources:\n\n" + promptSources.String() + "Original question: " + q
		if pass == 1 {
			synthMsgs = []embed.ChatMsg{
				{Role: "system", Content: researchSynthPrompt},
				{Role: "user", Content: userMsg},
			}
		} else {
			synthMsgs = []embed.ChatMsg{
				{Role: "system", Content: researchRefineSynthPrompt},
				{Role: "user", Content: userMsg + "\n\nYour prior draft answer:\n" + lastAnswer},
			}
		}
		full, serr := s.doChatStream(r.Context(), sc, synthMsgs, sse.chunk)
		if serr != nil {
			sse.send(map[string]any{"type": "error", "phase": "synth", "error": serr.Error()})
			return
		}
		lastAnswer = full

		// On the final pass, skip self-eval — nothing to escalate to.
		if pass == maxPasses {
			break
		}

		// Self-evaluate. The model decides whether to escalate.
		sse.phase("self_eval_start", map[string]any{"pass": pass})
		evalUserMsg := fmt.Sprintf(
			"Question: %s\n\nYour answer:\n%s\n\nSources used (id — title — url):\n%s",
			q, lastAnswer, summarizeSourceList(cumulativeSources),
		)
		evalRaw, eerr := s.doChat(r.Context(), sc, []embed.ChatMsg{
			{Role: "system", Content: selfEvalPrompt},
			{Role: "user", Content: evalUserMsg},
		})
		if eerr != nil {
			log.Printf("pebble-serve: /research self-eval pass %d failed: %v", pass, eerr)
			sse.phase("self_eval", map[string]any{"pass": pass, "error": eerr.Error()})
			break
		}
		ev, ok := parseSelfEval(evalRaw)
		if !ok {
			sse.phase("self_eval", map[string]any{"pass": pass, "parse_failed": true})
			break
		}
		sse.phase("self_eval", map[string]any{
			"pass": pass, "sufficient": ev.Sufficient,
			"missing": ev.Missing, "refine_queries": ev.RefineQueries,
		})
		if ev.Sufficient {
			break
		}
		// Blurb summarizing why we're escalating; UI renders inline.
		blurbText := strings.TrimSpace(ev.Missing)
		if blurbText == "" {
			blurbText = "More depth needed"
		}
		blurbText += " — searching deeper."
		sse.phase("blurb", map[string]any{"pass": pass, "text": blurbText})

		// Drive the next pass with the refined queries the model proposed.
		if len(ev.RefineQueries) == 0 {
			break
		}
		subs = ev.RefineQueries
		if len(subs) > 5 {
			subs = subs[:5]
		}
	}

	sse.done()
}

// summarizeSourceList formats sources into compact "[id] title — url" lines
// for the self-eval user message. Avoids piping the full excerpts back into
// the eval prompt — the eval is about coverage, not re-reading.
func summarizeSourceList(srcs []answerSource) string {
	var sb strings.Builder
	for _, s := range srcs {
		fmt.Fprintf(&sb, "[%d] %s — %s\n", s.ID, s.Title, s.URL)
	}
	return sb.String()
}

// retrievalFilters bundles the four post-retrieval predicates /search,
// /answer, /find_similar, and /research all share. Centralized in
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
// ("2026-01-15", treated as UTC midnight).
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
// word boundary if one is available within the trailing 16 bytes.
// Static prefix (not query-aware) — matches the SQLite-side DocMeta.Excerpt
// shape; a query-aware passage extractor is the path on the index
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

// POST /contents — batch URL → document. Up to 100 URLs in
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
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
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
// openPebbleOrFriendlyErr wraps store.OpenPebble with a more
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

// runVerifyViaServer GETs pebble-serve's /verify endpoint and
// renders the JSON body in the same shape runVerifyPebble's local path emits.
// Lets `cosift verify -server URL` work against a running pebble-serve while
// the writer lock is held by the crawl / serve process.
func runVerifyViaServer(ctx context.Context, serverURL string, asJSON bool) error {
	endpoint := strings.TrimRight(serverURL, "/") + "/verify"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
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
// in so the shape can be unit-tested without spawning a subprocess.
// Mirrors the /stats response — same jq filters compose against either source.
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

// operator visibility into LSM levels, WAL state, on-disk
// size, and compaction queue, surfaced via pebble.Metrics().String().
func runPebbleInfo(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("pebble-info", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (defaults to <cfg.DataDir>/pebble)")
	// Mirrors
	// the shape /stats returns ( retrievers list,'s
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
	// pebble-info already needs
	// to read the store; reading them costs O(1) and gives operators 'is this
	// store healthy' without round-tripping through pebble-serve /stats.
	if indexedDocs > 0 {
		fmt.Printf("  indexed_docs: %d\n", indexedDocs)
		fmt.Printf("  sum_doc_len:  %d\n", sumLen)
		fmt.Printf("  avg_doc_len:  %.2f\n", float64(sumLen)/float64(indexedDocs))
	}
	// when an HNSW meta blob is persisted, surface dim+nodes here
	// too. Same cheap 20-byte read pebble-serve does at startup.
	// Operators running pebble-info to inspect an offline store get the dense
	// shape without opening pebble-serve.
	// bm25/bm25-mlt
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

// verify the running counters ('m'+indexed_docs,
// 'm'+sum_doc_len) against an authoritative scan of the 'l' family.
// Drift would mean a crash or bug left the counter inconsistent —
// flagged here so it can be re-derived from the scan.
func runVerifyPebble(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (defaults to <cfg.DataDir>/pebble)")
	// machine-readable output for CI integration.
	asJSON := fs.Bool("json", false, "emit JSON report instead of human text (suitable for jq / CI)")
	// -server URL routes the check through a running pebble-serve's
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
