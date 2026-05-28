// Package config loads cosift's JSON config file.
//
// JSON, not TOML/YAML, so we stay on the stdlib. The config file is small enough
// that the readability gain from TOML doesn't justify a dep.
package config

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
)

// Config is the top-level configuration. Every field has a sane default;
// the config file only needs to override what you care about.
type Config struct {
	DataDir string `json:"data_dir"`
	Server  Server `json:"server"`

	Crawler Crawler `json:"crawler"`

	// Federation: which external retrieval sources to consult. Each is optional.
	Federation Federation `json:"federation"`

	// Embeddings: address of an HTTP embedding service. Empty = vector search disabled.
	Embeddings Embeddings `json:"embeddings"`

	// Chat: address of an HTTP chat-completion service. Empty = /answer disabled.
	Chat Chat `json:"chat"`

	// Rerank: post-retrieval reranker config.
	Rerank Rerank `json:"rerank"`

	// Defaults: instance-wide retrieval defaults applied when a request omits
	// the relevant query parameter. Per-request params always override. Lets
	// operators tweak their cosift for a specific use case (a docs-search
	// instance might want `rerank=true` always; an exploratory research
	// instance might want `expand=true` and `research_strategy=paraphrase`)
	// without forcing every client to set the params on every URL.
	Defaults Defaults `json:"defaults"`

	// Cluster: optional sharding/scaling settings. When NumShards=1 (default)
	// and Peers is empty, cosift runs single-node — code paths are
	// identical to today. Set NumShards>1 + MyShardID + Peers to participate
	// in a multi-machine cluster. The crawler routes URLs by hash to the
	// owning shard (forwarding to peers via POST /admin/crawl-enqueue), and
	// /search fans out to peers and RRF-merges.
	Cluster Cluster `json:"cluster"`
}

// Cluster controls the horizontal-scaling shape. All fields are
// optional. The defaults run as a single-node cluster of size 1, identical
// to pre behavior — no code path diverges until NumShards>1.
type Cluster struct {
	// NumShards is the total number of shards the cluster maintains. Each
	// URL maps to a shard via fnv32(url) % NumShards. Default 1 = single
	// node. Once set in a multi-node deployment, this value must match
	// across every peer or routing breaks.
	NumShards int `json:"num_shards"`

	// MyShardID is the shard this process owns. Range [0, NumShards).
	// Default 0. The crawler accepts URLs whose hash lands on MyShardID
	// and forwards the rest to Peers.
	MyShardID int `json:"my_shard_id"`

	// Peers lists every shard's address as "host:port" indexed by shard
	// ID. peers[i] is the host serving shard i. peers[MyShardID] should
	// equal cfg.Server.Addr (or be empty — self-loop is skipped). Used by
	// the crawler to forward out-of-shard URLs and by the gateway to
	// fan out searches. Default empty = single-node.
	Peers []string `json:"peers"`

	// GatewayMode (default false): when true, /search /answer /research
	// fan out to every peer and RRF-merge the results. When false, those
	// endpoints only return data from MyShardID (read-only on this shard's
	// slice of the corpus). Typical deployment: one or two dedicated
	// gateway nodes have this on, leaf shards have it off.
	GatewayMode bool `json:"gateway_mode"`

	// PeerAuthToken is the shared secret presented as a Bearer token on
	// inter-peer calls (/admin/crawl-enqueue, gateway fan-out). Default
	// empty (no auth). Required when peers communicate over a public
	// network; can be left empty inside a private subnet.
	PeerAuthToken string `json:"peer_auth_token"`
}

// Server holds HTTP server settings (listen address, admin auth, proxy trust).
type Server struct {
	Addr string `json:"addr"`
	// AdminToken protects /admin/* endpoints with a Bearer token. Empty disables
	// the admin surface entirely (returns 403). For Docker, source this from
	// an env file rather than committing it.
	AdminToken string `json:"admin_token"`
	// TrustedProxies are CIDRs of reverse proxies whose X-Forwarded-For headers
	// the server will trust when extracting the client IP for rate limiting.
	// Empty (default) = trust nothing, use the direct TCP peer. Set this in
	// any deployment behind nginx/caddy/cloudflare/fly to make per-IP limits
	// see the real client, not the proxy.
	TrustedProxies []string `json:"trusted_proxies"`
}

// Crawler holds the politeness, concurrency, and discovery settings used
// when fetching pages and expanding the frontier.
type Crawler struct {
	// UserAgent sent on every request. Include a contact path.
	UserAgent string `json:"user_agent"`

	// MaxConcurrent in-flight requests across all hosts.
	MaxConcurrent int `json:"max_concurrent"`

	// PerHostDelay between requests to the same host (ms).
	PerHostDelayMs int `json:"per_host_delay_ms"`

	// PerHostOverrides maps host (exact match, including port if
	// non-default) to a delay-ms override. Lets operators crawl slow servers
	// politely while pushing fast servers quickly. Hosts NOT in the map use
	// PerHostDelayMs. Example:
	//   "per_host_overrides": {"slow-server.example.com": 5000, "fast-cdn.com": 50}
	PerHostOverrides map[string]int `json:"per_host_overrides,omitempty"`

	// MaxBodyBytes caps the response size. Bigger pages get truncated.
	MaxBodyBytes int64 `json:"max_body_bytes"`

	// PerHostMaxBodyBytes maps host (exact match) to a body-size
	// override. Useful when archiving large PDFs from one host (e.g.,
	// papers.example.com: 50 MB) but keeping small blog posts modest on
	// another. Hosts NOT in the map use MaxBodyBytes. Override can exceed
	// the default — operator's explicit opt-in. Example:
	//   "per_host_max_body_bytes": {"papers.example.com": 52428800, "blog.example.com": 1048576}
	PerHostMaxBodyBytes map[string]int64 `json:"per_host_max_body_bytes,omitempty"`

	// MaxDepth from each seed (0 = seeds only, no expansion).
	MaxDepth int `json:"max_depth"`

	// PerHostMaxDepth maps host (exact match) to a depth override.
	// Lets operators crawl their own site deeply (e.g., docs.example.com: 10)
	// while limiting outbound links to a shallow depth (default 1 or 2).
	// Hosts NOT in the map use MaxDepth. The override is the FULL cap for that
	// host — it can exceed or undercut the default. Example:
	//   "per_host_max_depth": {"docs.example.com": 10, "external.io": 1}
	PerHostMaxDepth map[string]int `json:"per_host_max_depth,omitempty"`

	// IncludeDomains restricts crawling to these domains (empty = any).
	IncludeDomains []string `json:"include_domains"`

	// ExcludeDomains is a blocklist.
	ExcludeDomains []string `json:"exclude_domains"`

	// ExcludeURLPatterns drops URLs whose full string contains
	// any listed substring. Finer-grained than ExcludeDomains — lets an
	// operator ban "bandcamp.com/album/" while still allowing the
	// "bandcamp.com/blog" path on the same host. Substring match (not
	// glob, not regex) for predictability and zero-dep simplicity; if
	// operators need full regex they can pre-filter at the seed list.
	// Matched against the URL string before fetch, so saves the HTTP
	// round-trip too. Examples:
	//   "exclude_url_patterns": ["/album/", "/profile", "?from=footer"]
	ExcludeURLPatterns []string `json:"exclude_url_patterns,omitempty"`

	// RespectRobots toggles robots.txt enforcement (default true).
	RespectRobots bool `json:"respect_robots"`

	// AutoSitemap — when true, the crawler fires a
	// fire-and-forget /sitemap.xml fetch the first time it sees a host.
	// Compounds URL discovery: every new host typically brings hundreds-
	// to-thousands of URLs in one batch. Each host is probed at most
	// once per process lifetime (in-memory cache). Default false.
	AutoSitemap bool `json:"auto_sitemap,omitempty"`

	// RemoteFetcherURL — optional Cloudflare-Worker (or any
	// HTTP service speaking the same shape) that fetches URLs on behalf
	// of the crawler. When set, cosift's HTTP transport is wrapped so
	// every outbound GET becomes a POST to this URL with {url, ...}
	// body; the worker fetches from its own egress IPs and returns the
	// upstream body. Lets a single cosift box crawl through CF's 300+
	// POPs without per-IP rate limiting on the target sites.
	//
	// See scripts/cf-fetch-worker.js for the worker code. Pair with
	// RemoteFetcherToken for the shared Bearer.
	RemoteFetcherURL   string `json:"remote_fetcher_url,omitempty"`
	RemoteFetcherToken string `json:"remote_fetcher_token,omitempty"`

	// RemoteFetcherURLs — optional pool of Worker URLs. When
	// set (non-empty), the crawler round-robins outbound fetches across
	// the pool instead of hammering a single Worker. Each Worker on
	// Cloudflare's free tier handles 100K req/day; spreading load across
	// 5-10 Workers eliminates the HTTP/2 ENHANCE_YOUR_CALM stream cap
	// that throttles a single deployment to ~2K docs/min.
	//
	// All Workers in the pool MUST share the same RemoteFetcherToken.
	// Falls back to RemoteFetcherURL when this is empty.
	RemoteFetcherURLs []string `json:"remote_fetcher_urls,omitempty"`

	// Proxies — optional list of HTTP proxy URLs (e.g.
	//   "http://user:pass@proxy.example.com:8080"
	//   "socks5://proxy.local:1080"
	// Per-request a proxy is picked at random; empty list = direct
	// connections. Useful for bypassing per-IP rate limits and basic
	// anti-bot walls. Go's http.Transport handles HTTP, HTTPS, and SOCKS5
	// proxy schemes natively (the latter via golang.org/x/net/proxy which
	// is stdlib-adjacent).
	Proxies []string `json:"proxies,omitempty"`

	// StatusFile — when non-empty, the crawler writes a JSON
	// snapshot of frontier counts to this path every 10s. Operators reading
	// the file (cat / watch / jq) don't contend with the Pebble single-
	// writer lock that blocks `cosift stats -backend=pebble` mid-crawl.
	// Empty string disables; recommended value: <data_dir>/crawl-status.json
	StatusFile string `json:"status_file,omitempty"`

	// MaxURLsPerHost caps how many URLs from any single host can be
	// queued in the frontier at once. New outbound-link enqueues for a host
	// already at the cap are silently skipped; the cap is checked against
	// frontier.host count for status='queued'.
	//
	// Without this cap, fanout-heavy hosts (github.com, en.wikipedia.org)
	// grow their queue unboundedly as link discovery follows internal links.
	// Observed: a 6,632-doc crawl had github.com at 18,646 queued URLs — the
	// host-fair scheduler distributes WORK across hosts but doesn't limit
	// how much each host can queue. Crawler ends up spending most workers
	// fetching same-host duplicates that all redirect to canonical doc URLs
	// already in the index (fetched_at bumps, no new docs).
	//
	// Zero / unset = no cap (pre behavior). Recommended starting
	// value: 1000 for general crawls; 100-200 for tight target-site crawls.
	MaxURLsPerHost int `json:"max_urls_per_host,omitempty"`

	// ChunkSize and ChunkOverlap control passage windowing for the
	// dense indexer during crawl. ChunkSize is the target word count per chunk
	// (~0.6 BPE tokens/word for English → 320 words ≈ 512 tokens). ChunkOverlap
	// is the shared word count between consecutive chunks; helps recall on
	// queries straddling chunk boundaries.
	//
	// Zero / unset → defaults from index.NewChunker() (320/64). Operators tuning
	// for jargon-heavy short queries may prefer smaller chunks (e.g., 128/32);
	// long-form retrieval may prefer larger (e.g., 512/128). Tune via the eval
	// gate on your corpus.
	ChunkSize    int `json:"chunk_size,omitempty"`
	ChunkOverlap int `json:"chunk_overlap,omitempty"`

	// PerHostChunkSize / PerHostChunkOverlap map host (exact match,
	// matched against the originally-requested URL's host — redirects don't
	// inherit) to a per-host chunker override. Lets operators chunk PDF-heavy
	// hosts (papers.example.com) at 512/128 while keeping social-feed hosts
	// (twitter.com) at 128/32 in the SAME deployment. Hosts NOT in the map use
	// the global ChunkSize / ChunkOverlap (and ultimately NewChunker defaults
	// if those are zero). Example:
	//   "per_host_chunk_size": {"papers.example.com": 512, "feed.x.com": 128}
	//   "per_host_chunk_overlap": {"papers.example.com": 128, "feed.x.com": 32}
	PerHostChunkSize    map[string]int `json:"per_host_chunk_size,omitempty"`
	PerHostChunkOverlap map[string]int `json:"per_host_chunk_overlap,omitempty"`

	// MinTextLen drops parsed pages whose extracted text falls
	// below this many characters before they reach the index. Aggregator
	// pages (commerce listings, sitemap stubs, "page not found", footer-
	// only auto-discovered URLs) routinely parse to <200 chars of real
	// content and inflate the corpus without adding signal — they
	// dominate retrieval cost while shadowing higher-quality long-form
	// pages in BM25 IDF.
	//
	// Zero / unset = keep every non-empty parse (pre behavior).
	// Recommended starting value: 200 for general crawls; raise to 500
	// or 1000 for research/news corpora where short pages are almost
	// always navigation cruft.
	MinTextLen int `json:"min_text_len,omitempty"`
}

// Federation configures upstream search backends used as no-key
// fallbacks when the local corpus is empty or partial.
type Federation struct {
	// DuckDuckGoHTML enables the no-key DDG HTML proxy fallback for /search.
	DuckDuckGoHTML bool `json:"duckduckgo_html"`

	// SearXNGURL points at a self-hosted SearXNG instance, if any.
	SearXNGURL string `json:"searxng_url"`

	// Pilot enables the cosift→Pilot specialist router (requires pilotctl).
	Pilot bool `json:"pilot"`
}

// Embeddings configures the dense-vector backend: model, dimension,
// and one or more OpenAI-compatible endpoints to fan out across.
type Embeddings struct {
	// URL of an OpenAI-compatible embeddings endpoint, or empty to disable.
	URL string `json:"url"`

	// URLs — optional list of backend endpoints for fan-out.
	// When non-empty takes precedence over URL: cosift round-robins
	// embed requests across the entries. Use for multi-replica ollama /
	// vLLM deployments where one model copy can't saturate the GPU.
	URLs []string `json:"urls,omitempty"`

	// Model name to pass to the embeddings endpoint.
	Model string `json:"model"`

	// Dim of the model output (must match the index).
	Dim int `json:"dim"`

	// CacheDir — optional directory for a SHA256-keyed disk
	// cache. Same text → instant cache hit, no ollama call. Useful when
	// the crawler re-fetches existing URLs (content_hash unchanged) or
	// when /search receives the same query repeatedly. Empty = disabled.
	// Cache entries are ~3 KB each (768 float32). Pick a fast disk.
	CacheDir string `json:"cache_dir,omitempty"`
}

// Chat selects the chat-completion model used by /answer (and later /research).
// Empty Model disables /answer.
type Chat struct {
	URL   string `json:"url"`   // empty → public OpenAI
	Model string `json:"model"` // e.g. "gpt-4o-mini" or local equivalent
}

// Rerank controls the post-retrieval reranker. Two backends:
//   - URL set → HTTPReranker (Cohere /v1/rerank shape: Cohere, Voyage, Jina, TEI).
//     Roughly $2/1k queries hosted, free self-hosted. The default for production.
//   - URL empty + Chat.Model set → LLMReranker (RankGPT-style listwise via chat).
//     Roughly $10-30/1k queries. The bootstrap path; useful before standing up
//     a specialized backend.
type Rerank struct {
	Enabled    bool   `json:"enabled"`     // default false; auto-enables when URL or Chat.Model is set
	URL        string `json:"url"`         // HTTP /v1/rerank endpoint; if set, takes precedence over LLM
	APIKey     string `json:"api_key"`     // optional bearer; falls back to env COHERE_API_KEY / VOYAGE_API_KEY
	Model      string `json:"model"`       // model id passed to the reranker; for LLM, chat model name
	CandidateK int    `json:"candidate_k"` // candidates pulled from inner retriever; default 20
}

// Defaults are instance-wide retrieval defaults. Empty string / false means
// "no default — use the handler's built-in fallback". Per-request query params
// always win when present.
//
// Rerank is intentionally NOT a default field: WithReranker already auto-enables
// rerank when a reranker is configured. To turn rerank off per-request, pass
// ?rerank=false. A three-state default (unset/on/off) would be the only clean
// way to express "rerank off by default with reranker available" — not worth
// the API complexity for the marginal use case.
type Defaults struct {
	// Retriever sets /search?retriever= when the request omits it.
	// Valid: "" (fall through to "bm25"), "bm25", "dense", "hybrid".
	Retriever string `json:"retriever"`

	// Expand sets /search and /answer's ?expand= when the request omits it.
	// Has effect only if WithParaphraser is configured. Per-request ?expand=false
	// disables on a per-call basis.
	Expand bool `json:"expand"`

	// ResearchStrategy sets /research?strategy= when the request omits it.
	// Valid: "" (use "planner"), "planner", "paraphrase". "paraphrase" requires
	// WithParaphraser configured; if it isn't, the handler returns 400 the
	// same way it does for an explicit ?strategy=paraphrase request.
	ResearchStrategy string `json:"research_strategy"`

	// ResearchSynthK caps how many sources /research passes to the synthesis
	// LLM. Defaults to 10 (preserves behavior). Lower values reduce
	// the chance of the synth LLM citing tangentially-relevant noise sources
	// that the retrieval pulled in at positions 6-10 — measured this
	// failure mode in 16/38 (42%) paraphrase queries on the single-doc corpus.
	// measured that K=5 trades a small coverage delta for a larger
	// grounding gain on single-doc workloads; multi-faceted workloads may
	// prefer the historic K=10 for fuller multi-doc coverage.
	// Zero = use the built-in default (10). Range: positive integers.
	ResearchSynthK int `json:"research_synth_k"`

	// ExpandMainWeight scales the main (non-paraphrased) query's contribution
	// in the expand-path RRF fusion. Paraphrase lists keep weight 1.0.
	// >1.0 trusts the original query more than LLM paraphrases (typical when
	// the corpus is well-aligned with the user's vocabulary); <1.0 trusts
	// paraphrases more (rare; useful when queries are short / under-specified
	// and the LLM reliably broadens them).
	//
	// Zero / unset → 1.0 (equal weight = standard RRF behavior, unchanged
	// from pre). Non-positive values fall back to 1.0 — defensive
	// default so a typo can't silently drop the main retriever (same
	// safer-on-conflict pattern as).
	//
	// Tuning is operator-specific: measure with the eval gate on your own
	// corpus before pinning to anything other than 1.0.
	ExpandMainWeight float64 `json:"expand_main_weight"`

	// HybridDenseWeight scales the dense retriever's contribution in the
	// /search?retriever=hybrid RRF fusion. BM25 keeps weight 1.0. >1.0 trusts
	// dense (good for paraphrase / semantic-match workloads on well-trained
	// embeddings); <1.0 trusts BM25 (good for entity / jargon / code-heavy
	// queries where embedding models often blur exact-match precision).
	//
	// Zero / non-positive → 1.0 (equal weight = standard RRF, unchanged from
	// pre). OpenSearch/ Vespa / Elastic all expose this knob;
	// cosift now matches.
	//
	// Tune via the eval gate on your corpus — there is no universal answer.
	// Note: composes orthogonally with ExpandMainWeight when both expand
	// AND hybrid are active.
	HybridDenseWeight float64 `json:"hybrid_dense_weight"`

	// HostBoosts is a host-suffix → score multiplier map applied
	// to fused retrieval scores. Operators upweight authoritative sources
	// (e.g. "wikipedia.org": 1.5, "arxiv.org": 1.5, "nih.gov": 1.4) or
	// downweight long-tail aggregators ("bandcamp.com": 0.4, "itch.io":
	// 0.5) without rebuilding the index — the boost runs at query time
	// post-fusion.
	//
	// Suffix matching is longest-match-wins, so "blog.example.com": 0.7
	// composes cleanly with "example.com": 1.2. Default multiplier when
	// nothing matches is 1.0 — empty map (default) is a no-op and keeps
	// retrieval on the unboosted fast path.
	//
	// Applied by /query (the LLM-orchestrated endpoint). Per-request
	// override at retrieval API surfaces is left to operators via standard
	// JSON config reload.
	HostBoosts map[string]float64 `json:"host_boosts,omitempty"`
}

// Default returns sensible defaults for a fresh install.
func Default() *Config {
	return &Config{
		DataDir: "./cosift-data",
		Server: Server{
			Addr: "127.0.0.1:7777",
		},
		Crawler: Crawler{
			UserAgent:      "CosiftBot/0.0 (+https://github.com/pilot-protocol/cosift)",
			MaxConcurrent:  8,
			PerHostDelayMs: 1000,
			MaxBodyBytes:   5 << 20, // 5 MB
			MaxDepth:       2,
			RespectRobots:  true,
		},
		Federation: Federation{},
		Embeddings: Embeddings{},
	}
}

// Load reads config from path, applying defaults for any unset field.
// If the file is missing, returns defaults (no error). Also best-effort loads
// `./.env` so OPENAI_API_KEY (and friends) populate os.Getenv without a dep.
//
// Env overrides apply in BOTH the "file exists" and "file missing" paths —
// container deploys (Cloud Run, Fly, Heroku) typically don't ship a JSON
// config but DO inject PORT etc.
func Load(path string) (*Config, error) {
	_ = LoadDotEnv(".env") // best-effort; missing file is fine
	cfg := Default()
	b, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		// fall through with defaults
	} else {
		if err := json.Unmarshal(b, cfg); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
	}
	if cfg.DataDir == "" {
		cfg.DataDir = Default().DataDir
	}
	applyEnvOverrides(cfg)
	return cfg, nil
}

// applyEnvOverrides applies 12-factor env-var overrides on top of cfg.
// Cloud Run / Fly / Heroku / Render set PORT and expect the binary to bind
// on 0.0.0.0:$PORT. COSIFT_LISTEN is the full-address escape hatch.
func applyEnvOverrides(cfg *Config) {
	if port := strings.TrimSpace(os.Getenv("PORT")); port != "" {
		cfg.Server.Addr = "0.0.0.0:" + port
	}
	if addr := strings.TrimSpace(os.Getenv("COSIFT_LISTEN")); addr != "" {
		cfg.Server.Addr = addr
	}
	if dir := strings.TrimSpace(os.Getenv("COSIFT_DATA_DIR")); dir != "" {
		cfg.DataDir = dir
	}
}

// LoadDotEnv reads a `.env` file and sets variables in the process environment
// IF they are not already set. Format: one KEY=VALUE per line, # for comments,
// optional surrounding single or double quotes on VALUE. Returns nil if the
// file does not exist. ~30 LOC — avoids the godotenv dependency.
func LoadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.Index(line, "=")
		if eq < 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		if len(val) >= 2 {
			first, last := val[0], val[len(val)-1]
			if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}
	return sc.Err()
}
