# Cosift

A self-hostable HTTP API for **searching, retrieving, and researching** content you've indexed. A single Go binary handles the whole loop: it crawls a set of URLs you point it at, builds an inverted-index and optional dense-vector store of what it finds, and exposes search + retrieval + LLM-grounded synthesis over plain HTTP.

Designed to run on a small VPS, a laptop, or a container. Three dependencies, no cgo. SQLite for storage. Any OpenAI-compatible LLM/embedding provider — including local ones (vLLM, llama.cpp, text-embeddings-inference) — plugs in via HTTP.

```
                  ┌─────────────────────────────────────┐
seed URLs  ───▶  │  crawler  →  index  →  retriever     │  ───▶  HTTP API
                 │  (BM25 + optional dense + rerank)    │
                 └─────────────────────────────────────┘
                                       │
                                       ▼
                              /search   /find_similar
                              /contents /answer /research
```

## Quick start

```bash
# 1. Build
go build -o cosift ./cmd/cosift

# 2. Write a default config (refuses to overwrite without -force)
./cosift init                                 # generic
./cosift init -site https://docs.example.com  # pre-populates include_domains

# 3. (optional) put an OpenAI-compatible key in .env to enable dense
#    retrieval, /answer, /research, paraphrase expansion, HyDE.
echo 'OPENAI_API_KEY=sk-...' > .env

# 4. Index some content. Either crawl URLs, or ingest a curated corpus.
./cosift crawl https://example.com/some-page https://example.com/another
./cosift ingest -corpus testdata/eval/corpus.json

# 5. Serve the API
./cosift serve
```

Once `serve` is up:

```bash
curl 'http://127.0.0.1:7777/search?q=concurrent+programming'
curl 'http://127.0.0.1:7777/search?q=concurrent+programming&retriever=hybrid&rerank=true'
curl 'http://127.0.0.1:7777/answer?q=what+is+raft+consensus'
curl -N -H 'Accept: text/event-stream' \
     'http://127.0.0.1:7777/research?q=compare+go+and+rust+for+systems+programming'
```

Run `./cosift doctor` for a local config sanity check (data dir writable, schema migrated, key present where features need it). Run `make smoke` for an end-to-end real-runner check that crawls a public seed, exercises both CLI subcommands and HTTP endpoints, and reports in ~30 seconds.

## What's in the box

| Layer | What it does |
|-------|--------------|
| **Crawler** | Goroutine pool, per-host gate, robots.txt + Crawl-delay, sitemap.xml + sitemap index seeding, conditional GET (ETag / If-Modified-Since), content-hash dedup, retries with stored `last_error`, optional PDF parsing |
| **Index** | BM25 inverted index (k1=1.2, b=0.75) over SQLite postings. Optional dense vector index for OpenAI-compatible embeddings (`text-embedding-3-small`, BGE, GTE, any HTTP-shaped endpoint). |
| **Retrieval** | BM25 / dense / hybrid (RRF), with optional cross-encoder rerank (Cohere-shape HTTP or LLM listwise). MMR diversification, pseudo-relevance feedback, HyDE, paraphrase + RRF query expansion. All knobs compose. |
| **Synthesis** | `/answer` does one-shot RAG with numeric citations. `/research` does bounded multi-step research (planner or paraphrase strategy) with SSE token streaming. Every claim cites a source by integer id. |
| **Storage** | One SQLite database in WAL mode. Resumable crawl frontier, content table, postings, optional passage vectors, query outcomes for calibration, paraphrase + HyDE caches. |
| **Operations** | Bearer-token admin endpoints, Prometheus metrics, static-HTML dashboard, per-IP rate limiting on LLM endpoints, XFF with optional trusted-proxy allowlist. |
| **Eval** | Built-in retrieval eval (`recall@K`, `MRR`, `nDCG@10`) and LLM-judged answer-quality eval. Diffable JSON reports. |

## HTTP endpoints

| Endpoint | Method | What |
|----------|--------|------|
| `/search` | GET | Lexical / semantic / hybrid search, optional rerank, optional paraphrase expansion, optional date / domain / author filters |
| `/find_similar` | GET | k-nearest-neighbor over an already-indexed URL's embedding |
| `/contents` | GET / POST | GET `?url=` for one doc; POST `{urls:[…]}` for up to 100 in one round-trip. Returns cleaned text from the index; on-demand fetch falls back when a URL isn't indexed |
| `/answer` | GET | Single-question grounded answer with cited sources. SSE streaming via `?stream=true` |
| `/research` | GET | Bounded multi-step research (planner or paraphrase strategy). SSE streaming via `?stream=true` |
| `/feedback` | POST | Record a single retrieval outcome for offline calibration |
| `/stats` | GET | Document / term / frontier counts |
| `/healthz` | GET | Liveness |
| `/metrics` | GET | Prometheus text format |
| `/dashboard` | GET | Static HTML operator dashboard (admin token entered client-side) |
| `/sitemap.xml` | GET | Sitemap.org-format index of crawled URLs (capped at 50k) |
| `/robots.txt` | GET | Crawler policy: allow `/`, disallow `/admin/*`, advertise `/sitemap.xml` |
| `/admin/stats` | GET | Full operator stats (auth: `Authorization: Bearer <token>`) |
| `/admin/config` | GET | Resolved retrieval defaults + capability flags |
| `/admin/recrawl` | POST | Enqueue specific URLs into the crawl frontier |
| `/admin/recrawl-by-domain` | POST | Bulk recrawl matching a domain pattern |
| `/admin/reembed` | POST | Re-embed every doc with the configured model (SSE-streamed progress) |

## Configuration (`cosift.json`)

The config file lives in the working directory by default; override with `-config <path>`. `cosift init` writes a sensible default.

```json
{
  "data_dir": "./cosift-data",
  "server": {
    "addr": "127.0.0.1:7777",
    "admin_token": "set-me",
    "trusted_proxies": ["10.0.0.0/8"]
  },
  "crawler": {
    "user_agent": "Cosift/0.0",
    "max_concurrent": 8,
    "per_host_delay_ms": 1000,
    "per_host_overrides": {
      "slow-server.example.com": 5000,
      "fast-cdn.example.com": 50
    },
    "max_body_bytes": 5242880,
    "max_depth": 3,
    "respect_robots": true,
    "include_domains": ["docs.example.com"],
    "max_urls_per_host": 1000,
    "chunk_size": 320,
    "chunk_overlap": 64
  },
  "embeddings": {
    "model": "text-embedding-3-small",
    "url": "",
    "dim": 1536
  },
  "chat": {
    "model": "gpt-4o-mini",
    "url": ""
  },
  "rerank": {
    "url": "",
    "model": "rerank-english-v3.0"
  },
  "defaults": {
    "retriever": "hybrid",
    "expand": true,
    "research_strategy": "paraphrase",
    "research_synth_k": 0,
    "expand_main_weight": 2.0,
    "hybrid_dense_weight": 1.5
  }
}
```

`embeddings.url`, `chat.url`, and `rerank.url` all default to OpenAI's endpoints when empty. Point them at a local vLLM / llama.cpp / text-embeddings-inference / Ollama server for fully self-hosted operation; the wire protocol is OpenAI-compatible on the receiving end.

### Tweakable defaults per instance

Most retrieval knobs (`retriever`, `expand`, `research_strategy`, `research_synth_k`) are per-request query parameters. For an instance that should *always* default to a particular setup — for example a docs-search deployment that wants hybrid retrieval with paraphrase expansion on every call — set them once in `defaults`.

| field | effect | values |
|-------|--------|--------|
| `retriever` | `/search` falls back to this when `?retriever=` is absent | `""` (→ `bm25`), `bm25`, `dense`, `hybrid` |
| `expand` | `/search` and `/answer` enable LLM paraphrase expansion when `?expand=` is absent | `false`, `true` |
| `research_strategy` | `/research` picks this when `?strategy=` is absent | `""` (→ `planner`), `planner`, `paraphrase` |
| `research_synth_k` | how many sources `/research` passes to the synthesis LLM | `0` (→ default 10), any positive int |
| `expand_main_weight` | weight of the main query vs paraphrases in `?expand=true` RRF fusion | `0` (→ equal-weight), any positive float |
| `hybrid_dense_weight` | weight of dense vs BM25 in `?retriever=hybrid` RRF fusion | `0` (→ equal-weight), any positive float |

Per-request query params always override the defaults. A caller can pass `?expand=false` against an `expand=true` instance to opt out, or `?strategy=planner` against a `paraphrase` instance to compare.

### Per-host crawl tuning

`crawler.per_host_delay_ms` is the default delay between requests to the same host. Override it per host via `per_host_overrides`:

```json
{
  "crawler": {
    "per_host_delay_ms": 200,
    "per_host_overrides": {
      "slow-server.example.com": 5000,
      "fast-cdn.example.com": 50
    }
  }
}
```

The same override map applies to `crawler.max_depth`, `crawler.max_body_bytes`, `crawler.chunk_size`, and `crawler.chunk_overlap` via parallel `<knob>_overrides` maps. Crawl-time `include_domains` keeps unwanted hosts out of the index entirely; search-time `?include_domains=` / `?exclude_domains=` filters an indexed mixed-domain corpus per-query.

## CLI reference

```text
cosift init [-site URL] [-force]              write a default cosift.json
cosift serve                                  run the HTTP API
cosift crawl <urls...> [-backend sqlite|pebble] [-duration 30m]
                                              seed the persistent frontier and crawl. -backend
                                              selects storage: sqlite (default) or pebble (LSM-tree;
                                              scales past SQLite's million-row ceiling). -duration
                                              (iter 223) caps wall time for bounded runs; default 0
                                              runs until frontier empty or SIGTERM
cosift crawl -sitemap https://x/sitemap.xml   seed from a sitemap (urlset or index)
cosift crawl -refresh <urls...>               force re-crawl of URLs already in the frontier
cosift check-robots [-user-agent UA] <urls>   report robots.txt allow/deny for each URL
cosift crawl-errors [-limit N]                list recently-errored frontier URLs + reason
cosift refresh-due [-interval 1h]             re-enqueue URLs whose adaptive interval elapsed (use -interval for daemon mode)
cosift ingest -corpus PATH [-format auto|json|jsonl] [-progress 5s]
                                              ingest a pre-built corpus into the index
cosift export [-output PATH] [-format json|jsonl|text|md] [-limit N] [-include-domains CSV]
              [-exclude-domains CSV] [-since DATE] [-until DATE]
                                              dump the index. json round-trips with ingest
cosift migrate-to-pebble -output DIR [-progress 5s]
                                              copy a SQLite cosift data dir into a fresh Pebble store.
                                              Documents + postings (re-indexed via PebbleBM25 to
                                              preserve title boost). Refuses non-empty -output
cosift pebble-serve -dir DIR [-addr HOST:PORT]
                                              HTTP server backed by PebbleStore + PebbleBM25. Endpoints:
                                              /healthz /stats /metrics /verify /contents
                                              /search /find_similar (BM25 + optional rerank + HyDE expand)
                                              /answer /research (sync + SSE; opt-in via cfg.Chat.Model)
                                              Companion to cosift serve (SQLite-backed) — pick whichever
                                              storage backend fits the deployment scale
cosift reembed [-drop-old] [-progress 5s]     re-embed every doc with the configured model
cosift outcomes -format json|csv              dump query_outcomes for offline calibration
cosift gc [-min-attempts N] [-vacuum]         drop errored frontier rows and VACUUM
cosift compact-index [-vacuum]                drop orphan passages + stale terms, VACUUM
cosift doctor [-server URL] [-token TOKEN]    health check (local; -server adds remote check)
cosift query <text> [-k N] [-json]            one-shot BM25 query against the local index
cosift search <text> [-server URL] [-k N] [-retriever ...] [-rerank] [-expand]
              [-since DATE] [-until DATE] [-include-domains CSV] [-exclude-domains CSV]
              [-sort ...] [-format text|markdown] [-json]
                                              hit a running server's /search with the full pipeline
cosift answer <text> [-server URL] [-k N] [-expand] [-stream] [-format text|markdown] [-json]
                                              hit /answer (single-question grounded answer)
cosift research <text> [-server URL] [-strategy planner|paraphrase] [-stream]
              [-format text|markdown] [-json]
                                              hit /research (multi-step research with citations)
cosift find-similar <url> [-server URL] [-k N] [-format text|markdown] [-json]
                                              hit /find_similar (dense neighbors of an indexed URL)
cosift contents <url...> [-server URL] [-file PATH] [-text] [-json]
                                              hit /contents — single GET or batch POST
cosift admin <stats|config|recrawl|recrawl-domain|reembed> [-server URL] [-token TOKEN] [-json]
                                              admin-protected operator endpoints
cosift stats [-backend sqlite|pebble]         doc / term counts + data dir (per-backend)
cosift crawl-status [-hosts N] [-errors N] [-target N]
                                              live operator snapshot of an ongoing crawl: counts, frontier
                                              breakdown, top hosts, top error classes, 5/15/30-min doc
                                              rates, ETA to -target docs (default 1M). Safe to run
                                              alongside an active `cosift crawl` (SQLite WAL allows
                                              readers + writer concurrently)
cosift eval [-retriever ...] [-rerank] [-api URL]
                                              run the eval set against the local or remote index
cosift answer-eval [-corpus PATH] [-queries PATH] [-save PATH]
                                              LLM-judged answer-quality eval
cosift answer-eval-compare A.json B.json [-query-threshold N]
                                              diff two saved answer-eval reports
cosift bench [-n N -mode vector|bm25|crawl|all] [-per-host-delay MS] [-json]
                                              latency + crawler-throughput micro-benchmarks
cosift bench-compare A.json B.json             diff two saved bench reports
```

Every operation that mutates the index is available both as a CLI command and as an `/admin/*` HTTP endpoint, so an operator can pick the side that fits their automation.

## Retrieval pipeline

The retrieval pipeline has eight composable stages. Each is independent — pick whichever combination matches your use case and measure with `cosift eval`.

| Stage | Knob | Effect |
|-------|------|--------|
| 1. Lexical retrieval | `?retriever=bm25` | Custom BM25 (k1=1.2, b=0.75); SQLite postings; title-boost ×3; phrase queries via `"…"` |
| 2. Dense retrieval | `?retriever=dense` | Brute-force cosine over OpenAI-compatible embeddings |
| 3. Hybrid fusion | `?retriever=hybrid&hybrid_dense_weight=N` | RRF over BM25 + dense, with per-retriever weight |
| 4. HyDE | `?hyde=true` | LLM generates a hypothetical-answer passage; embed THAT instead of the raw query (dense / hybrid only). 2-level cached |
| 5. Paraphrase expansion | `?expand=true&expand_main_weight=N` | LLM paraphrase + RRF fusion; main-query weight tunable; 2-level cached |
| 6. Pseudo-relevance feedback | `?prf=true&prf_terms=5&prf_docs=10` | Mine top hits for distinctive terms, re-search (BM25 + hybrid) |
| 7. MMR diversification | `?mmr=true&mmr_lambda=0.7` | Maximal Marginal Relevance over dense vectors; reduces near-duplicate top-k |
| 8. Cross-encoder rerank | `?rerank=true` | LLM listwise or HTTP `/v1/rerank` (Cohere / Voyage / Jina / TEI) |

These compose orthogonally. For example:

```bash
curl 'http://127.0.0.1:7777/search?q=raft+consensus
        &retriever=hybrid&hybrid_dense_weight=2
        &hyde=true
        &expand=true&expand_main_weight=3
        &prf=true
        &mmr=true&mmr_lambda=0.7
        &rerank=true
        &k=10'
```

flows through HyDE → hybrid fusion → paraphrase fusion → PRF augmentation → MMR diversity → cross-encoder rerank. The `source` tag on each returned hit shows which stages fired (e.g. `hybrid+rerank+expand+hyde+mmr(lambda=0.70)+prf(3)`).

Empirical baseline (38 queries × 20 docs, the committed eval set):

| Retriever | R@1 | R@3 | R@10 | MRR | nDCG@10 |
|-----------|-----|-----|------|-----|---------|
| BM25 | 0.908 | 0.952 | 0.965 | 0.961 | 0.958 |
| Dense | 0.868 | 0.978 | 1.000 | 0.961 | 0.968 |
| Dense + rerank | 0.921 | 0.991 | 1.000 | 0.987 | 0.990 |

Run `make eval-dense` against your own corpus to measure your own stack.

### Response shape

Each `/search` hit:

```json
{
  "url": "https://docs.example.com/api",
  "title": "API Documentation",
  "score": 0.87,
  "source": "bm25+rerank",
  "domain": "docs.example.com",
  "published_at": "2024-06-15T12:00:00Z",
  "author": "Jane Doe",
  "image": "https://cdn.example.com/api-cover.jpg",
  "favicon": "https://docs.example.com/favicon.ico",
  "highlight": { "offset": 120, "length": 80, "text": "..." },
  "excerpt": "The API documentation describes endpoints for..."
}
```

`domain`, `published_at`, `author`, `image`, `favicon` are populated from the index — no extra `/contents` round-trip needed. `highlight` is the dense / hybrid retriever's precision-aligned passage span; `excerpt` is a body-prefix fallback for BM25-only hits. All enrichment fields are `omitempty` — callers see only fields with meaningful values.

Pass `?include_text=true` to inline the full document body in each hit (capped by `?max_text=N`, default 5000 chars). Saves a `/contents` round-trip for one-shot research pipelines; opt-in because it can balloon the response.

Pass `?calibrate=true` to populate a `score_calibrated` field per hit: top hit = 1.0, others as `score / max(score)`. Within-response normalization, comparable across retrievers within one response. The same flag also works on `/answer` and `/research`, where it calibrates the `sources[]` array.

### Filtering

| Filter | What it does |
|--------|--------------|
| `?since=DATE&until=DATE` | Range filter on `documents.published_at` (extracted from JSON-LD `datePublished`). `DATE` is `YYYY-MM-DD` or RFC3339. Docs without a known publication date are excluded when any date filter is active |
| `?sort=date_desc` / `?sort=date_asc` | Chronological order instead of relevance. Un-dated docs sort to the end regardless of direction |
| `?include_domains=a.com,b.org` / `?exclude_domains=spam.com` | Suffix-on-dot-boundary match. `example.com` matches `blog.example.com` but NOT `evilexample.com` |
| `?author=jane,john` / `?exclude_author=spam` | Case-insensitive substring match against `documents.author` (JSON-LD `author.name`) |

## Self-hosting

### Docker

```bash
make docker             # builds cosift:<git-describe> and cosift:latest
docker run -p 7777:7777 -v cosift-data:/data \
  -e OPENAI_API_KEY=sk-... \
  cosift:latest
```

A `docker-compose.yml` is included with the API server + a refresh-due sidecar that re-enqueues URLs on their adaptive interval. Shared volume for the SQLite database.

### Cloud Run / Fly / Heroku-class platforms

The binary listens on `PORT` when set (falls back to `server.addr`). A persistent volume (or a sidecar that mounts one) keeps the SQLite WAL between deploys. No external state — pointing a fresh instance at the same data dir resumes the crawl frontier and the index.

### Pebble storage backend (scale option)

For deployments past the low-million document range, cosift ships a Pebble (pure-Go LSM-tree) backend in addition to the default SQLite store. The Pebble path supports the same crawler + BM25 + dense (HNSW) features as SQLite, with substantially higher write throughput at scale.

```bash
# Crawl into a Pebble-backed store (lives in cfg.DataDir/pebble alongside SQLite)
cosift crawl --backend=pebble https://docs.example.com

# Stats for either backend
cosift stats --backend=sqlite
cosift stats --backend=pebble

# Migrate an existing SQLite store to Pebble
cosift migrate-to-pebble -output /var/lib/cosift/data/pebble

# Serve HTTP against a Pebble store.
# Endpoints: /healthz /stats /metrics /verify /contents /search /find_similar,
# plus /answer + /research (sync or SSE) when cfg.Chat.Model is set.
# All retrieval endpoints support include_domains / exclude_domains / since /
# until / rerank; /search additionally supports sort + HyDE expand.
cosift pebble-serve -dir /var/lib/cosift/data/pebble
```

For programmatic use, wire dense indexing through `index.NewHNSWWriter(hnsw, pebbleStore, persistEvery)` and pass it via `crawler.WithPassageWriter(...)`.

Bench `cosift bench -mode storage -n N -queries K` runs both backends head-to-head on synthetic data, emitting per-backend p50/p95/p99 latency and QPS.

**Resource sizing for crawls.** Pebble's write path is memory-hungry under sustained crawl load: each indexed document stages thousands of postings + term-info updates in a single Pebble batch, and at high concurrency batches stack faster than the LSM can flush. On a 16 GB VM with `max_concurrent: 16`, the crawler hits OOM in a few minutes against typical Wikipedia-sized pages. Mitigations:

- `COSIFT_PEBBLE_CACHE_MB=64 COSIFT_PEBBLE_MEMTABLES=2 ...` to tighten Pebble's memory ceiling (defaults are 128 MB / 2 memtables → ~192 MB Pebble; tighten further on small VMs).
- `COSIFT_PEBBLE_SYNC=false ...` to skip fsync per commit. Trades VM-crash WAL durability for an order-of-magnitude commit-latency drop. Acceptable for crawl workloads because the frontier resumes cleanly on restart.
- Reduce `crawler.max_concurrent` (8 or less on a 16 GB VM under Pebble).
- Reduce `crawler.max_body_bytes` (2 MB default is generous; 512 KB cuts per-page batch volume by 4x).
- Or scale up: 32 GB VM gives substantial headroom at the same concurrency.

The crawler also has per-worker `defer recover()` so an isolated panic in one worker logs the stack and lets siblings continue rather than silently exiting the whole process.

### Behind a reverse proxy

Set `server.trusted_proxies` to the CIDR(s) your proxy presents from. The per-IP rate limiter (LLM-cost endpoints) and `/feedback` audit then use `X-Forwarded-For`'s left-most untrusted hop. Malformed CIDR config fails loud at startup — there's no silent fallback to "trust all."

### Admin endpoints

Set `server.admin_token` in `cosift.json` to enable `/admin/*`. All requests need `Authorization: Bearer <token>`. When the token is unset, `/admin/*` returns 403 unconditionally — there's no "missing config" silent-open footgun.

```bash
# Force-recrawl specific URLs (frontier mutation only; refresh-due picks them up)
curl -X POST -H "Authorization: Bearer $COSIFT_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"urls": ["https://example.com/a", "https://example.com/b"]}' \
  http://127.0.0.1:7777/admin/recrawl

# Bulk recrawl every doc from a domain (suffix-on-dot-boundary)
curl -X POST -H "Authorization: Bearer $COSIFT_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"domain": "example.com"}' \
  http://127.0.0.1:7777/admin/recrawl-by-domain

# Re-embed all docs with the currently-configured model (SSE-streamed progress)
curl -N -X POST -H "Authorization: Bearer $COSIFT_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"drop_old": false, "since": "2025-01-01"}' \
  http://127.0.0.1:7777/admin/reembed
```

`/admin/recrawl` deliberately doesn't run the crawler in-band — it just sets `status='queued'` on the frontier. The refresh-due daemon (or `cosift crawl -refresh`) processes them. Keeps the endpoint stateless and fast.

### Dashboard

`GET /dashboard` serves a static HTML page (no framework, no external resources). It prompts for the admin token (stored in `localStorage`) and then polls `/admin/stats` on a 30-second refresh, rendering counts, capability flags, frontier breakdown, top domains, and LLM-cache sizes.

### Metrics

`GET /metrics` exposes the standard Prometheus text format:

```text
cosift_requests_total{path="..."}                       counter, by path
cosift_rate_limit_denied_total                          counter
cosift_request_duration_seconds_bucket{path,le="..."}  histogram
cosift_request_duration_seconds_sum{path}               histogram sum
cosift_request_duration_seconds_count{path}             histogram count
cosift_info{version, embedder, chat, reranker,          gauge always 1
            dense_enabled, answer_enabled,
            admin_enabled, trusted_xff}
```

Useful Prometheus queries:

```promql
# p95 search latency by build version
histogram_quantile(0.95,
  sum by (le) (rate(cosift_request_duration_seconds_bucket{path="/search"}[5m])))
  * on (instance) group_left (version) cosift_info

# Request rate by endpoint
sum by (path) (rate(cosift_requests_total[1m]))

# Rate-limit denials per minute
rate(cosift_rate_limit_denied_total[1m])
```

## Evaluation

```bash
cosift eval -retriever bm25                          # lexical baseline
cosift eval -retriever dense                         # dense (needs an embedder)
cosift eval -retriever dense -rerank                 # + reranker
cosift eval -retriever dense -rerank -save mine.json # save report
cosift eval -api https://cosift.example.com         # measure a deployed instance
```

Pass `-save PATH` to write a JSON report; pass `-baseline PATH` on a later run to print a diff. The harness drives `/search` if `-api` is set (no local index needed) or builds an in-process server otherwise.

For LLM-judged answer quality:

```bash
cosift answer-eval -corpus my-corpus.json -queries my-queries.json -save /tmp/run.json
cosift answer-eval-compare /tmp/before.json /tmp/after.json
```

The judge is invoked once per `(query, strategy)` pair. Use `-judge-model gpt-4o` (default) with a smaller `-synth-model gpt-4o-mini` to avoid self-preference bias.

## End-to-end smoke test (`make smoke`)

`cosift doctor` is local config sanity — no network, no LLM, no real fetch. For a real-runner E2E check, run `make smoke`. It builds the binary, crawls a public seed (default `https://go.dev/doc/effective_go`, 30s timeout), and exercises both operator subcommands and HTTP endpoints in ~30 seconds.

```bash
make smoke
# ✓ binary at /tmp/.../cosift
# ✓ check-robots returned status line
# ✓ no SQLite lock errors in crawl log
# ✓ stats shows 16 documents
# ✓ ingest added 3 docs (total now 19)
# ✓ /healthz 200
# ✓ /search returned 1 hits
# ✓ /contents returned doc payload
# ✓ /admin/stats schema has documents + frontier + paraphrases + hyde_cache
# ✓ /admin/stats requires bearer (401 without)
```

Override with `COSIFT_SMOKE_SEED=<url>`, `COSIFT_SMOKE_TIMEOUT=<seconds>`, `COSIFT_SMOKE_PORT=<port>`. The smoke test does NOT exercise `/answer`, `/research`, `/find_similar`, or `?hyde=` — those need an LLM key and are gated to a separate (deferred) `make smoke-test-llm` target.

## Feedback for calibration

`POST /feedback` (public, no auth, to minimize friction) records a single retrieval outcome:

```bash
curl -X POST -H 'Content-Type: application/json' \
  -d '{"query":"raft consensus","url":"https://x/distributed",
       "score":0.87,"useful":true,"source":"thumbs"}' \
  http://127.0.0.1:7777/feedback
```

Outcomes accumulate in `query_outcomes`. Once roughly 10k entries with both classes have accrued, dump them with `cosift outcomes -format csv` and fit a calibration model offline. The `"calibrated": false` field on `/answer` and `/research` becomes truthful only after such a model is wired in. Within-response normalization (`?calibrate=true`) covers most callers without needing a fitted model.

## Architecture

```
                    ┌──────────────┐
        seeds       │  Frontier    │ ◀── outbound links from parser
        sitemap.xml │  (SQLite,    │
        recrawl-due │   resumable) │
                    └──────┬───────┘
                           │ ClaimFrontier (atomic UPDATE…RETURNING)
                           ▼
                    ┌──────────────┐   robots cache · per-host gate
                    │ Worker pool  │   conditional GET (ETag /
                    │ (N goroutines)│  If-Modified-Since)
                    └──────┬───────┘
                           │ fresh body or 304
                           ▼
                    ┌──────────────┐   content_hash dedup → skip
                    │ Parse + index│   re-embed on unchanged content
                    └──┬────────┬──┘
                       │        │
              ┌────────┘        └─────────┐
              ▼                            ▼
       ┌──────────┐                ┌───────────────┐
       │  BM25    │                │ Vector index  │
       │ inverted │                │ (brute-force  │
       │  index   │                │  cosine)      │
       └────┬─────┘                └──────┬────────┘
            │                             │
            └──────────── RRF ────────────┘
                          │
                ┌─────────┴─────────┐
                ▼                   ▼
        ┌──────────────┐   ┌─────────────────┐
        │  HyDE /      │   │  Cross-encoder  │
        │  paraphrase /│   │  reranker       │
        │  PRF / MMR   │   │  (optional)     │
        └───────┬──────┘   └────────┬────────┘
                │                   │
                └─────────┬─────────┘
                          ▼
            ┌──────────────────────────────┐
            ▼            ▼          ▼      ▼
         /search   /find_similar  /answer  /research
                   /contents      (cite)   (cite + SSE)
```

All Go. Three dependencies:

- [`modernc.org/sqlite`](https://gitlab.com/cznic/sqlite) — pure-Go SQLite, no cgo
- [`golang.org/x/net/html`](https://pkg.go.dev/golang.org/x/net/html) — HTML parser
- [`ledongthuc/pdf`](https://github.com/ledongthuc/pdf) — PDF text extraction (~300 KB, MIT, no cgo)

LLM, embedding, and rerank calls go out over plain HTTP — to OpenAI, Cohere, Voyage, Jina, or a self-hosted vLLM / llama.cpp / text-embeddings-inference / Ollama instance. Same interface everywhere; no vendor SDKs.

## Design principles

1. **Bounded everything.** `/research` caps sub-queries, passages, and wall-time. Predictable cost, predictable latency.
2. **Citations or nothing.** Every synthesized claim carries a numeric source id. No prose without provenance.
3. **Uncalibrated is honest.** Confidence numbers ship with `"calibrated": false` until there's enough outcome data to fit a model. Honesty over false precision.
4. **Own everything you can in Go.** External services are HTTP-shaped (OpenAI-compatible embeddings / chat, optional Cohere-shaped rerank). No vendor SDKs, no cgo.
5. **Three deps, no more without justification.** A new dependency needs an evaluation: how much does it actually improve the system? Reaches for a fourth without one fail review.
6. **Tweakable, not opinionated.** Every retrieval knob exposed as a query param. Defaults sensible, overrides obvious. Per-host crawl tuning for slow / fast hosts. Per-instance defaults via `cosift.json`.

## Storage layout

Everything lives under `data_dir`:

```
cosift-data/
├── cosift.db        SQLite (WAL mode)
└── cosift.db-wal    write-ahead log
```

Tables of interest:

| Table | Holds |
|-------|-------|
| `documents` | one row per URL: title, text, published_at, author, image, favicon, content_hash, etag, last_modified, fetched_at, last_error |
| `terms`, `postings` | BM25 inverted index |
| `passages` | passage-level dense vectors keyed by `(doc_id, offset, model)` — multiple embedding models coexist while you A/B them |
| `frontier` | crawl queue: status, depth, priority, enqueued_at, last_error |
| `query_paraphrases`, `query_hyde` | L2 cache for paraphrase + HyDE LLM responses |
| `query_outcomes` | `/feedback` data for offline calibration |

The schema is migrated forward automatically on startup; old data dirs work with new binaries.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the project-specific patterns (server-first arcs, lock-in tests, destructive-op guards, dep policy). The patterns emerged organically across the early development arc and are codified in CONTRIBUTING.md rather than enforced by tooling.

## License

MIT.
