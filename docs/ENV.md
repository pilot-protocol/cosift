# cosift — Environment Variable Reference

This is the complete, code-verified reference for every runtime environment
variable the cosift engine reads. Defaults and effects below were read directly
from source (file:line cited in each row), not inferred.

**How to read this doc:**

- **Two configuration surfaces.** Env vars (this doc) are the *operator-tuning*
  surface — set them in the systemd unit / shell / container env. They sit
  *on top of* a second, separate surface: the JSON config file
  (`cosift.json`, typically `/home/ubuntu/cosift.json` in prod) parsed by
  `internal/config/config.go`. Things like the listen address, data dir,
  embeddings/chat URLs + models, rerank config, cluster topology, proxies,
  and the admin/peer-auth tokens live in `cosift.json`. This doc does **not**
  fully document `cosift.json`; it only notes where an env var overrides or
  interacts with a config field. The two should not be confused.
- **Helper semantics** (matters for what counts as a valid override):
  - `envIntDefault` (server, `serve_setup.go:1140`) — int; **must be `> 0`**, else falls back to the default. Empty/zero/negative/garbage → default.
  - `envDurationMsDefault` (server, `serve_setup.go:1152`) — integer **milliseconds**; must be `> 0`, else default. (Note: ms, not Go `"2s"` syntax.)
  - `envInt` (Pebble, `store/pebble.go:250`) — int; must be `> 0`, else default.
  - `envIntCrawler` (crawler, `crawler.go:635`) — int; must be `>= 0` (zero is a *valid* value here), else default.
  - `envFloatCrawler` (crawler, `crawler.go:964`) — float; must be `>= 0`, else default.
  - `firstEnv` (`main.go:381`) / `resolveAPIKey` (`main.go:355`) — return the first non-empty value among a list of names.
  - Inline `os.Getenv` checks vary; each row states the exact behavior.
- **Booleans are string-literal compares**, not generic parsing. Where a var is
  documented as `bool`, the code compares against an exact string (e.g.
  `== "1"`, `== "true"`, `== "false"`). Any other value behaves as "unset".
  The "default" column states the effective behavior when unset.

---

## SECRETS — read this first

cosift has **no env var that holds the server's admin password.** Be precise
about what each "token/key" var actually does:

| Var | What it really is | Secret? |
|---|---|---|
| `COSIFT_ADMIN_TOKEN` | **CLIENT-side** Bearer for the `cosift admin …` subcommands (`cmd_admin.go`) — the value the *CLI* sends to a remote box's `/admin/*`. It is **NOT** the server's auth gate. | Yes (client credential) |
| `COSIFT_EMBED_API_KEY` / `COSIFT_CHAT_API_KEY` | Provider API keys for the embed / chat slots (`resolveAPIKey`). Fall back to `OPENAI_API_KEY` → `OPENAI`. | Yes |
| `OPENAI_API_KEY` / `OPENAI` | OpenAI(-compatible) key; `OPENAI` is the accepted short form. | Yes |
| `COHERE_API_KEY` / `VOYAGE_API_KEY` | Rerank provider keys (HTTP reranker, when `cfg.Rerank.URL` is set and `cfg.Rerank.APIKey` is empty). | Yes |

**Server admin auth is NOT an env var.** The production `pebble-serve` admin
gate is `cluster.peer_auth_token` in `cosift.json` (and the legacy `serve`
path uses `server.admin_token`). See the **secret-ownership matrix in
[docs/HANDOVER.md §1](HANDOVER.md)** for the authoritative ownership classes,
rotation policy, and the current-state caveats (e.g. `peer_auth_token` empty in
prod = admin gate skipped).

**Production uses LOCAL models.** In prod, the embeddings and chat URLs in
`cosift.json` point at `127.0.0.1` (self-hosted vLLM / Ollama / TEI). Local
OpenAI-compatible endpoints accept anonymous requests, so
`OPENAI_API_KEY` / `OPENAI` / `COHERE_API_KEY` / `VOYAGE_API_KEY` /
`COSIFT_EMBED_API_KEY` / `COSIFT_CHAT_API_KEY` are **unset and unused in
production.** They exist only for operators who self-host against a keyed
OpenAI-compatible provider.

**Raw-query persistence is DISABLED in prod for confidentiality.**
`COSIFT_QUERY_LOG` and `COSIFT_FEEDBACK_LOG` enable on-box append of raw
queries / feedback to JSONL. Per **[docs/HANDOVER.md §2](HANDOVER.md)** these
are removed/unset at handover so no raw query text is persisted on the box. Do
not re-enable them without the confidentiality decision in that section.

---

## Server & Network

| Variable | Type | Default | Effect | Where read |
|---|---|---|---|---|
| `PORT` | int (string) | unset → no override | 12-factor port. When set, server binds `0.0.0.0:$PORT` (overrides `cosift.json` `server.addr`). For Cloud Run / Fly / Heroku / Render. | `internal/config/config.go:499` |
| `COSIFT_LISTEN` | string | unset → use `cosift.json` `server.addr` | Full listen address escape hatch (e.g. `127.0.0.1:8080`). Overrides `PORT`. | `internal/config/config.go:502` |
| `COSIFT_DATA_DIR` | string | unset → `cosift.json` `data_dir` (or built-in default) | Overrides the data directory (Pebble store + operational state). | `internal/config/config.go:505` |

> `cosift.json` is auto-augmented by a best-effort load of `./.env` at startup
> (`config.go:475`, `LoadDotEnv`), which sets any `KEY=VALUE` lines into the
> process env *only if not already set*.

---

## Auth & Secrets

| Variable | Type | Default | Effect | Where read |
|---|---|---|---|---|
| `COSIFT_ADMIN_TOKEN` | string (SECRET, client-side) | unset → no Bearer sent | Token the `cosift admin …` CLI subcommands send as the admin Bearer to a remote box. **Not** a server gate. | `cmd/cosift/cmd_admin.go:96,135,332,415,537`; `cmd_maintenance.go:618` |
| `COSIFT_EMBED_API_KEY` | string (SECRET) | unset → fall back to `OPENAI_API_KEY` → `OPENAI` → anonymous | API key for the **embed** slot. | `cmd/cosift/main.go:359` (`resolveAPIKey("embed")`) |
| `COSIFT_CHAT_API_KEY` | string (SECRET) | unset → fall back to `OPENAI_API_KEY` → `OPENAI` → anonymous | API key for the **chat** slot. | `cmd/cosift/main.go:361` (`resolveAPIKey("chat")`) |
| `OPENAI_API_KEY` | string (SECRET) | unset → try `OPENAI` → anonymous | OpenAI(-compatible) key; common fallback for both embed and chat slots, and read directly by `index` / `eval` / `serve` subcommands. | `main.go:368`; `cmd_index.go:155,612`; `cmd_eval.go:272,335,356,375,1316`; `cmd_serve.go:136`; `cmd_maintenance.go:562,609,610` |
| `OPENAI` | string (SECRET) | unset → anonymous | Accepted short form of `OPENAI_API_KEY`. Checked only after `OPENAI_API_KEY` is empty. | `main.go:368`; same call sites as `OPENAI_API_KEY` |
| `COHERE_API_KEY` | string (SECRET) | unset → try `VOYAGE_API_KEY` → anonymous | API key for the HTTP reranker when `cfg.Rerank.URL` is set and `cfg.Rerank.APIKey` is empty (checked first). | `serve_setup.go:431`; `cmd_serve.go:179` (via `firstEnv`) |
| `VOYAGE_API_KEY` | string (SECRET) | unset → anonymous | HTTP reranker key, checked only after `COHERE_API_KEY` is empty. | `serve_setup.go:433`; `cmd_serve.go:179` (via `firstEnv`) |

---

## Logging / Observability / Retention

| Variable | Type | Default | Effect | Where read |
|---|---|---|---|---|
| `COSIFT_QUERY_LOG` | string (path) | unset → query logging disabled (no-op) | Path to JSONL file; when set, every query-endpoint request is appended. Also gates the feedback log default location and the `/admin/query-log` tail endpoint. **Disabled in prod — see HANDOVER §2.** | `serve_setup.go:308`; `querylog.go:96`; `feedback.go:37,147` |
| `COSIFT_QLOG_NOLOG_TOKEN` | string (SECRET) | unset → header ignored (all requests logged) | When set, a query request whose `X-Cosift-No-Log` header exactly equals this value is served normally but **not** written to the query log — lets demand-loop replay traffic be excluded. Only effective when `COSIFT_QUERY_LOG` is set. | `serve_setup.go:316`; `querylog.go:52` |
| `COSIFT_FEEDBACK_LOG` | string (path) | unset → falls back to `feedback-log.jsonl` beside `COSIFT_QUERY_LOG`; if that's also unset → disabled | Path for the feedback JSONL log. **Disabled in prod — see HANDOVER §2.** | `feedback.go:34`; `serve_setup.go` (via `feedbackLogPath()`) |
| `COSIFT_SLA_LOG_PATH` | string (path) | unset → `<DataDir>/sla-violations.jsonl` (if DataDir set) | Where SLA-violation records are written. | `serve_setup.go:348` |
| `COSIFT_SLA_SEARCH_P95_MS` | duration (ms) | `1500` (1.5s) | P95 SLA target for `/search`. | `serve_setup.go:353` |
| `COSIFT_SLA_SEARCH_P99_MS` | duration (ms) | `4000` (4s) | P99 SLA target for `/search`. | `serve_setup.go:353` |
| `COSIFT_SLA_ANSWER_P95_MS` | duration (ms) | `8000` (8s) | P95 SLA target for `/answer`. | `serve_setup.go:354` |
| `COSIFT_SLA_ANSWER_P99_MS` | duration (ms) | `20000` (20s) | P99 SLA target for `/answer`. | `serve_setup.go:354` |
| `COSIFT_SLA_RESEARCH_P95_MS` | duration (ms) | `30000` (30s) | P95 SLA target for `/research`. | `serve_setup.go:355` |
| `COSIFT_SLA_RESEARCH_P99_MS` | duration (ms) | `60000` (60s) | P99 SLA target for `/research`. | `serve_setup.go:355` |
| `COSIFT_SLA_WINDOW_MS` | duration (ms) | `300000` (5min) | Rolling window over which SLA percentiles are computed. | `serve_setup.go:359` |
| `COSIFT_SLA_EVAL_MS` | duration (ms) | `30000` (30s) | How often the SLA monitor re-evaluates targets. | `serve_setup.go:360` |
| `COSIFT_WEB_DENOMINATOR` | int (int64) | `3500000000` (3.5B, Common Crawl est.) | Denominator for the `/stats` "% of indexed web" novelty figure; must be `> 0`. | `serve_stats.go:380` |
| `COSIFT_DEBUG_UPSERT` | bool (`"1"`) | unset → off | When `"1"`, prints `upsert-new: id=… url=…` to stderr for each newly-inserted document. Debug only. | `store/pebble.go:298,1413` |

> Note: `/healthz` (P95 50ms / P99 200ms) and `/stats` (P95 200ms / P99 1s) SLA
> targets are **hardcoded** (`serve_setup.go:356-357`) and not env-tunable.

---

## Rate Limiting

| Variable | Type | Default | Effect | Where read |
|---|---|---|---|---|
| `COSIFT_RATELIMIT_RPM` | float | unset / `<= 0` → **global limiter disabled** | Per-IP token-bucket refill rate (requests/min). Enabling this turns on the global rate limiter. | `serve_setup.go:1180` |
| `COSIFT_RATELIMIT_BURST` | float | `10` (only when RPM set) | Token-bucket burst capacity for the global limiter; must be `> 0`. | `serve_setup.go:1189` |
| `COSIFT_RATELIMIT_WHITELIST` | csv | empty → no whitelist | Comma-separated IPs that bypass the global limiter entirely. | `serve_setup.go:1195` |
| `COSIFT_FEEDBACK_RPM` | int | `20` | Per-client RPM for the **always-on** `/feedback` limiter (stricter than global); must be `> 0`. | `serve_setup.go:319` |
| `COSIFT_FEEDBACK_BURST` | int | `5` | Burst capacity for the `/feedback` limiter; must be `> 0`. | `serve_setup.go:320` |

---

## Retrieval & Ranking (BM25, decay, authority, expansion, caches)

| Variable | Type | Default | Effect | Where read |
|---|---|---|---|---|
| `COSIFT_BM25_K1` | float | `1.2` (PebbleBM25 default) | BM25 term-frequency saturation `k1`; applied only if parseable and `> 0`. | `serve_search.go:1350` |
| `COSIFT_BM25_B` | float | `0.75` (PebbleBM25 default) | BM25 length-normalization `b`; applied only if parseable and `> 0`. | `serve_search.go:1358` |
| `COSIFT_BM25_MIN_IDF` | float | `0.5` | IDF floor below which a query term is dropped as a stopword. `0` disables pruning. Must be `>= 0`. | `internal/index/pebble_bm25.go:38` |
| `COSIFT_BM25_DISABLE_MAXSCORE` | bool (non-empty disables) | unset → MaxScore optimization **enabled** | Any non-empty value disables the WAND/MaxScore early-termination optimization for benchmark-grade lossless ranking. | `internal/index/pebble_bm25.go:223` |
| `COSIFT_BM25_TOPK_POOL_FACTOR` | int | `50` | Sizes the metadata-resolution pool at `factor*k` candidates (PebbleBM25 top-k pool). Must be `>= 1`. Raise if the pool-cap log line fires on ranking-sensitive traffic. | `internal/index/pebble_bm25.go:50` |
| `COSIFT_BM25_DISABLE_TOPK_POOL` | bool (non-empty disables) | unset → top-k pool **enabled** | Any non-empty value restores the resolve-all metadata path (every scored candidate gets a `GetDocMeta`) — the pre-pool behavior, for lossless A/B comparison. | `internal/index/pebble_bm25.go:295` |
| `COSIFT_DEFAULT_DECAY_DAYS` | float | `180` (6-month half-life) | Default recency half-life (days) applied when a request has no explicit `?decay=`. `0` disables decay globally. Must be `>= 0`; explicit `?decay=N` still wins. | `serve_search.go:1592` |
| `COSIFT_AUTHORITY_ALPHA` | float | scorer's built-in default (`authority.New()`) | Authority-score blend weight; applied only if parseable and `>= 0`. | `serve_setup.go:188` |
| `COSIFT_TRANCO_CSV` | string (path) | unset → embedded whitelist + TLD heuristics only | Path to a Tranco rankings CSV to enrich authority scoring. | `serve_setup.go:193` |
| `COSIFT_MAJESTIC_CSV` | string (path) | unset → embedded whitelist + TLD heuristics only | Path to a Majestic Million CSV to enrich authority scoring. | `serve_setup.go:202` |
| `COSIFT_DISABLE_ENTITY_EXPAND` | bool (non-empty disables) | unset → entity expansion **enabled** | When unset, query is expanded with canonical-attribute rewrites (`qexpand.RewriteEntity`). Any non-empty value disables it. | `serve_search.go:2064` |
| `COSIFT_HYDE_CACHE_SIZE` | int | `256` | Capacity of the HyDE hypothetical-document cache; must be `> 0` (warns on bad value). | `serve_setup.go:161` |
| `COSIFT_PARA_CACHE_SIZE` | int | `256` | Capacity of the paraphrase cache; must be `> 0` (warns on bad value). | `serve_setup.go:173` |
| `COSIFT_ANSWER_CACHE_TTL_SEC` | int (seconds) | `60` | `/answer` response-cache TTL; must be `> 0` (use `0`/unset semantics via the answer-cache impl to effectively disable). | `serve_setup.go:299` |
| `COSIFT_ANSWER_CACHE_CAP` | int | `1024` | `/answer` response-cache entry cap; must be `> 0`. | `serve_setup.go:300` |
| `COSIFT_RESEARCH_CACHE_TTL_SEC` | int (seconds) | `600` (10min) | `/research` response-cache TTL; must be `> 0`. | `serve_setup.go:303` |
| `COSIFT_RESEARCH_CACHE_CAP` | int | `256` | `/research` response-cache entry cap; must be `> 0`. | `serve_setup.go:304` |
| `COSIFT_HOST_PARTITION` | bool (`"1"`) | unset → off | **Write path:** when `"1"`, host-partitioned postings are written on index (~2x posting write-amplification). | `store/pebble.go:3570` |
| `COSIFT_HOST_PARTITION_READ` | bool (`"1"`) | unset → off | **Read path:** when `"1"`, `site=` queries route to the `'P'`-family host partition (`SearchInHost`); off → the 50× host-boost path is used. | `serve_search.go:1976` |

---

## HNSW & Embeddings

| Variable | Type | Default | Effect | Where read |
|---|---|---|---|---|
| `COSIFT_LOAD_HNSW` | bool (`"true"`) | unset → graph **not** loaded into RAM (cheap meta only) | When `"true"`, loads the full HNSW vector graph into memory at startup (gigabytes at scale). Opt-in. | `serve_setup.go:90` |
| `COSIFT_HNSW_EF_SEARCH` | int | `50` (NewHNSW default) | HNSW `efSearch` override (raising to ~200 restored Recall@10); applied only when the graph is loaded and value `> 0`. | `serve_setup.go:104` |
| `COSIFT_DISABLE_PQ` | bool (`"true"`) | unset → PQ used if codebook present | When `"true"`, disables the runtime Product-Quantization search path even if PQ codes exist on disk (uses raw vectors). | `serve_setup.go:119` |
| `COSIFT_RECONCILE_ON_LOAD` | bool (`"false"` disables) | unset → reconcile **enabled** | After the async HNSW load, invalidates every graph node whose URL is no longer in the store's `'u'` family (purge-domain/purge-adult soft-delete docs without touching the graph). Set to `"false"` to skip. | `serve_setup.go` (`loadHNSWInto`) |
| `COSIFT_DENSE_FETCH_SLACK` | int | `16` | Pads the dense/hybrid fetch depth past k so candidates dropped at store resolution don't shrink results below k (mirrors BM25's `topKResolveSlack`). Must be `>= 0`. | `serve_search.go` (`denseFetchSlack`) |
| `COSIFT_HNSW_CHECKPOINT_SEC` | int (seconds) | `60` | Interval at which the in-crawl HNSW graph is checkpointed (`cosift crawl`). Override applied only if value `>= 5`. | `cmd/cosift/cmd_crawl.go:168` |
| `COSIFT_CRAWL_EMBED_CONCURRENCY` | int | `8` | Max concurrent embed calls from the in-serve crawler (throttle to protect interactive `/search`); must be `> 0`. | `serve_setup.go:742` |
| `COSIFT_EMBED_BATCH` | int | `128` | Max texts coalesced per inner embed call (batching embedder); must be `> 0`. | `serve_setup.go:755` |
| `COSIFT_EMBED_BATCH_WAIT_MS` | duration (ms) | `20` | Batching-embedder drain timer; must be `> 0`. | `serve_setup.go:761` |

---

## LLM (gates, deadlines, retries, degrade)

| Variable | Type | Default | Effect | Where read |
|---|---|---|---|---|
| `COSIFT_LLM_GATE_RERANK` | int | `8` | Concurrent-call cap for the rerank pool in the shared chat gate; must be `> 0`. | `serve_setup.go:291` |
| `COSIFT_LLM_GATE_ANSWER` | int | `4` | Concurrent-call cap for the answer pool in the shared chat gate; must be `> 0`. | `serve_setup.go:292` |
| `COSIFT_LLM_DEADLINE_ANSWER_MS` | duration (ms) | `30000` (30s) | Per-stage deadline for the `/answer` chat call; must be `> 0`. | `serve_setup.go:379` |
| `COSIFT_LLM_DEADLINE_RERANK_MS` | duration (ms) | `5000` (5s) | Per-stage deadline for the LLM rerank call; must be `> 0`. | `serve_setup.go:454` |
| `COSIFT_LLM_RETRIES` | int | `1` | Max retries for gated chat calls (both answer and rerank); must be `> 0`. | `serve_setup.go:380,455` |
| `COSIFT_LLM_DEGRADE_QUEUE` | int | `8` | vLLM queue-depth threshold above which the server sheds LLM work (degrade mode). Only active when `cfg.Chat.URL` is set. Must be `> 0`. | `serve_setup.go:338` |
| `COSIFT_LLM_PROBE_MS` | duration (ms) | `2000` (2s) | Interval for the vLLM `/metrics` load probe. Only active when `cfg.Chat.URL` is set; must be `> 0`. | `serve_setup.go:339` |
| `COSIFT_CHAT_MAX_TOKENS` | int | `2048` | Max-tokens cap on chat generation (prevents runaway/thinking loops); must be `> 0`. | `internal/embed/chat.go:95` |
| `COSIFT_THINKING` | bool (`"1"`) | unset → thinking **disabled** for Qwen3 models | When `"1"`, re-enables the model's "thinking" mode (only affects Qwen3* chat templates via vLLM). | `internal/embed/chat.go:112` |

---

## Crawler & Content

| Variable | Type | Default | Effect | Where read |
|---|---|---|---|---|
| `COSIFT_FETCH_HEADER_TIMEOUT_MS` | duration (ms) | `6000` (6s) | HTTP response-header timeout per fetch; must be `> 0`. | `internal/crawler/crawler.go:254` |
| `COSIFT_FETCH_TIMEOUT_MS` | duration (ms) | `12000` (12s) | Overall per-fetch HTTP timeout; must be `> 0`. | `internal/crawler/crawler.go:310` |
| `COSIFT_MAX_CONNS_PER_HOST` | int | `128` | `MaxConnsPerHost` / `MaxIdleConnsPerHost` for the crawler transport; must be `> 0`. | `internal/crawler/crawler.go:266` |
| `COSIFT_AUTO_SITEMAP_CONCURRENCY` | int | `16` | Cap on concurrent background auto-sitemap discoveries; must be `> 0`. | `internal/crawler/crawler.go:332` |
| `COSIFT_DYNAMIC_DOMAINS_FILE` | string (path) | unset → none | Path to a file of dynamic (JS-rendered) domains loaded at crawler init. | `internal/crawler/crawler.go:342` |
| `COSIFT_DIRECT_HOSTS` | csv | unset → built-in `defaultDirectHosts` list | Comma-separated hosts that bypass the remote fetcher and fetch directly. Setting it **replaces** the default list. | `internal/crawler/remote_fetcher.go:70` |
| `COSIFT_CRAWL_PDF` | bool (`"false"` disables) | unset → PDF parsing **enabled** (sandboxed) | Set to `"false"` to disable sandboxed PDF parsing. Any other value leaves it on. | `internal/crawler/crawler.go:1155` |
| `COSIFT_REFETCH_AFTER_HOURS` | int (hours) | `0` → disabled (every revisit issues a conditional GET) | Skip re-fetching a healthy URL fetched within this window. Also defines the "fresh" window for prefer-new (defaults to 24h there) and the WET fresh window. Must be `> 0`. | `crawler.go:1107,1492`; `wet.go:94` |
| `COSIFT_PREFER_NEW_URLS` | bool (`"1"`) | unset → off | When `"1"`, the frontier prefers never-seen URLs over recently-fetched ones (one `GetDocByURL` per candidate). | `internal/crawler/crawler.go:1488` |
| `COSIFT_ZOMBIE_RECLAIM` | bool (`"0"`/`"false"`/`"off"` disables) | unset → **on** | Marks a re-crawled URL's prior chunk generation invalid in the HNSW graph before adding fresh vectors, so the graph doesn't accumulate generations. O(k) per URL via the graph's URL index; invalidations persist with the next checkpoint. Zombies accumulate at the re-crawl rate until `/admin/hnsw-compact` (weekly timer, 15% threshold). Counters: `/stats.hnsw_reclaimed_total`, `cosift_hnsw_zombie_nodes`. | `crawler.go` (`ZombieReclaimEnabled`) |
| `COSIFT_EMBED_DECOUPLE_WORKERS` | int | `0` → decoupled embed pipeline **off** | Number of dedicated embed-worker goroutines that drain the crawl embed queue (decouples crawl from embed/HNSW-write latency). `0` keeps the synchronous path. Must be `>= 0`. | `internal/crawler/crawler.go:586` |
| `COSIFT_EMBED_DECOUPLE_BUFFER` | int | `4096` | Buffer size of the decoupled embed queue (only when workers `> 0`). Must be `>= 0`. | `internal/crawler/crawler.go:588` |
| `COSIFT_HOSTSWEEP_DISABLED` | bool (`"1"`) | unset → sweeper **enabled** | When `"1"`, disables the self-cleaning host sweeper entirely. | `internal/crawler/crawler.go:868` |
| `COSIFT_HOSTSWEEP_INTERVAL_SEC` | int (seconds) | `600` (10min) | Host-sweeper run interval; floored at 30s. Must be `>= 0`. | `internal/crawler/crawler.go:871` |
| `COSIFT_HOSTSWEEP_MIN_ATTEMPTS` | int | `100` | Min fetch attempts before a host is eligible for purge/demote. Must be `>= 0`. | `internal/crawler/crawler.go:875` |
| `COSIFT_HOSTSWEEP_DEAD_RATE` | float | `0.20` | Success-rate threshold below which a host is purged. Must be `>= 0`. | `internal/crawler/crawler.go:876` |
| `COSIFT_HOSTSWEEP_WEAK_RATE` | float | `0.50` | Success-rate threshold below which a host is demoted (between dead and weak). Must be `>= 0`. | `internal/crawler/crawler.go:877` |
| `COSIFT_WET_BATCH_SIZE` | int | `32` | Records per batch when ingesting Common Crawl WET archives; must be `> 0`. | `internal/crawler/wet.go:101` |
| `COSIFT_WET_WORKERS` | int | `8` | Worker goroutines for WET ingest; must be `> 0`. | `internal/crawler/wet.go:107` |

> Not an env var: `cosift.json` `crawler.auto_sitemap` (a config field) controls
> whether auto-sitemap discovery runs at all. A stale code comment references
> "`COSIFT_AUTO_SITEMAP=false`", but no such env var is read — only
> `COSIFT_AUTO_SITEMAP_CONCURRENCY` (above) exists.

---

## Pebble Store

| Variable | Type | Default | Effect | Where read |
|---|---|---|---|---|
| `COSIFT_PEBBLE_CACHE_MB` | int (MB) | `128` | Pebble block-cache size in MB; must be `> 0`. | `internal/store/pebble.go:153` |
| `COSIFT_PEBBLE_MEMTABLE_MB` | int (MB) | `32` | Pebble memtable size in MB; must be `> 0`. | `internal/store/pebble.go:154` |
| `COSIFT_PEBBLE_MEMTABLES` | int | `2` | Memtable count; `MemTableStopWritesThreshold = value + 2`. Must be `> 0`. | `internal/store/pebble.go:155` |
| `COSIFT_PEBBLE_COMPACTIONS` | int | `1` | Pebble `MaxConcurrentCompactions`. `1` (Pebble's default) serializes every background compaction on one slot — the cause of the 128K-SSTable pile and the 12 MB/s full persist observed on the production box; `4` is a sane value on a 64-core host. Must be `> 0`. | `internal/store/pebble.go` (`openPebble`) |
| `COSIFT_PEBBLE_SYNC` | bool (`"false"` disables) | unset → `Sync` (fsync each commit) | Set to `"false"` to use `NoSync` writes (skips per-commit fsync — faster crawls, drops durability vs OS crash; WAL still written so process-crash durability holds). | `internal/store/pebble.go:176` |

---

## Profiling

All profiling knobs are no-ops unless `COSIFT_PPROF_ADDR` is set.

| Variable | Type | Default | Effect | Where read |
|---|---|---|---|---|
| `COSIFT_PPROF_ADDR` | string (addr) | unset → pprof server off | When set (e.g. `127.0.0.1:6060`), starts a loopback pprof HTTP server. | `serve_setup.go:627` |
| `COSIFT_MUTEX_PROFILE_FRACTION` | int | unset → off (`0`) | `runtime.SetMutexProfileFraction(n)` — sample 1-in-`n` lock waits. Only applied when pprof is on and `n > 0`. | `serve_setup.go:633` |
| `COSIFT_BLOCK_PROFILE_RATE` | int | unset → off (`0`) | `runtime.SetBlockProfileRate(n)`. Only applied when pprof is on and `n > 0`. | `serve_setup.go:639` |
| `COSIFT_CHECKPOINT_DIR` | string (path) | unset → `/tmp` | Base directory for `/admin/checkpoint` Pebble checkpoints (a `cosift-ckpt-<nanos>` subdir is created under it). | `cmd/cosift/serve_admin.go:100` |

---

## Excluded (test-only, not real config)

These appear **only** in `*_test.go` files (verified: no non-test references)
and are not runtime configuration:
`COSIFT_DOTENV_BARE`, `COSIFT_DOTENV_SINGLE`, `COSIFT_DOTENV_DOUBLE`,
`COSIFT_DOTENV_EXISTS`, `COSIFT_DOTENV_TEST_KEY_1`,
`COSIFT_FIRSTENV_A`, `COSIFT_FIRSTENV_B`, `COSIFT_FIRSTENV_C`.
