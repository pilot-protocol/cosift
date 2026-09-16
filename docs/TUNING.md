# Tuning cosift for your corpus

Cosift exposes a lot of knobs intentionally — the right answer for a 100k narrow-domain index is different from the right answer for a 10M general-web index. This guide is the compass: when does flipping a given knob actually move retrieval quality.

The default config is a reasonable starting point for a mid-sized general-web corpus. If you've crawled docs.example.com or a single research site, several knobs below are worth flipping immediately.

## Retrieval quality

### Rerank: `?rerank=true`

**When to enable**: always for `/answer` and `/research`. Frequently for `/search` if your downstream consumer cares about top-1 / top-3 quality (chatbots, UI displays).

**When to skip**: when latency matters more than quality (autocomplete-style instant search) and your BM25 scores are already calibrated for your corpus.

**Knobs**:
- `cfg.Rerank.URL`: point at Cohere `/v1/rerank`, Voyage, Jina, or a local TEI server. Default — auto via `cfg.Chat.Model` if no URL.
- `cfg.Rerank.CandidateK` (default 20): how many candidates the reranker sees. Larger = better quality, more latency. 20 covers most regimes; raise to 50 for hard recall-bound tasks.

### Query expansion: `?expand=hyde` vs `?expand=paraphrase`

**HyDE** (`expand=true` or `expand=hyde`): 1 LLM call, appends a hypothetical answer passage to the query. Cheap recall win when query vocabulary diverges from corpus vocabulary ("how do I X" against docs that say "to X, you ...").

**Paraphrase** (`expand=paraphrase`): 1 LLM call generating N=3 paraphrases, runs BM25 against each, RRF-fuses the rankings. Higher quality than HyDE on ambiguous queries — different paraphrases surface different relevant docs.

**Cost**: paraphrase = N+1 BM25 calls per /search. /research with `expand=paraphrase` further multiplies by sub-queries. Caches (`hydeCache`, `paraCache`) absorb repeat queries; watch `cosift_paraphrase_cache_hits_total` on /metrics.

**Composability**: `expand=hyde&rerank=true` and `expand=paraphrase&rerank=true` both work — the reranker always scores against the original `q`, not the expanded version.

### Retriever: `?retriever=bm25` / `dense` / `hybrid`

**BM25** (default): lexical. Best on term-heavy, exact-match queries. No ML deps, sub-ms latency on the candidate pool.

**Dense** (`retriever=dense`): HNSW cosine over per-passage embeddings (pure-Go HNSW, no deps). Best on semantic / paraphrase-heavy queries where BM25 misses synonyms.

**Hybrid** (`retriever=hybrid`): BM25 + dense, fused via RRF (k=60). Generally the strongest default when vectors are available — gets BM25's lexical precision + dense's semantic recall. Each retriever returns `fetchK` candidates; the fused top-`fetchK` feeds the rest of the pipeline (filter → enrich → rerank).

**Requirements** for `dense` / `hybrid`:

1. `COSIFT_LOAD_HNSW=true` at server start so the graph loads from the `'v'` family.
2. An embedder configured (`cfg.Embeddings.Model`) so incoming queries can be embedded.

Missing either → request silently falls through to BM25 with a warning. No 5xx — clients see the warning and `retriever: "bm25"` label and know to fix server-side config.

**Composability**: `retriever=hybrid&expand=paraphrase&rerank=true` works — BM25 side picks up the paraphrase fan-out; dense side ignores it (HNSW search uses the embedded original query); rerank reorders the fused pool.

**Cost**: `dense` = 1 embed call + 1 HNSW search per /search. `hybrid` = 1 embed + BM25 + HNSW + RRF merge. For /research, multiply by sub-queries (each gets its own retrieval). HNSW search is O(log N) so the dense cost is dominated by the embed RPC.

### Time-decay: `?decay=<half_life_days>`

Use when freshness matters: news, releases, "latest" research. Multiplies each hit's score by `exp(-ln(2) · age_days / half_life)`. A doc from `half_life` days ago is worth half as much as a doc from today.

| Half-life | Behavior |
|-----------|----------|
| 7 | Very fresh — week-old docs are already at 0.5x. Use for news / fast-moving topics. |
| 30 | Modest decay — month-old docs at 0.5x, quarter-old at 0.125x. Typical "prefer recent" setting. |
| 365 | Mild decay — year-old docs at 0.5x. Good for evergreen content where age is a soft signal. |

Hits without a known `PublishedAt` are left unchanged — no signal means no penalty. Date filters (`since`/`until`) compose: `decay` is a soft re-weight, `since`/`until` are hard gates.

Applies BEFORE rerank, so when over-fetching the rerank pool sees the freshest-AND-most-relevant top-N. Pool re-sort transparently re-aligns `rerankTexts` so `decay` + `rerank` compose correctly.

### Diversification: `?mmr=<lambda>`

After retrieval (+rerank) the candidate pool is often dominated by near-duplicates — multiple chunks of the same paper, sibling pages on one site, paraphrases of the same concept. `mmr` reorders the pool with Maximal Marginal Relevance: at each step, pick the candidate maximizing

```
score = λ · sim(q, c)  −  (1−λ) · max_{s ∈ selected} sim(c, s)
```

`λ` lives in [0,1]:
- **1.0** — pure relevance, MMR is a no-op (same as not passing the flag).
- **0.7** — small diversification nudge; the top result is still always the most relevant hit, but later positions are reshuffled to break up duplicates.
- **0.5** — equal weight; useful when "give me a sampling of different angles" matters more than ranking precision.
- **0.0** — pure diversity, query relevance ignored.

**Requirements**: HNSW graph loaded (`COSIFT_LOAD_HNSW=true`) AND an embedder (to vectorize the query and look up per-hit vectors from the graph). Missing either → warning + silent skip (`retriever` label stays unchanged).

**Cost**: one embed RPC for the query + linear scans over the pool (O(K²) similarity comparisons where K = pool size ≤ rerankCandK). Negligible compared to LLM rerank.

**Composability**: `retriever=hybrid&rerank=true&mmr=0.7` is the strongest end-to-end /search you can build — BM25+dense fused, LLM-reranked, then diversified.

### BM25 parameters: `COSIFT_BM25_K1`, `COSIFT_BM25_B`

Defaults: k1=1.2, b=0.75. Visible on `/stats` as `bm25_k1` / `bm25_b`.

**Tune `b` (length normalization)** when your corpus has unusual document length distribution:
- Long-form docs (papers, full articles): try **b ≈ 0.5**. Less penalty for length.
- Short-form (titles, tweets, FAQs): try **b ≈ 1.0**. More penalty.
- Mixed: leave at 0.75.

**Tune `k1` (term-frequency saturation)** when terms repeat in relevant docs:
- Technical / how-to corpora where keyword repetition is meaningful: try **k1 ≈ 1.5–2.0**.
- General prose: leave at 1.2.

Changes take effect at server start; no re-index needed (params are scoring-time only).

### Top-k pool: `COSIFT_BM25_TOPK_POOL_FACTOR`, `COSIFT_BM25_DISABLE_TOPK_POOL`

PebbleBM25 resolves URL/title metadata for a bounded pool of `factor*k`
top-raw-score candidates (default factor 50) instead of every scored doc — at
multi-million-doc corpora this is the difference between tens and hundreds of
thousands of point reads per query. The selection is lossless (authority
reordering included) unless more than `factor*k` candidates sit within the
authority band of the k-th score; when that happens the engine logs
`top-k pool cap bound … results approximate` — raise the factor if the affected
queries are ranking-sensitive. Phrase queries and deleted-doc backfill expand
the pool automatically, so they never return fewer results than the resolve-all
path; a phrase that matches nothing still ends up scanning the full candidate
set (same cost as before the pool, plus a small selection overhead).

`COSIFT_BM25_DISABLE_TOPK_POOL=1` restores the resolve-all path for lossless
A/B comparison; both vars are read per query, no restart needed.

## Latency budget

### Don't enrich what you don't need: `?enrich=false`

`/search` defaults to `enrich=true` (per-hit GetDocByURL for excerpt/published_at/author). Set `enrich=false` when you're already going to call `/contents` for each hit downstream — saves k extra Pebble Gets per search.

### Include full text inline: `?include_text=true`

Off by default. Turn on when building a research pipeline that needs full doc text per source — saves an N+1 round trip to `/contents`. Cost: payload grows linearly with k × avg_doc_len.

### Stream synth: `?stream=true`

`/answer` and `/research` both support SSE. Self-hosted vLLM / Ollama on commodity hardware run 5–30s on non-trivial answers; streaming the `sources` event upfront and `chunk` events progressively makes the UX usable. Add `Accept: text/event-stream` or `?stream=true`.

## Storage / crawl

See [docs/PEBBLE.md](PEBBLE.md) for the full Pebble env-var reference. Headline:

- **Tight VM (≤16 GB)**: `COSIFT_PEBBLE_SYNC=false`, `max_concurrent: 8`, `COSIFT_PEBBLE_CACHE_MB=128`.
- **Bigger VM (32+ GB)**: raise `max_concurrent` to 16, `COSIFT_PEBBLE_CACHE_MB=256`.
- **Latency-sensitive read path**: leave `COSIFT_PEBBLE_SYNC=true` (default). The throughput cost is most felt on the write path during crawl.

## Observability for tuning

`/metrics` exposes everything you need to validate that a knob change actually helped:

- `rate(cosift_request_duration_seconds_sum{endpoint="/search"}) / rate(cosift_requests_total{endpoint="/search"})` — mean /search latency. Compare before/after a rerank flip.
- `rate(cosift_chat_duration_seconds_sum) / rate(cosift_chat_attempts_total)` — mean chat latency, isolated from retrieval.
- `rate(cosift_chat_failures_total) / rate(cosift_chat_attempts_total)` — chat provider health.
- `rate(cosift_hyde_cache_hits_total) / (rate(cosift_hyde_cache_hits_total) + rate(cosift_hyde_cache_misses_total))` — HyDE cache hit rate. < 50% suggests raising `COSIFT_HYDE_CACHE_SIZE` (default 256). Same shape for paraphrase via `COSIFT_PARA_CACHE_SIZE`.
- `rate(cosift_rerank_failures_total) / rate(cosift_rerank_attempts_total)` — reranker provider health.

## Target-specific website tuning

Crawling docs.example.com specifically? A short recipe:

1. `cosift init -site https://docs.example.com` — pre-populates `include_domains` so search defaults scope to that site.
2. Set `cfg.Crawler.MaxConcurrent: 4` (be polite to one host).
3. Default crawl, default BM25. Run `cosift verify` after the first 10k docs to confirm counter integrity.
4. Hand-eval 10 representative queries with `?rerank=true` and `?rerank=false`. If rerank wins clearly, set it as a default in your client.
5. If many docs are long (whitepapers, full pages), set `COSIFT_BM25_B=0.5` and re-eval the same 10 queries.

The eval delta is what tells you. Single-anecdote tuning leads in circles.

## Pitfalls / non-obvious behavior

Most of these used to be silent; every retrieval/synth response now carries a `warnings[]` field that names the specific issue. `jq .warnings` on a response is the fastest way to spot any of the no-op patterns below.

- **`?expand=true` is silent without `cfg.Chat.Model`.** The response's `expand` field echoes the canonical strategy (`hyde` / `paraphrase`); `effective_query` only changes when the chat call actually fired. Without a chat client the response `warnings[]` carries `"expand=... requested but no chat client configured"`. Set `cfg.Chat.Model` (and `OPENAI_API_KEY` or equivalent), or accept that the flag is a no-op for your deployment.
- **`?rerank=true` is silent without a reranker.** Same shape — flag accepted, no rerank applied. Response `warnings[]` flags it. `/stats` shows `reranker` when one is configured; absent means rerank is a no-op.
- **Unknown `?expand=` / `?sort=` values silently use the default.** The server now warns explicitly (`expand=foo is not a known strategy`, `sort=newest is not a known mode`). Pass valid values from `{hyde, paraphrase}` and `{relevance, date_desc, date_asc}`.
- **`?include_domains=https://example.com/foo` matches nothing.** The dot-boundary matcher expects bare hostnames; passing a URL silently filters everything out. The server emits a warning when any entry contains `/` or `://`.
- **`include_text=true` payload size is k × avg_doc_len.** A 500 KB doc at k=20 is 10 MB returned per call. Fine for backend pipelines, dangerous for browsers.
- **Pebble's single-writer lock blocks reads during crawl.** `cosift stats --backend=pebble` from a sidecar will fail. Use `cosift status-file` (lock-free) or curl `/stats` against a running `pebble-serve` instead.
- **`expand=paraphrase` is N+1 LLM calls per /search, more for /research.** Defaults are 3 paraphrases — so /search?expand=paraphrase is 1 chat (generate) + 4 BM25, /research is 1 chat (plan) + N×(1 chat + 4 BM25). The paraphrase cache absorbs repeats; watch `cosift_paraphrase_cache_hits_total`.
- **`/research` is slow without `stream=true`.** Total latency is 2-3 chat rounds + N×BM25 + 1 synth chat = 10-30s typical. Streaming makes the UX usable; sync is for backend pipelines that can wait.
- **Reranker failures fall back silently to BM25 order.** `cosift_rerank_failures_total / cosift_rerank_attempts_total` is the alert signal — if it crosses ~5% sustained, your reranker provider is degraded.
- **`?sort=date_desc` re-orders the top-k pool, not the candidate pool.** Raise `k` (or `cfg.Rerank.CandidateK` if reranking) to widen before sort if you're seeing too few dated results.
- **Chat calls go through a sync `time.Now()` boundary on `doChatStream` only after the stream completes.** Mean chat latency from `/metrics` includes full SSE duration for the streamed paths — useful for capacity planning but expect numbers higher than a raw single-shot completion.
