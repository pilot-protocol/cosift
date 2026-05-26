# pebble-serve HTTP API

Operator reference for `cosift pebble-serve` (the Pebble-backed companion to `cosift serve`). Endpoints below assume the default `127.0.0.1:7777` bind; substitute your `-addr`. All responses are JSON unless noted (`/metrics` is Prometheus text; SSE endpoints emit `text/event-stream`).

## Common query parameters

These compose across `/search`, `/find_similar`, `/answer`, `/research`:

| Param | Type | Default | Notes |
|-------|------|---------|-------|
| `q` | string | — | required for /search /answer /research |
| `url` | string | — | /find_similar: indexed source URL (URL-encoded). Either `url` or `text` required |
| `text` | string | — | /find_similar: arbitrary text for content-based MLT (no source URL needed); optional `title` for ×3 boost |
| `k` | int | 10 (5 for /answer, 8 for /research) | top-k results / sources |
| `include_domains` | CSV | — | dot-boundary suffix match: `example.com` matches `blog.example.com`, not `evilexample.com` |
| `exclude_domains` | CSV | — | same matcher; applied after include |
| `since` / `until` | YYYY-MM-DD or RFC3339 | — | filters on `doc.PublishedAt`; zero-date docs dropped under any date filter |
| `rerank` | bool | false | no-op when no reranker is configured |
| `expand` | string | false | `true` / `hyde` → HyDE passage appended to q. `paraphrase` → N chat-generated paraphrases, BM25 each, RRF-fuse. Supported by /search, /answer, /research (per sub-query). No-op when no chat client is configured. |
| `retriever` | string | `bm25` | `dense` → HNSW cosine over per-passage embeddings. `hybrid` → BM25 + dense, RRF-fused (k=60). Both require `COSIFT_LOAD_HNSW=true` at server start AND a configured embedder (`cfg.Embeddings.Model`); missing either → warning + silent fall through to BM25. Supported by /search, /answer, /research (per sub-query). |
| `include_text` | bool | false | inline full `doc.Text` on each hit/source |

`/search` has additional sort/enrich knobs (below). `/answer` and `/research` add `stream`.

---

## `GET /healthz`

Liveness probe. Always returns `{"status":"ok"}` with 200.

```bash
curl http://127.0.0.1:7777/healthz
```

## `GET /stats`

One canonical "shape of the index" call. O(1) — reads the iter-207 running counters.

```bash
curl http://127.0.0.1:7777/stats
```

```json
{
  "documents": 10353,
  "terms": 0,
  "indexed_docs": 10353,
  "sum_doc_len": 18204517,
  "avg_doc_len": 1758.32,
  "uptime": "27m12s",
  "backend": "pebble",
  "bm25_k1": 1.2,
  "bm25_b": 0.75,
  "reranker": "llm:gpt-4o-mini",
  "rerank_candidate_k": 20,
  "chat_model": "gpt-4o-mini"
}
```

`reranker`, `rerank_candidate_k`, and `chat_model` only appear when the matching capability is configured.

## `GET /metrics`

Prometheus exposition format. Hand-rolled, no client_golang dep. Covers index counters, HyDE cache, rerank attempts/failures, chat attempts/failures/duration, per-endpoint request count + duration sum.

```bash
curl http://127.0.0.1:7777/metrics
```

## `GET /verify`

Counter-drift check: compares the iter-207 counters to an authoritative scan of the `'l'` family. Returns 503 with drift fields when they disagree, so it composes into k8s liveness probes.

```bash
curl http://127.0.0.1:7777/verify
```

---

## `GET /search` / `POST /search`

BM25 retrieval over the Pebble store. Supports the common params above plus:

| Param | Default | Notes |
|-------|---------|-------|
| `enrich` | true | per-hit Excerpt + PublishedAt + Author; opt out with `enrich=false` |
| `sort` | `relevance` | `date_desc` / `date_asc` reorder the top-k pool; raise `k` to widen before re-sorting |

```bash
curl 'http://127.0.0.1:7777/search?q=raft+consensus&k=5&include_domains=docs.example.com'

# JSON body — same params, easier for long CSVs / quoted phrases:
curl -X POST -H 'Content-Type: application/json' \
  -d '{"q":"raft consensus","k":5,"include_domains":"docs.example.com","rerank":true}' \
  http://127.0.0.1:7777/search
```

```json
{
  "query": "raft consensus",
  "retriever": "bm25",
  "hits": [
    {"url":"https://docs.example.com/raft","title":"Raft Consensus","score":12.4,"excerpt":"…","published_at":"2024-05-12T00:00:00Z"}
  ],
  "took": "12ms"
}
```

With `rerank=true` + `expand=true`:

```bash
curl 'http://127.0.0.1:7777/search?q=raft&rerank=true&expand=true'
```

`retriever` becomes `bm25+hyde+rerank:<reranker name>`. `effective_query` appears when HyDE actually contributed terms.

### Retriever choices (`?retriever=`)

Three retrievers compose with the rest of the pipeline (filter → enrich → rerank). `?retriever=dense` and `?retriever=hybrid` apply to `/search`, `/answer`, and `/research` (per sub-query).

| Value | What runs | When to use |
|-------|-----------|-------------|
| `bm25` (default) | Lucene-style BM25 over the inverted index. Zero ML deps. | Term-heavy corpora, exact matches, sub-ms latency. |
| `dense` | HNSW cosine over per-passage embeddings (pure-Go HNSW, no deps). | Paraphrase-heavy / semantic queries where BM25 misses synonyms. |
| `hybrid` | BM25 (with `expand` if requested) + dense, fused via Reciprocal Rank Fusion (k=60). | Generally the strongest default once vectors are available — gets BM25's lexical precision + dense's semantic recall. |

`dense` and `hybrid` require **both**:

1. The HNSW graph is loaded at startup — set `COSIFT_LOAD_HNSW=true` before starting `pebble-serve`. The graph is built during crawl/index when `cfg.Embeddings.Model` is set; persisted under the `'v'` family.
2. A configured embedder (`cfg.Embeddings.*`). Without one, the server can't embed incoming queries.

If either is missing the request **does not fail** — it falls through to BM25 and adds a warning to the response so the caller knows the request didn't run dense:

```json
{
  "retriever": "bm25",
  "warnings": ["retriever=dense requested but HNSW graph not loaded (set COSIFT_LOAD_HNSW=true at server start) — fell back to BM25"]
}
```

Label vocabulary (`retriever` field on /search, /answer, /research responses):

| Label | Means |
|-------|-------|
| `bm25` | Plain BM25 |
| `bm25+hyde` | BM25 with HyDE expansion that actually fired (chat available + non-empty passage) |
| `bm25+paraphrase` | BM25 fanned across N paraphrases, RRF-fused |
| `dense` | HNSW cosine only |
| `bm25+dense:rrf` | Hybrid — BM25 ∪ dense, RRF-fused |
| `...+rerank:<name>` | Suffix appended to any of the above when a reranker reordered the candidate pool |

```bash
# Hybrid retrieval + rerank — typical "frontier" config
curl 'http://127.0.0.1:7777/search?q=raft+leader+election&retriever=hybrid&rerank=true'
# → "retriever": "bm25+dense:rrf+rerank:<name>"
```

## `GET /find_similar` / `POST /find_similar`

"More like this" via BM25 MLT (top-tf·idf terms → BM25). Either `url` (use an indexed doc as the source, neighbors exclude it) or `text` (arbitrary content, no source to exclude). Same common params as `/search`. Add `?q=...` to constrain neighbors with extra terms.

```bash
# Source is an indexed URL — find what's similar
curl 'http://127.0.0.1:7777/find_similar?url=https%3A%2F%2Fdocs.example.com%2Fraft&k=5'

# Source is arbitrary text (an unindexed draft, a snippet) — find similar indexed docs
curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:7777/find_similar \
  -d '{"text": "Raft is a consensus protocol that elects a leader...", "title": "draft on raft", "k": 5}'
```

```json
{
  "query": "raft consensus paxos replicated log leader election",
  "retriever": "bm25-mlt",
  "hits": [...],
  "took": "8ms"
}
```

### Dense `?retriever=dense`

When the HNSW graph is loaded (`COSIFT_LOAD_HNSW=true`), `/find_similar` can run HNSW cosine search around the source's persisted vector. URL-mode does this with **zero embed RPCs** — the source doc's vector is already in the graph from indexing. Text-mode embeds the supplied `title + text` before searching.

```bash
# URL-mode dense: no embedder needed; reads source vector from the graph
curl 'http://127.0.0.1:7777/find_similar?url=https%3A%2F%2Fdocs.example.com%2Fraft&retriever=dense&k=5'

# Text-mode dense: requires a configured embedder
curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:7777/find_similar \
  -d '{"text": "leader election in distributed systems", "retriever": "dense", "k": 5}'
```

Response shape is identical to BM25-MLT but `retriever` becomes `"dense"` (or `"dense+rerank:<name>"` with `rerank=true`). Fallback rules:

- HNSW graph not loaded → falls through to BM25-MLT with a warning.
- Text-mode and no embedder → falls through to BM25-MLT with a warning.
- URL-mode and no embedder → **still works** (graph lookup needs no embedder). The "no embedder" warning is suppressed for this case.

## `GET /contents` / `POST /contents`

Cached document retrieval.

```bash
# Single
curl 'http://127.0.0.1:7777/contents?url=https%3A%2F%2Fdocs.example.com%2Fraft'

# Batch (up to 100 URLs per request)
curl -X POST -H 'Content-Type: application/json' \
  -d '{"urls":["https://a.example.com","https://b.example.com"]}' \
  http://127.0.0.1:7777/contents
```

Batch response: `{"results":[{url, found, title, text, lang, cached, fetched_at, error?}, ...], "took": "..."}` — URLs not in the index get `found:false` in place.

## `GET /answer` / `POST /answer`

Single-question grounded answer with cited sources. Requires `cfg.Chat.Model` set; returns 501 otherwise.

```bash
curl 'http://127.0.0.1:7777/answer?q=what+is+raft+consensus&k=5'
```

```json
{
  "query": "what is raft consensus",
  "answer": "Raft is a consensus algorithm [1] designed for understandability [2]...",
  "sources": [
    {"url":"…","title":"Raft Paper","excerpt":"…","published_at":"…"},
    {"url":"…","title":"Raft Tutorial","excerpt":"…","published_at":"…"}
  ],
  "model": "gpt-4o-mini",
  "took": "2.4s"
}
```

SSE streaming:

```bash
curl -N -H 'Accept: text/event-stream' \
  'http://127.0.0.1:7777/answer?q=what+is+raft&stream=true'
```

Event sequence: optional `warnings` (when the request had silent no-ops) → `sources` (after retrieval; payload includes `query`, `sources`, `model`, `total_candidates`) → `answer_chunk` (per delta, payload `{"text": "..."}`) → `done` with `took`. On error: `error` event with the message and stream ends. Falls back to sync when the chat client doesn't implement streaming.

## `GET /research` / `POST /research`

Multi-step research: LLM plans 2-3 sub-queries → BM25 each → dedupe by URL keeping best score → optional rerank → cited synth. Requires `cfg.Chat.Model`.

```bash
curl 'http://127.0.0.1:7777/research?q=compare+raft+and+paxos&k=8'
```

```json
{
  "query": "compare raft and paxos",
  "plan": ["raft consensus algorithm", "paxos consensus algorithm", "raft vs paxos tradeoffs"],
  "answer": "Raft [1] differs from Paxos [3] primarily in understandability...",
  "sources": [...],
  "model": "gpt-4o-mini",
  "took": "5.8s"
}
```

SSE streaming event sequence: optional `warnings` → `plan` (payload `{query, plan, model, expand?}`) → `sources` (after rerank if any; payload includes `total_candidates`) → `answer_chunk` (per delta, payload `{"text": "..."}`) → `done` with `took`. On failure: `error` event tagged with `phase: "plan" | "synth"` and stream ends.

---

## Tuning knobs

Set in `cosift.json` (or wherever `-config` points):

```jsonc
{
  "chat": { "url": "https://api.openai.com/v1/chat/completions", "model": "gpt-4o-mini" },
  "rerank": { "enabled": true, "candidate_k": 20 },
  // cfg.Rerank.URL set → HTTPReranker (Cohere/Voyage/Jina/TEI wire shape)
  // else cfg.Rerank.Enabled + cfg.Chat.Model → LLMReranker (listwise via chat)
}
```

Environment:

| Var | Effect |
|-----|--------|
| `OPENAI_API_KEY` | falls through to chat + LLM reranker auth |
| `COHERE_API_KEY` / `VOYAGE_API_KEY` | fallback for `cfg.Rerank.URL` when `cfg.Rerank.APIKey` is empty |
| `COSIFT_PEBBLE_*` | see [docs/PEBBLE.md](PEBBLE.md) for cache / memtable / sync overrides |
