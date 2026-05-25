# Pebble backend — design + tuning

The Pebble (pure-Go LSM-tree) storage backend is cosift's path-to-scale beyond SQLite's effective million-row ceiling. It coexists with the SQLite store; both implement the same operator-visible API surface (`cosift crawl --backend=…`, `cosift stats --backend=…`, `cosift query --backend=…`). This document is a one-page reference for what's there and how to operate it.

## When to pick Pebble vs SQLite

| If your corpus is… | Use |
|---|---|
| ≤ 100k docs, occasional writes, simple ops | SQLite (default; reader-friendly, no tuning required) |
| 100k–10M docs, sustained crawl writes | Pebble |
| > 10M docs | Pebble + larger VM (see *Resource sizing* below) |

The two backends have parity for BM25 search quality (`TestPebbleBM25MatchesSQLite` locks the invariant). They differ on operational characteristics, not retrieval.

## Schema (one-byte family-tag prefixes)

```
'd' + uint64-be(docID)            → gob(Document)
'u' + url                         → uint64-be(docID)
'h' + host + 0x00 + uint64-be(id) → empty (host scan index)
'i' + uint64-be(docID)            → uvarint-len-prefixed (url, title)
'g' + uint64-be(docID)            → uvarint(N) + N × uvarint(termID)
't' + term-string                 → packed (termID, doc_freq)
'p' + uint64-be(termID) + docID-be → 16 bytes (tf-uint64, doc_len-uint64)
'l' + uint64-be(docID)            → uint64-be(doc_len)
'f' + 'u' + url                   → packed frontierEntry
'f' + 'q' + host + 0x00 + url     → empty (queued, host-keyed index)
'f' + 'i' + host + 0x00 + url     → empty (in-flight, host-keyed index)
'v' + 0x00 + "meta"               → HNSW meta blob
'v' + 0x01 + uint64-be(nodeID)    → HNSW node blob
'm' + name                        → counter bytes (next_doc_id, next_term_id,
                                                   sum_doc_len, indexed_docs)
```

Family-tag prefix bytes keep families disjoint for prefix scans. Big-endian IDs give natural ascending iteration order. Counter values in `'m'` enable O(1) corpus stats (iter 207, 226).

## Hot path: what runs where

**`IndexDocument(docID, title, text)`** (iter 201, 208):
1. Tokenize title (×3 boost, iter 197) + body
2. Load `'g'` for docID — set of old termIDs from previous index of this doc
3. For each token: GetTermInfo(`'t'+term`), bump doc_freq if new for this doc, write `'p'+termID+docID` (16 bytes: tf + docLen)
4. For old termIDs not in new set: DELETE `'p'+oldTermID+docID` (iter 208 — orphan postings cleanup)
5. Write new `'g'`, updated `'l'`, batch-commit
6. Maintain `'m'+sum_doc_len` and `'m'+indexed_docs` running counters (iter 207)

**`PebbleBM25.Search(q, k)`** (iter 202, 207):
1. `corpusStats()` → O(1) read of running counters
2. For each unique query token: GetTermInfo → IteratePostings (prefix scan) — sum BM25 scores into a doc-id map
3. For each scored doc: GetDocMeta(`'i'+docID`) — cheap URL+title (iter 207 side-blob, NOT a full Document gob decode)
4. Phrase filter against doc.Text if `q` has quoted phrases (iter 198)
5. Sort, top-k

**`ClaimFrontier()`** (iter 209, 210, 221):
1. Prefix-scan `'f'+'i'` to collect in-flight host set
2. Prefix-scan `'f'+'q'` for the first URL whose host is NOT in the in-flight set (host-fair)
3. Atomic batch: read primary, transition status → in_flight, swap `'f'+'q'` → `'f'+'i'`
4. Iterators closed via `defer` in closures (iter 221 — fixes leak under panic)

## Operating the Pebble backend

### Crawling

```bash
cosift crawl --backend=pebble \
  --duration 30m \
  https://docs.example.com
```

Useful flags:
- `--duration 30m` — bounded run (iter 223); workers exit cleanly when ctx times out
- `cfg.Crawler.MaxConcurrent` (cosift.json) — start at 8 on a 16 GB VM
- `cfg.Crawler.StatusFile` — path to crawl-status.json (iter 224); operators read with `cosift status-file` (iter 225)

### Environment knobs

```
COSIFT_PEBBLE_CACHE_MB=128     # default; lower for tight VMs
COSIFT_PEBBLE_MEMTABLE_MB=32   # default; per-memtable budget
COSIFT_PEBBLE_MEMTABLES=2      # default; max in-memory tables
COSIFT_PEBBLE_SYNC=false       # opt-in: skip fsync per commit (iter 219)
```

`COSIFT_PEBBLE_SYNC=false` is the single biggest crawler-throughput win on commodity disks. Tradeoff: WAL writes survive process crash but not VM crash. Crawler resumes cleanly from frontier on next start, so the loss window is bounded.

### Monitoring a live crawl

Pebble locks the data dir for single-writer access — `cosift stats --backend=pebble` from a sidecar process fails while the crawler runs. Two options:

**Option A — status file** (iter 224, 225):
```bash
# Set cfg.Crawler.StatusFile in cosift.json
# Then read without taking the lock:
watch -n 5 cosift -config cosift.json status-file

# Machine-readable form (iter 229) — feeds jq / Prometheus exporters:
cosift status-file -json | jq '.indexed_docs, .age_seconds'
```

**Option B — pebble-info** (iter 217, only after crawler exits):
```bash
cosift pebble-info -dir /var/lib/cosift/data/pebble
```

### Migration

```bash
# SQLite → Pebble (iter 204)
cosift migrate-to-pebble -output /var/lib/cosift/data/pebble

# Migrate runs through PebbleBM25.IndexDocument — same code path used
# for fresh crawls, so the migrated index is byte-equivalent to a
# clean re-crawl (no divergence risk between two posting writers).
```

### Bench

```bash
# Head-to-head: SQLite vs Pebble at N=10000, 100 queries
cosift bench -mode storage -n 10000 -queries 100 -json > out.json

# Compare two saved runs:
cosift bench-compare baseline.json out.json
```

## Resource sizing

This session's empirical data (e2-standard-4, 16 GB RAM, 750 GB pd-balanced):

| Setting | Outcome |
|---|---|
| `max_concurrent: 16`, default Pebble cache, Sync mode | OOM-killed at 15.9 GB RSS in ~5 min |
| `max_concurrent: 4`, iter-218 cache caps, Sync mode | OOM-killed at 15.9 GB RSS in ~2 min |
| `max_concurrent: 4-8`, NoSync mode (iter 219) | Stable; ran several minutes without OOM |
| `max_concurrent: 8`, NoSync, iter-220 panic recovery, iter-221 defers | Recommended starting config for 16 GB |

**Recommendations:**
- **16 GB VM**: `max_concurrent: 8`, `COSIFT_PEBBLE_SYNC=false`, `COSIFT_PEBBLE_CACHE_MB=128`
- **32 GB VM**: `max_concurrent: 16`, `COSIFT_PEBBLE_SYNC=false`, `COSIFT_PEBBLE_CACHE_MB=256`
- **Bigger**: lift `max_concurrent` proportionally; cache `(VM_GB / 4) × 64 MB` is a reasonable rule of thumb

## Known limitations (today)

| Gap | Workaround |
|---|---|
| Pebble single-writer lock blocks reads while crawling | Use `cosift status-file` (iter 224/225) |
| `cosift serve` is SQLite-only; Pebble path uses `cosift pebble-serve` (/healthz, /stats, /search, /find_similar, /answer (sync+SSE), /research (sync+SSE), /contents, /verify, /metrics) | Paraphrase-fan-out + RRF wrapper not ported (HyDE is, via /search?expand=true) |
| HNSW vector indexing during crawl needs explicit wiring | `crawler.WithPassageWriter(index.NewHNSWWriter(hnsw, ps, persistEvery))` |
| Doc-freq isn't decremented on iter-208 orphan posting cleanup | IDF accuracy shifts by sub-rounding-noise; acceptable until proven otherwise |

## Iter map (path-2 rework)

```
iter 199 — HNSW vector index (pure Go, no deps)
iter 200 — Pebble doc store (UpsertDocument, GetDocByURL)
iter 201 — Postings + terms + doc_len families
iter 202 — PebbleBM25 read path
iter 203 — HNSW persistence to 'v' family
iter 204 — Migration tool: SQLite → Pebble
iter 205 — cosift pebble-serve (minimal HTTP)
iter 206 — Storage bench (SQLite vs Pebble)
iter 207 — Perf: corpusStats O(1), 'i' family, inline doc_len
iter 208 — Orphan posting cleanup on re-index
iter 209 — Frontier MVP (Push/Claim/Complete/Fail/Stats)
iter 210 — Host-fair claim via secondary indexes
iter 211 — CountQueuedPerHost + RecrawlURL
iter 212 — Crawler backend interfaces (CrawlerStore, PassageWriter)
iter 213 — `cosift crawl --backend=pebble`
iter 214 — HNSWWriter bridge
iter 215 — README Pebble section
iter 216 — `cosift query --backend=pebble`
iter 217 — `cosift pebble-info` (Pebble Metrics surface)
iter 218 — Memory caps (Pebble cache + memtables)
iter 219 — COSIFT_PEBBLE_SYNC=false
iter 220 — Per-worker panic recovery + stack trace
iter 221 — Iterator-leak defers in ClaimFrontier + CountQueuedPerHost
iter 222 — README resource-sizing notes
iter 223 — `cosift crawl -duration` for bounded runs
iter 224 — status.json crawler dump
iter 225 — `cosift status-file` reader
iter 226 — status.json carries indexed_docs + avg_doc_len
iter 228 — `cosift verify` (counter-drift detection)
iter 229 — `cosift status-file -json` (Prometheus / jq output)
iter 230 — pebble-serve `/verify` endpoint (HTTP counter-drift check)
iter 231 — pebble-serve `/metrics` (Prometheus exposition, no client dep)
iter 232 — pebble-serve `/search` include_domains / exclude_domains
iter 233 — pebble-serve `/search` hit enrichment (excerpt, published_at, author)
iter 234 — pebble-serve `/search` since / until date filters
iter 235 — pebble-serve `/search` sort=date_desc / date_asc
iter 236 — pebble-serve `/find_similar` (BM25 more-like-this, no embeddings)
iter 237 — pebble-serve `/search?include_text=true` (inline full text, no N+1)
iter 238 — pebble-serve `/stats` includes indexed_docs / sum_doc_len / avg_doc_len
iter 239 — pebble-serve `/find_similar?q=` augments the auto-derived MLT query
iter 240 — pebble-serve `/answer` (BM25 retrieval + OpenAI-compatible chat synth)
iter 241 — pebble-serve `/answer` honors include_domains / exclude_domains / since / until
iter 242 — pebble-serve `/answer?stream=true` (SSE: sources → chunk → done)
iter 243 — pebble-serve `/research` (planner: decompose → retrieve → dedupe → synth)
iter 244 — pebble-serve `/research?stream=true` (SSE: plan → sources → chunk → done)
iter 245 — pebble-serve `/find_similar` honors include_domains / exclude_domains / since / until
iter 246 — pebble-serve `/research` honors retrieval filters (sync + SSE); shared `retrievalFilters` helper
iter 247 — pebble-serve include_text=true now uniform across /search /find_similar /answer /research
iter 248 — pebble-serve `/search?rerank=true` (HTTP Cohere/Voyage/Jina/TEI, or LLM listwise)
iter 249 — pebble-serve `/answer?rerank=true` (rerank pool feeds synth; citation numbers track final order)
iter 250 — pebble-serve `/research?rerank=true` (sync + SSE; SSE 'sources' fires after rerank)
iter 251 — pebble-serve `/find_similar?rerank=true` (full rerank coverage across retrieval endpoints)
iter 252 — pebble-serve `/search?expand=true` (HyDE-style query expansion; reranker still scores against original q)
iter 253 — README updated to reflect full pebble-serve endpoint set + capabilities
iter 254 — pebble-serve `POST /contents` (batch URL → document; up to 100 per request)
iter 255 — pebble-serve `POST /contents` wire shape aligned with SQLite (results+took, found, cached, lang)
iter 256 — pebble-serve `/answer?expand=true` (HyDE expansion on the retrieval step)
iter 257 — pebble-serve `/research?expand=true` (per-sub-query HyDE) + shared `expandQuery` helper
iter 258 — pebble-serve /search + /answer migrated to the shared expandQuery helper
iter 259 — pebble-serve bounded in-memory HyDE cache (256 entries, drop-arbitrary on overflow)
iter 260 — pebble-serve `/metrics` exposes cosift_hyde_cache_hits_total / misses_total
iter 261 — pebble-serve `/metrics` exposes cosift_requests_total{endpoint="…"} via counting middleware
iter 262 — pebble-serve `/metrics` adds cosift_request_duration_seconds_sum (mean latency via PromQL)
iter 263 — pebble-serve `/metrics` adds cosift_rerank_attempts_total / _failures_total
iter 264 — pebble-serve `/metrics` adds cosift_chat_attempts_total / _failures_total (all 7 sites)
iter 265 — pebble-serve `/search` response includes `effective_query` when HyDE expand actually fired
iter 266 — pebble-serve `/answer` response includes `effective_query` (omitempty) on both paths
```
