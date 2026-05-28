# Pebble backend — design + tuning

The Pebble (pure-Go LSM-tree) storage backend is cosift's path-to-scale beyond SQLite's effective million-row ceiling. It coexists with the SQLite store; both implement the same operator-visible API surface (`cosift crawl --backend=…`, `cosift stats --backend=…`, `cosift query --backend=…`). This document is a one-page reference for what's there and how to operate it.

> **Status (as :** `cosift pebble-serve` mirrors the full `cosift serve` API surface — `/search`, `/find_similar` (URL or arbitrary text), `/answer` and `/research` (sync + SSE), `/contents` (single + batch), plus `/healthz` / `/stats` / `/metrics` / `/verify`. All retrieval/synth endpoints compose `include_domains` / `exclude_domains` / `since` / `until` filters with optional `rerank=true` and `expand=hyde|paraphrase`. Observability covers per-endpoint count + duration, HyDE/paraphrase cache hits, rerank/chat attempts + failures + duration, and warnings emission. Cross-references: [API.md](API.md) for the HTTP surface, [TUNING.md](TUNING.md) for knob choices, [EXAMPLES.md](EXAMPLES.md) for curl + CLI recipes.

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

Family-tag prefix bytes keep families disjoint for prefix scans. Big-endian IDs give natural ascending iteration order. Counter values in `'m'` enable O(1) corpus stats.

## Hot path: what runs where

**`IndexDocument(docID, title, text)`**:
1. Tokenize title (×3 boost) + body
2. Load `'g'` for docID — set of old termIDs from previous index of this doc
3. For each token: GetTermInfo(`'t'+term`), bump doc_freq if new for this doc, write `'p'+termID+docID` (16 bytes: tf + docLen)
4. For old termIDs not in new set: DELETE `'p'+oldTermID+docID`
5. Write new `'g'`, updated `'l'`, batch-commit
6. Maintain `'m'+sum_doc_len` and `'m'+indexed_docs` running counters

**`PebbleBM25.Search(q, k)`**:
1. `corpusStats()` → O(1) read of running counters
2. For each unique query token: GetTermInfo → IteratePostings (prefix scan) — sum BM25 scores into a doc-id map
3. For each scored doc: GetDocMeta(`'i'+docID`) — cheap URL+title
4. Phrase filter against doc.Text if `q` has quoted phrases
5. Sort, top-k

**`ClaimFrontier()`**:
1. Prefix-scan `'f'+'i'` to collect in-flight host set
2. Prefix-scan `'f'+'q'` for the first URL whose host is NOT in the in-flight set (host-fair)
3. Atomic batch: read primary, transition status → in_flight, swap `'f'+'q'` → `'f'+'i'`
4. Iterators closed via `defer` in closures

## Operating the Pebble backend

### Crawling

```bash
cosift crawl --backend=pebble \
  --duration 30m \
  https://docs.example.com
```

Useful flags:
- `--duration 30m` — bounded run; workers exit cleanly when ctx times out
- `cfg.Crawler.MaxConcurrent` (cosift.json) — start at 8 on a 16 GB VM
- `cfg.Crawler.StatusFile` — path to crawl-status.json; operators read with `cosift status-file`

### Environment knobs

```
COSIFT_PEBBLE_CACHE_MB=128     # default; lower for tight VMs
COSIFT_PEBBLE_MEMTABLE_MB=32   # default; per-memtable budget
COSIFT_PEBBLE_MEMTABLES=2      # default; max in-memory tables
COSIFT_PEBBLE_SYNC=false       # opt-in: skip fsync per commit
COSIFT_BM25_K1=1.2             # BM25 term-frequency saturation
COSIFT_BM25_B=0.75             # BM25 length normalization
COSIFT_HYDE_CACHE_SIZE=256     # expandQuery passage cache
COSIFT_PARA_CACHE_SIZE=256     # paraphraseQuery cache
```

`COSIFT_PEBBLE_SYNC=false` is the single biggest crawler-throughput win on commodity disks. Tradeoff: WAL writes survive process crash but not VM crash. Crawler resumes cleanly from frontier on next start, so the loss window is bounded.

### Monitoring a live crawl

Pebble locks the data dir for single-writer access — `cosift stats --backend=pebble` from a sidecar process fails while the crawler runs. Two options:

**Option A — status file**:
```bash
# Set cfg.Crawler.StatusFile in cosift.json
# Then read without taking the lock:
watch -n 5 cosift -config cosift.json status-file

# Machine-readable form — feeds jq / Prometheus exporters:
cosift status-file -json | jq '.indexed_docs, .age_seconds'
```

**Option B — pebble-info**:
```bash
cosift pebble-info -dir /var/lib/cosift/data/pebble
```

### Migration

```bash
# SQLite → Pebble
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
| `max_concurrent: 4`, cache caps, Sync mode | OOM-killed at 15.9 GB RSS in ~2 min |
| `max_concurrent: 4-8`, NoSync mode | Stable; ran several minutes without OOM |
| `max_concurrent: 8`, NoSync, panic recovery, defers | Recommended starting config for 16 GB |

**Recommendations:**
- **16 GB VM**: `max_concurrent: 8`, `COSIFT_PEBBLE_SYNC=false`, `COSIFT_PEBBLE_CACHE_MB=128`
- **32 GB VM**: `max_concurrent: 16`, `COSIFT_PEBBLE_SYNC=false`, `COSIFT_PEBBLE_CACHE_MB=256`
- **Bigger**: lift `max_concurrent` proportionally; cache `(VM_GB / 4) × 64 MB` is a reasonable rule of thumb

## Known limitations (today)

`cosift pebble-serve` now mirrors `cosift serve`'s API surface (search, find_similar, answer, research with SSE, contents, verify, metrics) with full HyDE + paraphrase + rerank composition. The remaining gaps:

| Gap | Workaround |
|---|---|
| Pebble single-writer lock blocks reads while crawling | Use `cosift status-file` or curl `/stats` on a running pebble-serve |
| HNSW vector indexing during crawl isn't auto-wired | Compose manually: `crawler.WithPassageWriter(index.NewHNSWWriter(hnsw, ps, persistEvery))` — bridge exists; default crawl path is BM25-only |
| Doc-freq isn't decremented on orphan posting cleanup | IDF accuracy shifts by sub-rounding-noise; acceptable until proven otherwise. Counter-drift is still detectable via `cosift verify` / GET `/verify` |

