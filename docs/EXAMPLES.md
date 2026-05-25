# pebble-serve recipes

Concrete curl invocations for common patterns. All assume the default `127.0.0.1:7777`. See [docs/API.md](API.md) for the full per-endpoint reference and [docs/TUNING.md](TUNING.md) for when to flip which knob.

## Search

### Bare BM25, top 10

```bash
curl 'http://127.0.0.1:7777/search?q=raft+consensus'
```

### Scope to a domain, last year

```bash
curl 'http://127.0.0.1:7777/search?q=raft+consensus&include_domains=docs.example.com&since=2024-01-01'
```

### Highest quality (rerank + paraphrase expansion)

```bash
curl 'http://127.0.0.1:7777/search?q=raft+consensus&rerank=true&expand=paraphrase'
```

`retriever` in the response becomes `bm25+paraphrase+rerank:<name>`. `total_candidates` shows how deep the BM25 pool went before filter + rerank.

### Latency-sensitive (skip enrichment, no chat)

```bash
curl 'http://127.0.0.1:7777/search?q=raft&enrich=false&k=20'
```

### Sort by recency among top-50

```bash
curl 'http://127.0.0.1:7777/search?q=raft&k=50&sort=date_desc'
```

### POST with JSON (long domain list)

```bash
curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:7777/search \
  -d '{
    "q": "raft consensus",
    "k": 10,
    "include_domains": "docs.example.com,docs.golang.org,research.example.org",
    "rerank": true
  }'
```

## Find similar

### Neighbors of a known URL

```bash
curl 'http://127.0.0.1:7777/find_similar?url=https%3A%2F%2Fdocs.example.com%2Fraft'
```

### Constrained "more like this BUT mentioning pricing"

```bash
curl 'http://127.0.0.1:7777/find_similar?url=https%3A%2F%2Fdocs.example.com%2Fraft&q=pricing'
```

### Cross-domain neighbors only

```bash
curl 'http://127.0.0.1:7777/find_similar?url=https%3A%2F%2Fdocs.example.com%2Fraft&exclude_domains=docs.example.com'
```

### Content-based: feed arbitrary text, find similar indexed docs

```bash
curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:7777/find_similar \
  -d '{
    "text": "Raft is a consensus protocol that elects a leader to manage a replicated log...",
    "title": "draft notes",
    "k": 5,
    "rerank": true
  }'
```

## Answer

### Default sync

```bash
curl 'http://127.0.0.1:7777/answer?q=what+is+raft+consensus'
```

### Highest quality

```bash
curl 'http://127.0.0.1:7777/answer?q=what+is+raft&rerank=true&expand=paraphrase'
```

### Streaming with SSE

```bash
curl -N -H 'Accept: text/event-stream' \
  'http://127.0.0.1:7777/answer?q=what+is+raft&stream=true&rerank=true'
```

Event sequence: `warnings?` → `sources` → `chunk` × N → `done`.

### Scoped + streaming POST

```bash
curl -N -X POST -H 'Content-Type: application/json' http://127.0.0.1:7777/answer \
  -d '{
    "q": "what does raft say about leader election",
    "k": 8,
    "include_domains": "docs.example.com",
    "since": "2024-01-01",
    "rerank": true,
    "stream": true
  }'
```

## Research

### Default

```bash
curl 'http://127.0.0.1:7777/research?q=compare+raft+and+paxos'
```

### Streaming (recommended — 10–30s total)

```bash
curl -N -H 'Accept: text/event-stream' \
  'http://127.0.0.1:7777/research?q=compare+raft+and+paxos&stream=true'
```

Event sequence: `warnings?` → `plan` → `sources` → `chunk` × N → `done`.

### Multi-step with paraphrase across sub-queries

```bash
# 1 plan chat + 3 paraphrase chats × N sub-queries — opt in deliberately.
curl -N -H 'Accept: text/event-stream' \
  'http://127.0.0.1:7777/research?q=compare+raft+and+paxos&expand=paraphrase&rerank=true&stream=true'
```

## Contents

### Single URL

```bash
curl 'http://127.0.0.1:7777/contents?url=https%3A%2F%2Fdocs.example.com%2Fraft'
```

### Batch (up to 100 URLs)

```bash
curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:7777/contents \
  -d '{
    "urls": [
      "https://docs.example.com/raft",
      "https://docs.example.com/paxos",
      "https://docs.example.com/2pc"
    ]
  }'
```

## Operations

### Pre-flight check before going live

```bash
cosift doctor                # local probes (data dir, sqlite, pebble, env)
cosift verify                # counter drift on Pebble (local — needs writer lock)
cosift verify -server http://127.0.0.1:7777  # routes through running pebble-serve's /verify (no lock needed)
curl http://127.0.0.1:7777/verify   # same drift check via HTTP
```

### Live crawl progress without taking the lock

```bash
# Status file is written every 10s; lock-free read.
cosift status-file -target 1000000

# Or JSON for piping to Prometheus textfile / jq:
cosift status-file -json
```

### Scrape Prometheus

```bash
curl http://127.0.0.1:7777/metrics
```

Counters of interest:
- `cosift_requests_total{endpoint="…"}`
- `cosift_request_duration_seconds_sum{endpoint="…"}` (mean latency via PromQL ratio)
- `cosift_chat_attempts_total` + `_failures_total` + `_duration_seconds_sum`
- `cosift_rerank_attempts_total` + `_failures_total`
- `cosift_hyde_cache_hits_total` / `_misses_total`
- `cosift_paraphrase_cache_hits_total` / `_misses_total`
- `cosift_warnings_emitted_total` (alert on misconfigured-request rate)

### Debug a misconfigured request

Every retrieval/synth response carries a `warnings[]` field when the request named a capability that didn't fire (chat unset, reranker unset, unknown `expand` / `sort` value, non-integer `k`).

```bash
# Was rerank actually applied?
curl -s 'http://127.0.0.1:7777/search?q=raft&rerank=true' | jq '.warnings, .retriever'

# Probe whether expand=paraphrase landed (false → returns null warning + bm25 retriever)
curl -s 'http://127.0.0.1:7777/search?q=raft&expand=paraphrase' | jq '.warnings, .expand, .effective_query'
```

`rate(cosift_warnings_emitted_total)` on `/metrics` is the alert metric — a sudden spike usually means a deploy started sending malformed requests.

### Verify a config override took effect

```bash
COSIFT_BM25_B=0.5 cosift pebble-serve -dir /var/lib/cosift/data/pebble
# In another terminal:
curl http://127.0.0.1:7777/stats | jq '{bm25_k1, bm25_b, reranker, chat_model}'
```

## CLI shortcuts

The `cosift` binary wraps the same HTTP API for headless use. All four retrieval / synth commands accept the same flags their HTTP counterparts accept (`-rerank`, `-expand`, `-since`, `-until`, `-include-domains`, `-exclude-domains`, …).

```bash
# Search
cosift search "raft consensus" -include-domains docs.example.com -rerank

# Find similar (by URL)
cosift find-similar https://docs.example.com/raft -k 5

# Find similar (by arbitrary text — e.g., a draft article)
cosift find-similar -text-file draft.md -text-title "draft on raft" -rerank

# Answer (streamed)
cosift answer "what is raft consensus" -stream -rerank

# Research with paraphrase fan-out
cosift research "compare raft and paxos" -expand paraphrase -rerank -stream
```

`-server URL` overrides the default endpoint when pebble-serve is on a non-default port.
