# Benchmarks

Reference performance data from a recent build. Numbers are illustrative —
real-world throughput depends on the machine, the corpus, and the
configuration. Re-run on your own hardware with `cosift bench` to get
numbers that match your deployment.

## Retrieval latency (`cosift bench -mode both`)

Synthetic corpus of 1000 docs, 50 queries:

| Mode | p50 | p95 | p99 | QPS |
|------|-----|-----|-----|-----|
| Vector (cosine, dim=384) | 262 µs | 298 µs | 569 µs | 3555 |
| BM25 (custom postings)   | 3230 µs | 4544 µs | 4924 µs | 300 |

Single goroutine, in-process, no network. Raw output in
[`bench-vector-bm25.json`](bench-vector-bm25.json).

The BM25 numbers reflect SQLite-backed postings lookups; the vector numbers
reflect pure-memory cosine over a brute-force linear scan. At larger corpora
the vector index moves to HNSW (deferred, see `DEPS.md`) — at the scales
cosift targets out-of-the-box, brute force is faster than the HNSW build cost.

## Crawl throughput (`cosift bench -mode crawl`)

Synthetic in-process site of 100 linked pages, single-machine crawl:

| Pages | Elapsed | Pages/sec |
|-------|---------|-----------|
| 100   | 1.5 s   | 66.5      |

Raw output in [`bench-crawl.json`](bench-crawl.json). The bench eliminates
network jitter by serving everything in-process; expect real-world
throughput to be bounded by network round-trip plus the `per_host_delay_ms`
politeness gate.

## Retrieval quality (eval gate)

The committed eval set is 38 queries × 20 docs. Run `cosift eval -retriever
<mode>` to reproduce.

| Retriever | R@1 | R@3 | R@10 | MRR | nDCG@10 |
|-----------|-----|-----|------|-----|---------|
| BM25 | 0.908 | 0.952 | 0.965 | 0.961 | 0.958 |
| Dense (OpenAI `text-embedding-3-small`) | 0.868 | 0.978 | 1.000 | 0.961 | 0.968 |
| Dense + LLM rerank | 0.921 | 0.991 | 1.000 | 0.987 | 0.990 |

Diff two saved eval reports with `cosift eval -baseline <path>` or, for
LLM-judged answer quality, `cosift answer-eval-compare A.json B.json`.

## Running these yourself

```bash
# Retrieval latency (single binary, no external services)
cosift bench -mode both -n 10000 -queries 100 -json > my-bench.json

# Crawl throughput (synthetic in-process site)
cosift bench -mode crawl -n 1000 -json > my-crawl.json

# Retrieval quality (needs the eval corpus; OPENAI_API_KEY for dense modes)
cosift eval -retriever bm25 -save my-eval.json
cosift eval -retriever dense -rerank -save my-eval-dense.json
```

Two saved bench outputs can be diffed for regression checking:

```bash
cosift bench-compare my-bench.json baseline-bench.json
```
