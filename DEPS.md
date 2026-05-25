# Dependencies

Every dependency justified. Add nothing without writing the case below.

## Direct dependencies

### `modernc.org/sqlite`
**Why:** SQLite without cgo. Required for the project goal of "single-binary, self-hostable, low-friction." The cgo alternative (`mattn/go-sqlite3`) is faster but breaks the static-build promise and the cross-compile story.
**Cost:** ~5 MB binary size, ~10% slower than cgo SQLite under heavy load (acceptable at v0 scale).
**Alternative considered:** writing our own embedded KV/SQL — not feasible.

### `golang.org/x/net/html`
**Why:** Standards-compliant HTML5 tokenizer. The cost of writing our own correctly is prohibitive; this is the de-facto Go stdlib HTML parser.
**Cost:** ~200 KB binary. Maintained by the Go team.
**Alternative considered:** `goquery` — heavier (depends on `x/net/html` anyway, adds jQuery-like API we don't need).

### `github.com/cockroachdb/pebble`
**Why:** SQLite + brute-force kNN was always going to plateau in the low hundreds of thousands of passages — adequate for v0, inadequate for the billion-scale rework the directive's "frontier-grade IR" line points at. Pebble is a mature pure-Go LSM-tree from CockroachDB, with block-level compression, mmap reads, batch writes, and native prefix scans. At million-doc scale it sustains roughly 100k writes/sec on commodity hardware vs SQLite WAL's ~10k cap.
**Cost:** ~5 MB binary size + ~25 transitive deps (zstd, protobuf, prometheus instrumentation, go-humanize, etc.). The transitive footprint is the price of admission for a real LSM; Pebble is the smallest credible option that ships pure-Go production-grade storage.
**Alternative considered:** (1) writing our own LSM — multi-week effort to do correctly, plus durability + recovery + compaction would each be load-bearing surface; (2) `dgraph-io/badger` — comparable on paper but heavier code footprint and a different design philosophy that doesn't compose as well with cosift's prefix-scan-heavy access patterns; (3) staying on SQLite — proven not to scale past low-million row counts at this workload, demonstrated empirically over the iter-188..198 crawl runs; (4) `bbolt` — fine for small key sets but the read amplification on multi-million-row scans becomes load-bearing.
**Where it's used:** `internal/store/pebble.go` (`PebbleStore`). Coexists with the SQLite `Store`; operators pick via config. Migration tooling (copy SQLite → Pebble) is a follow-up iter; for now `PebbleStore` is the destination format for new deployments.
**Dep policy exception:** This is the explicit "well-vetted dep" tradeoff documented in the path-2 design discussion. Pebble is ~200k LOC of mature, production-used storage that replaces ~10k LOC of segment management we would otherwise own. Carries its weight 100x over.

### `github.com/ledongthuc/pdf`
**Why:** Real-world docs corpora are commonly 20–40% PDF (technical specs, academic papers, government docs). Without PDF support the crawler misses a large slice of typical user workloads. The PDF format is a 1000+-page spec — writing a parser ourselves would be a multi-quarter project for marginal incremental value over an existing pure-Go option.
**Cost:** ~300 KB binary. Pure Go, MIT-licensed, no cgo, no system deps, no new transitive deps (verified against `go.sum` after `go get`). Text extraction only — does not pull in font/image/encryption surface that other PDF libraries do.
**Alternative considered:** (1) shelling out to `pdftotext` from poppler-utils — adds a runtime system dep, breaks the "single static binary" promise + bloats the Alpine Docker image; (2) `unidoc/unipdf` — AGPL-licensed, problematic for redistribution of a self-hosted MIT project; (3) `gen2brain/go-fitz` — uses cgo via muPDF, breaks the no-cgo rule.
**Where it's used:** `internal/crawler/pdf.go` `ParsePDF()` only. Dispatch from `processClaimed` and `FetchOne` when Content-Type contains `pdf`; HTML path is unchanged.

## Stdlib-only choices

Everything else uses stdlib. Specifically:

- **Config:** JSON via `encoding/json`. No TOML/YAML dep.
- **HTTP client:** `net/http` with a tuned `Transport`. No client library.
- **robots.txt:** ~80 LOC parser in `internal/crawler/robots.go`. Crawler-specific, our needs are simple.
- **BM25 / lexical index:** custom impl in `internal/index/bm25.go`. ~200 LOC.
- **URL canonicalization:** `net/url` + small helpers.
- **Content extraction:** custom Readability-style extractor in `internal/crawler/parse.go`. Targets main-content recall; not 100% F1 like Trafilatura but good enough for v0.
- **Embeddings / chat / rerank:** all out-of-process. Cosift speaks plain HTTP to any OpenAI-compatible endpoint (OpenAI itself, llama.cpp, vLLM, text-embeddings-inference, Ollama, or a Cohere-shaped HTTP reranker). No SDKs vendored.
- **Vector search:** brute-force linear scan over float32 vectors. ~200 LOC pure Go. Adequate up to ~1M passages on commodity hardware; HNSW reserved for if/when that ceiling matters.

## Rule of thumb

A new dep needs to pass three tests:

1. **Pure Go** (no cgo) unless there is no alternative.
2. **Replaces ≥ 200 LOC** we'd otherwise write, OR provides correctness guarantees we can't easily replicate.
3. **Has been maintained in the last 12 months** OR has so few changes that "abandoned" is fine.
