# Why the same query returns different results at different k

Measured on production 2026-09-15: 15,718,478 docs / 78,652,652 vectors, `POOL_FACTOR=200`,
authority alpha 2.0. Five binary arms, the 60 golden queries at k=10 and k=50.

The short version: **there are two independent defects, and the one everybody was chasing is not the
one that matters most.** This document exists so the next person does not repeat the three wrong
turns below.

## The symptom

`GET /search?q=X&k=10` and `GET /search?q=X&k=50` disagree about the top 10. On the goldens, 51 of 60
queries disagreed. `quantum gate teleportation logical qubits` was the reported example: wrong at
k=10, correct from k=15.

## Defect 1 — time decay re-ranks a k-sized window (the bigger one)

`serve_search.go` applies time decay *after* retrieval, over the candidate list it fetched. For a
plain query `fetchK == k`, so k=10 decays a 10-item list and k=50 decays a 50-item list. A deeper
list gives decay more to choose from, and different documents surface into the top 10.

Isolating it, on one binary at one moment:

| retriever | queries whose k=10 top-10 ≠ k=50 top-10 |
|---|---|
| `bm25+decay:180d` (the default) | 50/59 |
| `bm25` (`?decay=0`) | **0/59** |

On stock v0.2.5, turning decay off takes 51/60 → 19/60. So decay accounts for roughly two thirds of
the reported symptom, and it is **not reachable from `internal/index`** — no BM25 change affects it.

Magnitude, stock v0.2.5, k=10 against the k=50 reference:

| | decay on | decay off |
|---|---|---|
| mean overlap@10 | 0.7133 | 0.9450 |
| queries with an identical top-10 *set* | 12/60 | 46/60 |

**Not fixed.** The counterpart fix is to decay over a fixed-depth candidate window rather than a
k-sized one, so the input to decay stops depending on the caller's k. Not attempted.

## Defect 2 — BM25 makes two k-shaped decisions (the real engine bug)

With decay off, 19 of 60 goldens still disagree. Two places in `pebble_bm25.go` branch on the
caller's k:

1. **The MaxScore break.** `theta := kthLargest(scores, k)` — a smaller k gives a *higher* theta, so
   the scan stops earlier and produces a different score map before anything is ranked.
2. **The resolution pool.** `poolCap = factor * k` — a smaller k resolves fewer candidates. At
   factor 200 and k=10 the pool is 2,000, while the cap-bind log shows a median bound query with
   **68,497 in-band candidates** and a worst case of 403,943. Under 3% of eligible candidates get
   resolved.

Running both at `kEff = max(k, COSIFT_BM25_RANK_DEPTH)` and truncating to k afterwards makes the
top-k of any k ≤ depth a prefix of one ranking: **19/60 → 0/59, exactly zero.**

### Choosing the depth

k-sweep on stock v0.2.5 with decay off, each k's top-10 against the k=50 reference:

| k | queries differing | mean overlap@10 | p50 | p90 |
|---|---|---|---|---|
| 10 | 19/60 | 0.9450 | 42 ms | 162 ms |
| **15** | 15/60 | **0.9817** | 55 ms | 176 ms |
| 20 | 16/60 | 0.9800 | 66 ms | 184 ms |
| 30 | 14/60 | 0.9850 | 94 ms | 213 ms |
| 50 | 0/60 | 1.0000 | 147 ms | 324 ms |

The knee is at **15**: it captures most of the accuracy for +31% p50 and +9% p90, and it is where
`quantum gate teleportation logical qubits` goes from 0.20 to 1.00. Beyond 15 the curve is flat until
50, which is the reference and therefore trivially perfect.

`COSIFT_BM25_RANK_DEPTH` defaults to 0 (off, current behaviour). Depth 15 has **not** been measured
as its own production arm.

## Three wrong turns, recorded so they are not repeated

**1. "MaxScore × authority dominates."** The engine tracker concluded this on 2026-09-12 after
raising the pool factor 50 → 200 and seeing k-dependence barely move (48/60 → 46/60). The correct
reading is *raising the factor does not help*, not *MaxScore is the cause*. Cap-bind rate was
essentially identical across every arm here (15 vs 13 over comparable windows).

**2. Making the MaxScore bound authority-aware does not fix it, and is expensive.** The bound
`remainingMax < theta` ignores the authority multiplier applied after the scan, so with alpha 2.0 it
is 3× too loose and drops genuine top-k members. Correcting it to `remainingMax*maxMult < theta` is
provably more exact in isolation — against a lossless oracle on a small corpus where the pool never
binds, agreement with exact ranking goes 0.8362 → 0.9793. On production it measured:

| | k-dependence | k=10 p50 | k=10 p90 |
|---|---|---|---|
| control | 51/60 | 56 ms | 168 ms |
| authority-aware bound | 53/59 | 184 ms | 943 ms |

No improvement to the symptom, 3.3× p50 and 5.6× p90. It leaves theta k-shaped, so it changes *where*
the scan stops without changing *that it stops somewhere k-dependent*. The correctness gain is real
but is not separately measurable at production scale, and the cost is not.

**3. Flooring the pool alone does not fix it either.** An absolute floor on `poolCap` makes the
candidate universe k-independent but leaves theta k-shaped, so the score map still differs before the
pool is built: 48/59, and 210 ms p50. Both floors are needed, which is why the knob is a single rank
depth rather than a pool minimum.

## Things worth knowing that came out of this

- **`/search` does not pass the caller's k to the index.** It computes `fetchK` from `keepCap`, which
  widens 5× for include/exclude filters, 10× for a date filter, a flat 300 for `site=`, and 2× for
  rerank, capped at 500. For a plain query `fetchK == k`. Anything reasoning about "the k the index
  sees" has to start there.
- **`COSIFT_BM25_DISABLE_MAXSCORE=1` is a usable oracle on small corpora only.** It is exact by
  construction and far too slow at 15.7M documents.
- **Latency numbers here are not comparable across sessions.** The later arms ran alongside the 16:00
  UTC scheduled snapshot; measured contention on an unchanged binary was +7% p50 / +8% p90. The
  k-sweep ran on a quiet box, which is why its k=10 p50 reads 42 ms against 56 ms elsewhere.

## Reproducing

```sh
# On the box. decay=0 is the flag that separates the two defects.
python3 cosift-golden-capture.py -q cosift-golden-queries.txt -o out.jsonl \
  -b http://127.0.0.1:7777 -k 10,50 -t 75 -x decay=0
```

Then compare each query's k=10 top-10 against its k=50 top-10. Raw captures for all five arms are in
the monorepo at `tools/golden/2026-09-15-t0-prod/`.
