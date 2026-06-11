# Calibration — design notes for a cosift confidence model

**Status:** deferred (no scaffolding shipped). Cosift's `/answer` and
`/research` responses carry a `"calibrated": false` field that has been
unconditionally false . This document explains why that's
honest and what changes the day it can flip to `true`.

**Decision point:** the calibration model can't be fit until a deployment
accumulates roughly 10k outcomes with both classes (useful=true and
useful=false) in the `query_outcomes` table. No known deployment has that
volume as . Until one does, scaffolding for the model would be
abstraction over hypothetical future code — a pattern
CONTRIBUTING.md explicitly warns against.

---

## What calibration means here

"Calibrated" answers come with a numeric confidence in [0, 1] whose value
is **comparable across queries**: `0.9` on query A and `0.9` on query B
should both correspond to roughly the same probability that the answer is
correct (or at least, that its top-cited sources are relevant). Today's
`score` field on a SearchHit doesn't have that property — it's BM25 sum,
cosine, or RRF reciprocal depending on retriever, and the absolute values
shift with corpus size, query length, and retriever choice.

Within-result normalization already exists (`?calibrate=true` populates
`score_calibrated = score / max_score`). That makes scores comparable
WITHIN one response (top hit always 1.0, others as fractions). It does
NOT make scores comparable across responses — and that's the gap real
calibration would close.

---

## The data flow that's already in place

`/feedback` already ships and records the per-result outcome:

```bash
POST /feedback
Content-Type: application/json
{"query":"raft consensus","url":"https://x/distributed","score":0.87,"useful":true,"source":"thumbs"}
```

The handler appends a row to the `query_outcomes` table. Today nothing
reads this data; just established the pipe. The schema:

| Column      | Type    | What it carries                       |
|-------------|---------|---------------------------------------|
| `id`        | INTEGER | autoincrement                         |
| `query`     | TEXT    | the user's query string               |
| `url`       | TEXT    | the URL the outcome refers to         |
| `score`     | REAL    | the cosift score at retrieval time    |
| `useful`    | INTEGER | 1 = useful, 0 = not                   |
| `source`    | TEXT    | client-supplied label (thumbs, etc.)  |
| `recorded_at` | INTEGER | Unix seconds                        |

This is the calibration feedback loop's raw input. `cosift outcomes -format
csv` dumps the table for offline analysis.

---

## What would actually need to ship

Three stages of work, each its own iter (or arc of iters):

### Stage 1 — model fit (offline, deployment-specific)

The operator runs:

```bash
cosift outcomes -format csv > outcomes.csv
# fit a model — sklearn, scipy, or a Go logistic regression
# output: a model.json file with weights
```

Models that would work:
- **Platt scaling**: 1-parameter sigmoid fit on (score, useful) pairs.
  Tiny model (1 weight + bias). Right starting point.
- **Isotonic regression**: non-parametric monotonic fit. More flexible;
  more data-hungry. Right when 10k+ outcomes accumulate.
- **Logistic regression with retriever/score features**: 4-5 weights
  (one per retriever type + score + maybe corpus size). Right when
  operators want to differentiate calibration per retriever.

All three fit in <100 lines of Python or Go. None requires a new
dependency in the cosift binary — the model would be applied at runtime
via a tiny inference function.

### Stage 2 — runtime application

The Server gains a `Calibrator` interface:

```go
type Calibrator interface {
    Calibrate(retrieverKind, rawScore float64) float64
}
```

`Server.WithCalibrator(c Calibrator) *Server` wires it. When set, each
SearchHit's `score_calibrated` field is populated via `c.Calibrate(...)`,
and `/answer`/`/research` flip `Calibrated:true`.

A `PlattCalibrator` implementation reads `model.json` at startup and
applies `1 / (1 + exp(-(w * score + b)))` per hit. ~30 LOC.

### Stage 3 — observability

`/admin/stats` already exposes outcome counts via `CountOutcomes`. Add a
"calibration model loaded" flag + the deployment's reported AUC (if the
fit reported one). Operators see whether calibration is active and how
trustworthy it is.

---

## Why none of this is shipped yet

Two reasons, both honest:

1. **No deployment has the data.** Cosift is self-hosted; the
   `query_outcomes` table fills with whatever rate the deployment's users
   submit feedback. Real public-facing deployments typically need months
   of traffic to accumulate 10k useful/not-useful pairs. Shipping
   scaffolding before any deployment can use it ships dead code.

2. **Within-response normalization covers most cases.**
   Operators asking "is hit X significantly better than hit Y in THIS
   response" get a clean answer from `score_calibrated`. The remaining
   gap is operators asking "is response A globally better than response
   B" — which is much rarer and probably better answered by a separate
   "re-query with new params" UX than by a global confidence number.

The combination means the practical demand for real calibration is low
even when the data exists.

---

## What would warrant re-opening this doc

- A deployment reports 10k+ outcome rows with mixed useful=true/false +
  asks for cross-query confidence
- A new retriever type is added that has wildly different score
  distributions from existing ones (e.g., ColBERT-style multi-vector
  retrieval shipped as a separate retriever) — operators might want
  per-retriever calibration even without the data
- The `Calibrated` field becomes load-bearing for an external integration
  (e.g., a downstream agent that gates LLM calls on the confidence) —
  shipping the Platt scaffolding becomes the cheapest fix

Any of those reopens the iter.

---

## What stays as-is until then

- `Calibrated:false` on `/answer` and `/research` responses — honest, not
  a placeholder
- `score_calibrated` populated by `?calibrate=true` — the within-response
  signal that's actually useful
- `query_outcomes` table accumulates via `/feedback` — data keeps flowing
  for the eventual fit
- `cosift outcomes -format csv` for offline analysis — operators can
  inspect their accumulated data without waiting for the model
