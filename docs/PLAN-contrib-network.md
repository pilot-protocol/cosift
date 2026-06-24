# PLAN — Cosift Open Contribution Network

Status: DRAFT for review · Owner: teodor · Last updated: 2026-06-12

Two coupled initiatives, one shared trust substrate:

1. **Publisher path** — developers index their documentation; the index entry stays
   active only while their Pilot app holds a live lease (heartbeats), solving the
   LLM-knowledge-cutoff problem for new dev tools and driving Pilot adoption.
2. **Open frontier** — third-party peers claim crawl/embed work from the frontier
   and submit signed results; contributions are quarantined, verified by sampling,
   attributed, and revocable.

Everything below is grounded in current code. Line numbers verified 2026-06-12
against branch state; numbers marked `≈` came from exploration agents and should
be re-confirmed at implementation time (symbol names are authoritative).

---

## 0. Definitions

| Term | Meaning |
|---|---|
| **Principal** | An ed25519 pubkey registered with cosift. Roles: `publisher`, `contributor`, or both. The key of an appstore app instance (`$APP/identity.json`) or any Pilot identity. |
| **Collection** | A claimed domain/path-prefix owned by a publisher principal. Carries lease state. |
| **Lane** | Frontier priority class. `0=submitted` (publisher docs), `1=refresh`, `2=discovered`, `3=bulk`. |
| **Claim** | A leased unit of work (fetch one URL, or embed N server-defined chunks) handed to a contributor, with TTL. |
| **Envelope** | The canonical signed message wrapping every authenticated request (Appendix A). |
| **Quarantine** | Staging area for contributed results; nothing enters the main index without passing verification. |
| **Tier** | Per-principal reputation level; determines sampling rate, quotas, and lane access. |

## 0.1 Security invariants (must hold at every phase)

- **I1** — No write reaches even quarantine without a valid envelope signature from a registered, non-banned principal referencing a live claim.
- **I2** — No contributed content reaches the main index without passing the verification pipeline for its kind.
- **I3** — Every promoted doc/vector is attributable: `doc → (principal, sig, ts)` is stored and survives restarts.
- **I4** — Banning a principal makes retroactive purge of all their promoted contributions possible and bounded (one index walk).
- **I5** — A brand-new principal is sampled at 100%; sampling rate only decreases through verified-good history (Sybil identities gain zero leverage).
- **I6** — Embed jobs are always server-defined text. We never accept a vector for text the server did not hand out.
- **I7** — The submission/work auth surface never reuses `Server.AdminToken` or `Cluster.PeerAuthToken`.
- **I8** — Suspended collections and banned principals are filtered at the single retrieval choke point shared by `/search`, `/query`, `/answer`, `/research`.
- **I9** — All contribution-path traffic (`/pub/*`, `/work/*`) is transport-bound to Pilot: the only network-reachable surface is an overlay listener. The HTTP server's contribution paths are bound to a loopback/UDS the operator's daemon owns; they are not routed by the public reverse proxy. The envelope's signing key (WS0.1) MUST equal the Pilot peer pubkey that authenticated the overlay tunnel — mismatch = reject. This makes "the data came through Pilot" cryptographically verifiable, not just operationally assumed.

---

## WS0 — Envelope + principal registry (foundation; everything depends on this)

### WS0.1 Canonical signed envelope
- **Where (new):** `internal/principal/envelope.go` (+ `envelope_test.go`).
- **Reuse:** `crypto.Verify()` / key codecs from web4 — `pilot-protocol/common/crypto/identity.go` (`Identity.Sign` ≈:32, `Verify` ≈:38, base64 key codecs). Import or vendor; decide module path at implementation (cosift currently has no dependency on `common`).
- **What:** Define `Envelope{PrincipalPub, Kind, ClaimID, BodySHA256, ModelID, Seq, Sig}` with a deterministic canonical byte serialization (length-prefixed fields, fixed order — not raw JSON). `Kind ∈ {register, pub_claim, pub_submit, heartbeat, work_claim, work_submit, work_renew, retire}`. `Seq` is per-principal strictly monotonic.
- **Acceptance:**
  - Round-trip test: sign → verify passes; any single-byte mutation of any field fails verification.
  - Replay test: same envelope presented twice → second rejected (`seq` not advanced).
  - Cross-impl fixture: a JSON test vector checked in so the worker app (WS10) can verify its signing against the same bytes.
  - Fuzz test on the canonical decoder (`go test -fuzz=FuzzEnvelopeDecode -fuzztime=30s` clean).

### WS0.2 Principal registry
- **Where (new):** `internal/principal/registry.go`; persistence in WS1.1.
- **What:** `Register(pub, roles)`, `Get(pub)`, `Bump(pub, outcome)` (verified-ok / verified-fail / mismatch counters), `Ban(pub, reason)`, seq tracking, per-role quotas (URLs/day, bytes/day, max in-flight claims).
- **Acceptance:**
  - Unit tests for quota windows (day rollover), seq monotonicity, ban idempotency.
  - Banned principal: every envelope kind rejected with a distinct error code (`403 problem+json, type=principal_banned`).

### WS0.3 HTTP auth middleware
- **Where:** `cmd/cosift/pebble_serve.go` — new `wrapSigned(handler)` alongside the existing route table (route registration block at `:474-532`). Mounted only on `/pub/*` and `/work/*`.
- **What:** Parse envelope from `X-Cosift-Envelope` header (base64) covering the request body hash; verify; load principal; enforce quota; stash principal in request context.
- **Acceptance:**
  - Table-driven handler tests: no envelope / bad sig / stale seq / banned / over-quota → 401/403/429 respectively with problem+json bodies; happy path passes principal to handler.
  - `Server.AdminToken` and `Cluster.PeerAuthToken` are never consulted on these routes (assert via test that requests carrying only those tokens are rejected) — invariant **I7**.

---

## WS1 — Store schema (Pebble families, attribution, doc provenance)

Key-family conventions live at `internal/store/pebble.go:12-21` (schema comment
block) — extend that comment with every new family below.

### WS1.1 New key families
- **Where:** `internal/store/pebble.go` (+ split into `pebble_contrib.go` if >300 lines).
- **What:**
  - `'P' + pubkey` → gob(PrincipalRecord) — registry persistence for WS0.2.
  - `'C' + collectionID` → gob(CollectionRecord{Owner, Prefix, State, LeaseSeq, LastRenewal, VerifiedAt, Quotas}); secondary `'C' + 'p' + prefix` → collectionID for prefix-claim lookup.
  - `'S' + ulid` → gob(StagedContribution) — quarantine, TTL'd.
  - `'A' + pubkey + docID` → empty (principal→docs walk for purge); `'A' + 'd' + docID` → {pubkey, sigSHA, ts} (doc→provenance).
  - `'W' + claimID` → gob(WorkLease{Kind, Payload, Principal, Expiry, Attempts}).
- **Acceptance:**
  - CRUD unit tests per family; `cosift pebble-compact` (cmd/cosift/pebble_compact.go) runs clean over a store containing all new families.
  - Snapshot/restore: create records → checkpoint (`POST /admin/checkpoint`, route at `:518`) → reopen from checkpoint → records intact.

### WS1.2 Document provenance + docMeta versioning
- **Where:** `Document` struct `internal/store/store.go:30-66` (add `CollectionID uint32`, `Principal []byte` — empty for operator-crawled); docMeta packed blob written in `UpsertDocument` `internal/store/pebble.go:250-319` (write at ≈:296); all docMeta readers (grep `GetDocMeta`).
- **What:** Add a leading version byte to the docMeta packed value (`v2 = version, collection_id, url, title`). Reader accepts v1 (no version byte ⇒ collection 0) and v2. No eager migration — lazy upgrade on next write of each doc.
- **Acceptance:**
  - Mixed-store test: store with v1 and v2 entries; reads return correct collection for both; search enrichment loop unaffected.
  - On the GH200 corpus (~9M-host scale): `/stats` doc count identical before/after deploy; zero `docMeta decode` errors in logs over 24h.

### WS1.3 Purge-by-principal / purge-by-collection
- **Where (new):** `internal/store/purge.go`; deletion paths must cover: doc keys (`'d','u','i','h'` families per pebble.go:12-21), postings (`'p','t','l','g'` — see `IndexDocument` ≈:1033-1100), HNSW passages (delete path near `internal/index/hnsw.go:445`), PQ codes (`'q'` family, `internal/index/pq_persist.go`), attribution rows.
- **What:** `PurgePrincipal(pub)` and `PurgeCollection(id)` — walk `'A'`/collection index, delete everything, emit count. Rate-limited background job (not request-path).
- **Acceptance:**
  - E2E test: index 100 docs from principal X and 100 from Y → purge X → `/search` returns only Y's docs for matching queries; HNSW node count drops accordingly; `/stats` reflects new count; re-running purge is a no-op (idempotent).

---

## WS2 — Frontier lanes + work leases

### WS2.1 Lane byte in frontier keyspace
- **Where:** `frontierStatusIndexKey` `internal/store/pebble.go:371` (key becomes `'f' + sub + lane + host + 0x00 + url`); `frontierEntry` struct `:415` (add `Lane uint8`); `PushFrontier` `:1187` (new signature or `PushFrontierLane(ctx, url, depth, lane, priority)`); status constants `:404-409` unchanged.
- **What:** All four lanes; existing callers map to lanes — crawler discovery → lane 2; RSS/sitemap refresh (`internal/crawler/rss.go`, `sitemap.go`) → lane 1; WET/bulk handlers (`pebble_serve.go:2643, :2771`) → lane 3; publisher submissions (WS3) → lane 0.
- **Migration:** one-time startup scan: re-key existing `'f'+sub+host…` entries to lane 2. Gate behind a meta marker (`'m' + "frontier_lane_migrated"`) so it runs once.
- **Acceptance:**
  - `go test ./internal/store/...` green including new lane round-trip tests.
  - Migration test: build store with old-format keys → open → `/queue` totals (queued/in_flight/done/errored) identical pre/post; all entries report lane 2.
  - On GH200: migration completes < 10 min for current frontier size; log line reports counts.

### WS2.2 Lane-weighted claiming
- **Where:** `ClaimFrontier` `internal/store/pebble.go:1230`; per-lane cursor replaces single `frontier_cursor` meta (load ≈:199-201).
- **What:** Weighted drain (default 50/30/15/5, configurable WS7) via deterministic weighted round-robin over non-empty lanes; host-fairness logic preserved *within* a lane; empty lanes donate their share.
- **Acceptance:**
  - Statistical test: seed 4 lanes × 1k URLs, claim 1k → per-lane counts within ±10% of weights; with lane 0 empty, its share redistributes.
  - Existing crawler e2e (`main_e2e_test.go`, `bench_crawl_test.go`) still green.

### WS2.3 Work leases (external claims)
- **Where (new):** `internal/store/lease_work.go` using `'W'` family (WS1.1); re-queue uses `frontierEntry.Attempts` (`pebble.go:415`).
- **What:** `ClaimWork(principal, kinds, lanes, max)` → atomically pop frontier entries (fetch jobs) or embed-pending chunks (WS5) into `'W'` leases with TTL (default 10 min fetch / 5 min embed); `RenewLease`; expiry sweeper returns work to frontier with `Attempts++`; per-principal in-flight cap (default 32).
- **Lane policy:** lane 0 fetch jobs only to tier ≥ `trusted` principals (or server-fetched) — publisher-owned content has higher poisoning stakes. Lanes 1–3 open per tier table (Appendix C).
- **Acceptance:**
  - Lease expiry test (injected clock): claimed-not-submitted work returns to frontier, attempts incremented, claimable by another principal.
  - Starvation test: principal claiming and dropping repeatedly hits in-flight cap; frontier drains via others.
  - Double-submit on same claimID → second rejected (409).

---

## WS3 — Publisher path (collections, verification, leases)

### WS3.1 Collection claims + domain verification
- **Where (new):** `internal/collections/` (records via WS1.1 `'C'`); HTTP in `pebble_serve.go` (`POST /pub/claim`, `GET /pub/verify-status`).
- **What:** Claim = `{prefix}` where prefix is `host[/path-prefix]` (**OPEN DECISION D2**). Verification worker fetches `https://<host>/.well-known/cosift-verify.txt` expecting the challenge token (DNS TXT `cosift-verify=<token>` fallback). First verified claim on a prefix wins; overlapping claims rejected (longest-prefix rule for nesting). Re-verify every 30 days; failure → collection `unverified` (docs stay, submissions blocked).
- **Acceptance:**
  - E2E with `httptest` origin: claim → serve token → verified within one worker tick; wrong/missing token → stays pending with reason; second principal claiming same prefix → 409.
  - Re-verification failure flips state and blocks `/pub/submit` (403) but does not suspend serving.

### WS3.2 Lease state machine + heartbeats
- **Where (new):** `internal/collections/lease.go`; sweeper goroutine started where the server wires background loops in `runPebbleServe` (near crawler wiring `pebble_serve.go:758-766`).
- **What:** `active → grace(72h) → suspended(7d) → purged(60d)`; renewal at any pre-purge state reactivates instantly. Heartbeat = envelope kind `heartbeat`; **nonce-chained**: each heartbeat response carries `next_nonce`; the next heartbeat must sign it (**OPEN DECISION D1** — recommended, no inbound push needed since appstore apps can't receive unsolicited messages).
- **In-memory gate state:** suspended-collection set rebuilt from `'C'` at startup, mutated by sweeper, consumed by WS6.
- **Acceptance:**
  - Clock-injected unit tests for every transition incl. reactivation from grace and suspended.
  - Heartbeat replay (old nonce) rejected; out-of-order seq rejected.
  - E2E: submit docs → indexed → stop heartbeats → advance clock 72h → docs rank-decayed (grace); +7d → absent from `/search`; renew → present again without re-crawl; +60d → purged via WS1.3 (verify HNSW/postings shrink).

### WS3.3 Publisher submission
- **Where:** `pebble_serve.go` new `POST /pub/submit` (urls | sitemap_url | `replace_prefix`); pushes lane 0 via WS2.1; sitemap path reuses `internal/crawler/sitemap.go` streaming seeder.
- **What:** Per-collection quotas (pages, bytes/day). `replace_prefix` semantics: on successful index of the new set, docs under the prefix not in the new set are purged (supersede; fixes version pollution) (**OPEN DECISION D3** — replace vs coexist; replace recommended).
- **Acceptance:**
  - E2E: submit 10 URLs → all crawled from lane 0 ahead of seeded lane-2 noise → docs carry `CollectionID`; over-quota submit → 429.
  - `replace_prefix` test: v1 set indexed → submit v2 with replace → only v2 retrievable; doc count correct.

---

## WS4 — Open frontier: fetch contributions (quarantine → verify → promote)

### WS4.1 Work + submit API
- **Where:** `pebble_serve.go` new `POST /work/claim`, `POST /work/submit`, `POST /work/renew` (signed, WS0.3); claims from WS2.3.
- **What:** Fetch-job payload to contributor: `{claim_id, url, robots_ok: true, politeness_deadline, max_body: 5MB, accepted_content_types}` — mirrors server-side gates (content-type allowlist `internal/crawler/crawler.go` ≈:1131; body cap ≈:1139-1145; robots checked **server-side before handing out**, reusing `internal/crawler/robots.go`). Submission: `{claim_id, final_url, http_status, fetched_at, raw_sha256, extracted: {title, text, lang, published_at, …}}` in an envelope. Lands in `'S'` quarantine only.
- **Acceptance:**
  - Submit without claim / expired claim / wrong principal → 4xx, nothing staged.
  - Robots-disallowed URLs never appear in handed-out claims (test with fixture robots.txt).
  - Oversized text / disallowed content-type rejected at intake (mirror of crawler gates).

### WS4.2 Content verification pipeline
- **Where (new):** `internal/verify/` — `content.go`, `scheduler.go`, `simhash.go`.
- **Reuse:** re-fetch via crawler fetch path incl. `remote_fetcher.go` CF-worker transport (`internal/crawler/remote_fetcher.go:37,68`); extraction/normalization from `internal/crawler/parse.go` (drop-tags ≈:228-242, boilerplate tokens ≈:244-285, `collapseWhitespace` ≈:601-616) so both sides compare like-for-like.
- **What (new):** 64-bit simhash over word shingles (no near-dup machinery exists today — ContentSHA is exact-only, see `crawler.go` ≈:674-767). Verdicts: `match` (hamming ≤ 6), `partial` (≤ 14), `mismatch`. Sampling per principal tier (Appendix C); `partial` is neutral (page churn, geo-variance); repeated `mismatch` pattern → tier drop / ban via WS0.2.
- **Promotion gate chain:** schema checks → sampled re-fetch compare → `authority.Score(host)` floor (reject < 0.2 unless lane 0; `internal/authority/authority.go`) → optional judge pass (`internal/judge/judge.go`, fail-open as in `/answer` use at `pebble_serve.go` ≈:5402-5421) → exact-dup check (ContentSHA) → `UpsertDocument` + `IndexDocument` with provenance (WS1.2/WS1.3 fields) → reputation bump.
- **Acceptance:**
  - Red-team e2e (new `zz_contrib_redteam_test.go`): honest submissions promote; tampered text (entity swap / injected spam paragraph) at 100% sampling → caught, staged-rejected, principal counters reflect it; after ban, principal's previously-promoted docs purged (I4).
  - Churn tolerance: same page with rotated ad block / timestamp → `match` or `partial`, never `mismatch` (fixture pair tests for simhash thresholds).
  - Quarantine TTL: unverified entries older than 14 days dropped; `'S'` family size bounded in soak test.
  - Throughput: verification worker sustains ≥ 5 re-fetches/s without disturbing crawl politeness (shares hostGate `internal/crawler/gate.go`).

### WS4.3 Reputation, tiers, sampling
- **Where (new):** `internal/reputation/` (uses WS0.2 counters; tier table Appendix C).
- **What:** Tier function over verified history; emits sampling rate (1.0 → 0.05 floor), per-tier quotas, lane access. All transitions logged + metric'd.
- **Acceptance:**
  - Property test: expected accepted-bad-before-ban ≈ (1−r)/r honored by simulation within tolerance; r=1.0 for fresh principals (I5).
  - Tier promotion requires ≥ N verified-good (default 200) AND age ≥ 7 days; demotion immediate on mismatch pattern.

---

## WS5 — Embed contributions (server-defined text only — I6)

### WS5.1 Index-level model pin (blob-sha, not name)
- **Production reality** (verified 2026-06-12 on GH200): embedder is **`nomic-embed-text-v1.5`** (137M params, 768-dim, F16 GGUF, `nomic-bert` family, ~274 MB). Served via 8 ollama replicas (ports 11435-11442); cosift `embeddings.urls` round-robins (`internal/embed/client.go` `RoundRobinEmbedder`). On-disk blob: `sha256-970aa74c0a90ef7482477cf803618e776e173c007bf957f635f1015bfcfef0e6`.
- **Where:** HNSW persistence meta blob (`internal/index/hnsw_persist.go` meta ≈:28-34, magic `HSW1`) — add `model_family` (string), `model_blob_sha256` (32 bytes), `dim` (already implicit). Surfaced in `/stats` (`buildStatsBody` `pebble_serve.go:3084`) and `/healthz`. The blob sha is the strongest pin — name `nomic-embed-text:latest` can drift; the blob bytes can't.
- **Server-side blob verification:** at startup, hit each `embeddings.urls` endpoint with a known fixture text, compare returned vector against a checked-in fixture vector (cosine ≥ 0.9999); mismatch → refuse to start with mode `dense_enabled`. Fixture lives at `testdata/embed_fixture.json` (text + expected 768-float vector). Fixes a latent operator-side gap (Pebble path currently stores no model identity at all; only legacy SQLite did).
- **Acceptance:**
  - Open index with mismatched configured embedder → startup warning + dense disabled + `/healthz` reports `degraded`.
  - Matching blob → normal; snapshot round-trip preserves pin.
  - Fixture cosine check runs against each of the 8 replicas at startup; any replica failing fixture removed from rotation.

### WS5.2 Embed jobs + recompute verification
- **Where:** job source = the embed-backfill scan (docs lacking vectors — `handleEmbedBackfill` `pebble_serve.go:2383`, chunker `internal/index/chunk.go` 320/64 words, truncation `truncateForEmbedLite` ≈:2485); claims via WS2.3; intake via `/work/submit`; insertion via `AddPassageBatch` `internal/index/hnsw.go:471` (normalization in-place `internal/index/vector.go` ≈:275-287).
- **What:** Claim payload: `{claim_id, model_family, model_blob_sha256, dim, chunks: [{doc_id, offset, length, text}]}` (server sends the exact text — contributor never chooses it — **I6**). Submission: vectors (dim-checked, finite-checked, then unit-normalized server-side). Verification: recompute a sample with local embedder; **calibrated cosine threshold ≥ 0.9999** (same-blob F16 cross-machine determinism in practice; calibrate on GH200 vs Jetson during WS15.3b — refuse to lower if measurement diverges). Batch fails if any sampled vector fails. Contributors whose reported `model_blob_sha256` ≠ index pin: claim refused at `/work/claim`, never handed work.
- **Contention framing:** the embed bottleneck is not raw watts (~3 GB GPU, 0-30% util on the H100) — it's **vLLM contention on the same H100 during /answer**. Offloading embeds gives `/answer` latency headroom and unblocks moving to a bigger embedder later (e.g., nomic-embed-text → bge-large) without regressing answer p95.
- **Acceptance:**
  - E2E with mock embedder: honest vectors promote into HNSW (searchable via `/search?mode=dense`); perturbed vector (cosine 0.99 vs recompute, below 0.9999 threshold) at sampling=1.0 → batch rejected, reputation hit.
  - Wrong `model_blob_sha256` / wrong dim / NaN → rejected at `/work/claim` or `/work/submit` (400), never staged.
  - Dogfood: Jetson node runs worker app (WS10) bundled embedder against staging box; ≥ 10k chunks contributed, sampled-verified, zero false rejects at calibrated threshold.
  - Contention metric: with contributors active during a synthetic `/answer` load test (10 rps), p95 `/answer` latency drops ≥ 15% vs same load with local embed-backfill running.

### WS5.3 Bundled embed runtime (contributor side, NO ollama dependency)
- **Where (new, in `pilot-protocol/cosift-node/` — see WS10):** `embed/` package wrapping a llama.cpp sidecar.
- **What:** The worker app bundle ships:
  - `bin/embed-runtime` per arch — llama.cpp built in `embedding` mode (server, unix-socket transport), ~5-10 MB per arch. MIT license, redistributable.
  - `models/nomic-embed-text-v1.5.f16.gguf` — the exact blob (`sha256:970aa74c…`) the GH200 serves. ~274 MB. Apache 2.0 license, redistributable.
  - `embed_fixture.json` — same fixture used in WS5.1, lets the app self-test on first start.
- **App lifecycle:** on `node.worker_start`, app spawns embed-runtime with `--model models/…gguf --uds $APP/embed.sock --embedding --pooling mean`; on shutdown kills the child. Health-check by running the fixture text through the sidecar and asserting cosine ≥ 0.9999 vs the fixture vector — refuses to enter the worker loop if it fails (would mean tampered weights or wrong build).
- **Why a sidecar, not cgo into the Go app:** keeps the Go binary cross-compile-clean (no cgo cross), keeps llama.cpp updates independent of the app, lets the sandbox kill the child without restarting the parent. Standard pattern; well-trodden.
- **Acceptance:**
  - Install on linux/arm64 (Jetson) and darwin/arm64 → embed-runtime spawns within 5 s; fixture self-test passes; `node.worker_stats` reports `runtime_ready: true, model_blob_sha256: 970aa74c…`.
  - Tampered model file (one byte flipped) → fixture cosine fails → worker refuses to start; structured error returned to operator via `node.worker_start`.
  - Cross-runtime fidelity test: same 200 fixture texts embedded by (a) GH200 ollama and (b) bundled llama.cpp → all pairs cosine ≥ 0.9999. Run in CI to detect any llama.cpp version that drifts from ollama-served output.
  - No external dependency required: `pilotctl appstore install io.pilot.cosift-node && node.worker_start` works on a freshly-imaged machine with no ollama / no python.

---

## WS6 — Retrieval gating (grace decay, suspension, bans)

- **Where:** single choke point — post-`retrieve()` filter/enrichment loop shared by all retrieval handlers (`pebble_serve.go` ≈:3700-3750, `retrievalFilters` at `:6742`); decay precedent `decayMultiplier`/`applyTimeDecay` ≈:4657-4677.
- **What:** Check docMeta v2 `collection_id` (WS1.2) against in-memory suspended set (WS3.2) → drop; collection in grace → multiply score by configurable factor (default 0.5), reusing the multiplicative-decay pattern; doc principal banned (rare transient window before purge completes) → drop.
- **Acceptance:**
  - Unit: hits from suspended collections absent across `/search`, `/query`, `/answer`, `/research` (one test per handler — they share the loop but assert anyway, I8).
  - Grace decay changes ordering deterministically in a fixture corpus.
  - Overhead: enrichment loop p95 unchanged within noise on a 1k-hit benchmark (`bench_compare_test.go` pattern).

---

## WS7 — Config

- **Where:** `internal/config/config.go` — new top-level `Contrib` section beside `Cluster` (≈:57-88) / `Server` (≈:91-103); example file `cosift.json.example`; doc `docs/CLI-FLAGS.md`.
- **What:**
  ```jsonc
  "contrib": {
    "enabled": false,            // master kill-switch (default OFF)
    "publishers_enabled": false,
    "workers_enabled": false,
    "lane_weights": [50,30,15,5],
    "lease_ttl_fetch_s": 600, "lease_ttl_embed_s": 300,
    "grace_h": 72, "suspend_d": 7, "purge_d": 60,
    "sampling_floor": 0.05, "tier_promote_min_ok": 200,
    "quotas": { "fetch_per_day": 2000, "embed_chunks_per_day": 50000, "pub_pages_per_day": 1000 },
    "simhash_match_hamming": 6, "simhash_partial_hamming": 14,
    "embed_verify_cosine": 0.99,
    "multi_attestation": false   // OPEN DECISION D4
  }
  ```
- **Acceptance:** defaults compile to a fully inert server (no new routes mounted when `enabled=false` — route-table test); every field has an env override following the existing `COSIFT_*` pattern; `doctor` (zz_doctor_runner_test.go surface) validates ranges.

## WS8 — Observability

- **Where:** `handleMetrics` `pebble_serve.go:3267`, `handleStats` `:3020`, `handleQueue` `:2894`; `assets/openapi.json` + swagger assets.
- **What:** Prometheus: `cosift_lane_depth{lane}`, `cosift_staging_depth`, `cosift_verify_total{kind,verdict}`, `cosift_principals{tier}`, `cosift_collections{state}`, `cosift_work_claims_active`, `cosift_embed_external_chunks_total`, `cosift_purged_docs_total{cause}`. `/queue` gains per-lane breakdown + open-claim counts (contributor storefront — deliberately public). `/stats` gains `collections` + `contrib` blocks. OpenAPI documents `/pub/*`, `/work/*`, envelope header, problem types.
- **Acceptance:** metrics appear with zero values when feature off; soak test scrapes parse with `promtool check metrics`; openapi.json passes `swagger-cli validate` (or the repo's existing check); `/queue` p95 unchanged (it's public + hot).

## WS9 — Overlay transport (Pilot-only for contribution traffic — I9)

This is not a "expose it on the overlay too" workstream — it is the only transport for contributions.

- **Where:** new `cmd/cosift/pilot_listener.go`. Web4 primitives: daemon IPC `CmdBind` (`web4/pkg/daemon/ipc.go` ≈:26-75; remember the `/tmp/pilot.sock` cleanup gotcha from cluster memory); gateway `Map` (`gateway/gateway.go` ≈:33-78) is the fallback if the IPC path is blocked.
- **What — three coupled changes:**
  1. **Pilot listener with verified peer identity.** Cosift opens an overlay listener via `CmdBind` on virtual port **7700**. Each accepted stream's remote pubkey comes from the Pilot tunnel handshake (cryptographically authenticated by Pilot itself — see `pkg/daemon/ports.go` ≈:357-371 listener `AcceptCh`). The listener wraps each stream as an in-process `http.Handler` invocation with the remote pubkey stamped into request context under a sentinel key `pilotPeerPubKey`.
  2. **Envelope ↔ tunnel identity binding.** The signed envelope middleware (WS0.3) reads `pilotPeerPubKey` from context and asserts `envelope.PrincipalPub == ctx.PilotPeerPubKey`. Mismatch → 401, principal counter bumped (this is a Sybil-ish indicator). For the HTTP/loopback dev path, this check is bypassed only when the request originates from `127.0.0.1` AND a `Contrib.AllowHTTPDev` config flag is set (default false).
  3. **Public HTTP surface stripped of contribution routes.** `/pub/*` and `/work/*` are removed from the Caddy reverse-proxy config; they bind on cosift's HTTP server but are 404 to any non-overlay-non-loopback caller. The Caddy site config (`/etc/caddy/Caddyfile` on GH200) gains an explicit `respond /pub/* 404` and `respond /work/* 404` ahead of the `reverse_proxy` directive. (`encode gzip` stays out — known SSE killer from prior memory.)
- **What overlay consumers see:** `/pub/*`, `/work/*`, `/queue`, `/healthz` (read-only). `/admin/*` is **forbidden via overlay** (route allowlist enforced at the listener wrapper, not just at handler-time).
- **Acceptance:**
  - From a second Pilot node with a fresh pubkey K: `net.dial` → `/work/claim` with envelope signed by K → succeeds end-to-end over overlay.
  - From the same node: `/work/claim` with envelope signed by K' ≠ K → 401 `envelope_peer_mismatch`, counter on K bumps.
  - From the public internet via Caddy: `/work/claim` → 404 (route stripped); `/pub/submit` → 404.
  - From loopback with `Contrib.AllowHTTPDev=false`: same → 404. With flag on (dev only): bypasses tunnel-identity check, still requires valid envelope.
  - `/admin/*` over overlay → 403 (allowlist refuses).
  - GH200 daemon restart re-binds within 10 s (systemd retry loop, IPC reconnect); listener test covers daemon-death-during-stream cleanup.
  - `docs/API.md` documents the overlay-required posture and the dev-flag escape hatch.

## WS10 — `io.pilot.cosift-node` appstore app (publisher + worker; self-contained)

- **Where (new repo/dir):** `pilot-protocol/cosift-node/` modeled on `pilot-protocol/wallet` (manifest schema `app-store/pkg/manifest/manifest.go` ≈:23-155; IPC dispatcher `app-store/pkg/ipc/server.go` ≈:22-95; envelope `pkg/ipc/envelope.go` ≈:33-41). Bundles the embed runtime (WS5.3) — **no external ollama / python / model-download steps for the user**.
- **What:**
  - Manifest: id `io.pilot.cosift-node`, `exposes`: `node.help`, `node.claim_prefix`, `node.submit_docs`, `node.status`, `node.retire`, `node.worker_start`, `node.worker_stop`, `node.worker_stats`, `node.embed_runtime_status`. Grants: `net.dial` (overlay to cosift broker), fs on `$APP/`, spawn-child for the bundled embed sidecar. Per-app identity `$APP/identity.json` (supervisor passes `--identity`) is **both** the appstore-app key and the cosift principal key — single key throughout. **OPEN DECISION D5**: one app two roles (recommended) vs two apps.
  - **All broker traffic goes over Pilot (I9).** The app uses `net.dial` to virtual port 7700 on the broker's overlay address; **never** dials the broker's public HTTPS surface. Configuration carries `broker_overlay_addr`, not a URL.
  - Publisher role: registers principal, claims prefix, drives `/pub/*`, runs heartbeat loop (6h, nonce-chained). Uninstall ⇒ identity destroyed ⇒ heartbeats stop ⇒ lease lapses — the "active only while installed" semantics enforced by key lifecycle.
  - Worker role: loop = claim → either (a) fetch (own egress; honor handed-out politeness deadline + robots/UA fields) or (b) embed via bundled sidecar (WS5.3) — claim refused server-side if app's reported `model_blob_sha256` doesn't match index pin → sign → submit. Honors `worker_stop`; backs off on 429.
  - First-run sequence: identity load-or-create → spawn embed sidecar → run fixture self-test (WS5.3 acceptance) → register principal with broker (overlay) → publish `node.help` schema → wait for IPC. If sidecar self-test fails, app stays alive for `node.help` / `node.status` but returns `runtime_ready: false` and refuses worker_start.
- **Acceptance:**
  - `node.help` returns full method/param schema; advertises `embed_runtime: { family: nomic-embed-text-v1.5, blob_sha256: 970aa74c…, dim: 768, runtime: llama.cpp }`.
  - Clean-machine install (no ollama, no python): `pilotctl appstore install io.pilot.cosift-node` → spawn → ready → `node.worker_start` produces signed-and-submitted embed contributions within 60 s.
  - Publisher path: `node.claim_prefix` + well-known token on a test origin → verified via overlay → docs live; `pilotctl appstore uninstall` → (clock-advanced on broker) suspension. Reinstall → new key ⇒ re-claim required and succeeds via still-present well-known token.
  - Identity binding (I9): tamper with envelope to use a different pubkey from the overlay tunnel → broker rejects with `envelope_peer_mismatch`; counter incremented on tunnel's pubkey, not on forged envelope.
  - Worker soak: 24h on Jetson, ≥ 99% of claims either submitted or expired cleanly (no leaks), zero signature failures; embed sidecar memory steady (< 1.5 GB RSS), no leaks.
  - Signing cross-check uses WS0.1 fixture vectors.
  - All broker calls visible in pilot tunnel logs; if `net.dial` to overlay fails (daemon down), app surfaces structured error and retries with backoff (does NOT silently fall back to HTTPS).

## WS11 — Packaging & catalogue (chunky bundle, deliberately)

- **Where:** bundles per `pilot-protocol/catalog/README.md` (tar.gz: `manifest.json` + `bin/<binary>` + extra files; sha256 pinned manifest→binary, catalogue→bundle); catalogue entry in `pilot-protocol/web4/catalogue/catalogue.json`.
- **Bundle contents (per arch):**
  - `manifest.json` (~2 KB)
  - `bin/cosift-node` Go binary (~25 MB)
  - `bin/embed-runtime` llama.cpp embedding server (~8 MB)
  - `models/nomic-embed-text-v1.5.f16.gguf` (~274 MB; Apache 2.0)
  - `testdata/embed_fixture.json` (~30 KB)
  - **Total per arch: ~307 MB compressed (~310 MB uncompressed).**
- **Why F16 and not quantized:** matches the server blob byte-for-byte, enabling cosine ≥ 0.9999 verification (WS5.2). Q5_K_M would save ~180 MB per bundle but lower the threshold to ~0.998, weakening the rejection signal. Worth offering Q5_K_M as a second variant later (manifest `variants` list) if download size hurts adoption.
- **Manifest extension:** add a `data` array alongside `binary`, each entry `{path, sha256, size}` — supervisor verifies every entry's sha256 on each spawn (`app-store/INTEGRATION.md` ≈:120-137 already verifies the binary blob; extend to `data` entries). **OPEN DECISION D8:** require appstore manifest schema extension (cross-repo PR) or stash the GGUF inside a `.tar` blob whose sha256 the supervisor checks as a single file. *Recommended: extend manifest, cleaner long term.*
- **What:** Per-arch builds (linux/amd64, linux/arm64, darwin/arm64); CI job producing bundles + checksums; staged rollout (staging catalogue first if one exists, else version-gate). Bundles published to GitHub Releases or GCS (decide at impl); bundle_url + bundle_sha256 inserted into `catalogue.json`.
- **Acceptance:**
  - `pilotctl appstore install io.pilot.cosift-node` on macOS arm64 + Jetson (linux/arm64) downloads + verifies + installs in < 60 s on a 100 Mbps link; spawns; `ready`.
  - Supervisor sha256 re-verification on restart covers binary + model file; tampered model → refuses to spawn (mirrors WS5.3 fixture check, but earlier).
  - Bundle size displayed clearly in catalogue + install confirmation.
  - License manifest: NOTICE file in bundle lists nomic-embed (Apache-2.0) + llama.cpp (MIT) attributions.

## WS11a — Multi-broker forward-compatibility (federation runway)

Stated requirement: "the actual broker needs to live on pilot, or we should have multiple brokers orchestrating the distribution and ingest of things." Single broker now, multi-broker later — but Phase 1-5 must not paint us into a corner.

- **What stays single in v1:** the broker (one cosift instance on GH200) is the only frontier authority, the only `'P'`/`'C'`/`'A'` ledger, the only verifier. This is fine for launch and removes 80% of the federation complexity.
- **What must be portable from day one (so v2 federation is additive, not a rewrite):**
  - **Principal records (`'P'` family, WS1.1):** signed by the principal themselves on registration (`register` envelope kind in WS0.1). Other brokers must be able to import a principal's signed registration and verify it standalone — no broker-specific secret in the record.
  - **Collection claims (WS3.1):** the well-known token contains the broker's pubkey as part of the challenge, but the verified-domain *fact* is portable — a second broker can re-verify against the same well-known endpoint and accept the same claim.
  - **Attribution rows (`'A'`, WS1.1):** include the broker's pubkey alongside the principal's signature. A doc promoted by broker B1 carries an attribution row signed by B1; broker B2 importing the doc keeps the trail intact.
  - **Reputation deltas:** counters are local-broker state in v1, but expose a signed-export endpoint (`GET /pub/reputation-export?since=…` envelope-authed) so v2 brokers can opt to import. Don't ship the import side in v1.
- **Discovery:** v1 advertises a single overlay address. v2: brokers advertise themselves on a Pilot directory (likely `list-agents` keyword `cosift-broker`), workers/publishers discover and pick a broker by latency/affinity. The app's `broker_overlay_addr` config field (WS10) becomes `broker_overlay_addrs` (plural) — design the schema as plural from v1 to avoid migration.
- **Frontier ownership in multi-broker:** reuse the existing sharding code (`internal/config/cluster.go` `ShardOf` FNV-32 hashing) — each broker owns a hash range; cross-broker forwarding via the existing `ForwardFn` seam (`internal/crawler/crawler.go:316`); claims handed only by the owning broker.
- **Conflict resolution between brokers:** out of scope for this plan. The portability decisions above keep the door open without committing.
- **Acceptance (forward-compat sanity checks, not features):**
  - Principal record round-trip test: register on broker A → export the signed registration → verify in broker B's verification logic against pubkey only (no broker A secret involved).
  - Collection-claim portability test: well-known token includes broker A's pubkey → second broker B (with different pubkey, mock origin) issues its own challenge token → both verify independently without contradiction.
  - App config schema: `broker_overlay_addrs: [string]` parses with a single-element list in v1; tests assert behavior identical to a hypothetical scalar `broker_overlay_addr`.

## WS12 — MCP shim (consumer reach; unchanged scope)

- **Where (new):** `pilot-protocol/cosift-mcp/` — stdio MCP server wrapping `pilotctl appstore call io.pilot.cosift cosift.search|cosift.contents` (+ `cosift.answer` optional). Published to npm for copy-paste Claude Code config.
- **What:** Tools `cosift_search`, `cosift_contents`, `cosift_answer`; missing pilotctl/daemon → actionable error ("install Pilot: …") — transport is overlay-only by design (adoption requirement).
- **Acceptance:** `claude mcp add` flow works on a machine with Pilot (search returns results inside Claude Code); on a machine without Pilot the tool error explains the install path; README covers both.

## WS13 — Docs

- **Where:** `docs/API.md` (envelope, `/pub/*`, `/work/*`, error catalogue), `docs/CONTRIB-NETWORK.md` (operator guide: tiers, sampling, ban/purge runbook), `README.md` (one paragraph + links), `assets/openapi.json` (WS8), publisher-facing quickstart in cosift-node repo.
- **Acceptance:** a publisher can go claim→verified→indexed using only the quickstart (validated by a clean-room run); runbook covers: ban a principal, purge, raise quotas, tune sampling, read verification metrics.

## WS14 — Test & eval matrix (cross-cutting)

| Layer | What | Gate |
|---|---|---|
| Unit | envelope, registry, lanes, leases (clock-injected), simhash, tier function | `go test ./... -parallel 4` green (repo norm) |
| E2E | publisher lifecycle; fetch contribution happy/red-team; embed contribution; purge | new `zz_contrib_*_test.go` suites green |
| Migration | frontier lane re-key; docMeta v1/v2 | counts invariant tests (WS1.2/WS2.1) |
| Soak (staging box) | 24h worker + verifier; staging/lease sweepers | no leaks: `'S'`/`'W'` bounded, goroutines flat |
| Red-team | poisoning at each tier; Sybil burst (100 fresh keys); replay; claim-hoarding | all caught; (1−r)/r property holds |
| Quality eval | existing eval harness (`answer_eval_test.go`, eval-baseline-v2.json) before/after contributions enabled | pass-rate non-regression (±1pt) |
| Perf | retrieval p95 with gating on; `/queue`; claim path 100 rps | within noise vs baseline |

## WS15 — Rollout (GH200) & migration order

1. Deploy Phase 1 binary, `contrib.enabled=false` → **acceptance:** byte-identical behavior on eval suite + 24h error-log clean; lane migration ran once (log marker).
2. Enable `publishers_enabled` with allowlist of 1 (your own test principal) → run WS3 acceptance live.
3. Enable `workers_enabled` allowlisted (Jetson fleet principals) → WS4/WS5 acceptance live; calibrate `embed_verify_cosine` + simhash thresholds with real traffic.
4. Open registration (no allowlist) + publish app to catalogue + announce.
5. Each step reversible by config flag; **rollback:** flag off → routes unmount, sweepers stop; data families are inert when disabled. Caddyfile untouched throughout (no `encode gzip` — known SSE killer). Snapshots (`snapshot.sh`, hourly timer) already cover new families since they live in the same Pebble dir — verify restore in step 1.

---

## Phase → work-item map

| Phase | Items | Exit criterion |
|---|---|---|
| 1 Foundation | WS0.*, WS1.*, WS2.1–2.2, WS6, WS7, WS8 (metrics only) | deployed flag-off on GH200, WS15.1 green |
| 2 Publisher | WS2.3 (pub-only), WS3.*, WS13 (API.md), WS15.2 | own docs collection live + lease lifecycle proven on box |
| 3 Open frontier (fetch) | WS4.*, WS8 (/queue lanes), WS15.3a | Jetson worker contributing crawl, red-team suite green in CI |
| 4 Embed jobs | WS5.*, WS15.3b | measurable embed offload, calibrated thresholds |
| 5 Apps + overlay | WS9 (overlay-only enforcement), WS10 (incl. bundled embed runtime), WS11, WS11a (portability hooks only) | install→claim→serve works from a clean Mac via catalogue with no external deps; I9 holds in adversarial tests |
| 6 Flywheel | WS12, analytics extension of WS8, credits (**D6**), reassess **D10** for federation | MCP in npm; first external publisher onboarded |

## Open decisions (blockers marked ⛔ for their phase)

- **D1** ⛔P2 — Nonce-chained heartbeats (no inbound push). *Recommended: yes.*
- **D2** ⛔P2 — Claim granularity: `host[/path-prefix]`, longest-prefix nesting. *Recommended: yes.*
- **D3** ⛔P2 — Publisher docs replace operator-crawled docs under claimed prefix. *Recommended: replace.*
- **D4** P3 — Multi-attestation (2 independent contributors agree) for low-tier sensitive lanes. *Recommended: config-flag, default off.*
- **D5** ⛔P5 — One app `io.pilot.cosift-node` with both roles. *Recommended: one.*
- **D6** P6 — Credits economy (work → lane-0 quota). *Recommended: defer; quotas suffice at launch.*
- **D7** ⛔P1 — Dependency route for `crypto.Verify`: import `pilot-protocol/common` vs vendor the ~40-line ed25519 wrapper. *Recommended: vendor (keeps cosift module dependency-light).*
- **D8** ⛔P5 — Appstore manifest extension for multi-file bundles (WS11). Extend `manifest.json` schema with a `data` array (each entry sha256-pinned, supervisor-verified per spawn) vs stash the model + binaries inside one `.tar` blob whose single sha256 is in the existing `binary` field. *Recommended: extend manifest — cleaner long-term, and the model file is the first instance of a class of payloads (PQ codebooks, fixtures) that will recur.*
- **D9** P5 — Embed model quantization for the worker bundle: F16 (~274 MB, matches server byte-for-byte, cosine ≥ 0.9999) vs Q5_K_M (~95 MB, cosine ≈ 0.998). *Recommended: F16 in v1; offer Q5_K_M as a manifest `variants` option later if download size is a real adoption blocker.*
- **D10** P6 — Multi-broker federation kickoff: stay single-broker indefinitely vs commit to v2 work (WS11a portability hooks become user-facing). *Recommended: ship v1 single-broker; reassess after 90 days based on broker load and external interest. WS11a deliverables keep the door open regardless.*

## Top risks

| Risk | Mitigation |
|---|---|
| docMeta/frontier migration corrupts hot store | version-byte + run-once marker + checkpoint before deploy + count-invariant acceptance (WS1.2/WS2.1) |
| Content-churn false mismatches punish honest workers | graded verdicts, `partial` neutral, per-principal patterns not single-doc slashing (WS4.2) |
| Embed cosine threshold wrong → mass false rejects | calibrate on GH200-vs-Jetson real noise before freezing (WS5.2 acceptance) |
| New auth surface bug = open write path | I1–I8 invariant tests in CI; routes unmounted unless enabled; allowlist phases (WS15) |
| Verification cost swamps box | sampling floors, verifier shares hostGate politeness, staging TTL bounds queue (WS4.2) |
| Branch drift during long build (external process switches cosift checkout) | per repo memory: verify `git branch --show-current` before every commit |

---

## Appendix A — Envelope canonical bytes

```
ver(1) ‖ kind(1) ‖ pub(32) ‖ seq(8,be) ‖ claim_id(16; zero for register/claim kinds)
       ‖ body_sha256(32) ‖ model_id_len(2,be) ‖ model_id ‖ sig(64) over all preceding bytes
```
Transport: base64 in `X-Cosift-Envelope`; body hash binds the HTTP body. Server state per principal: `last_seq`, `pending_nonce` (heartbeats).

## Appendix B — State machines

```
Collection: claimed → verified → active ⇄ grace(72h) → suspended(7d) → purged(60d)
                         │ re-verify fail → unverified (serving continues, writes blocked)
Principal:  registered → tier{new→basic→trusted} ; mismatch-pattern → banned → purged
WorkLease:  open → claimed(TTL) → submitted → verified|rejected ; TTL expiry → open(attempts++)
Staged:     received → sampled? → promoted | rejected ; age>14d → dropped
```

## Appendix C — Tier table (initial values; config-tunable)

| Tier | Entry condition | Sampling | Fetch/day | Embed chunks/day | Lanes |
|---|---|---|---|---|---|
| new | registration | 1.00 | 200 | 5,000 | 2,3 |
| basic | ≥200 ok ∧ ≥7d ∧ 0 mismatches/30d | 0.25 | 2,000 | 50,000 | 1,2,3 |
| trusted | ≥2,000 ok ∧ ≥30d ∧ operator approve | 0.05 | 10,000 | 250,000 | 0,1,2,3 |
| banned | mismatch pattern / operator | — | 0 | 0 | — |

Expected un-caught bad submissions per burned identity ≈ (1−r)/r: new=0, basic=3, trusted=19 — and purge (I4) reverses survivors.
