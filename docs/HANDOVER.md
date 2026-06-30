# cosift — Operational Handover Runbook

**Audience:** the incoming operations agency (the "agency") and the principal (system owner).
**Scope:** giving the agency operational control of the production cosift system while keeping
real user queries and responses confidential from the agency.
**Status of this document:** authoritative handover runbook. Read it end to end before touching prod.

---

## 0. The handover model (read first)

This is the decided model. Everything below implements it.

- **The agency GETS full SSH** to the production GH200 box (host `192.222.56.72`, public name
  `cosift.pilotprotocol.network`). They operate it: deploy, debug, restart, run admin/offline
  tooling, manage the harvesters. Box shell is real root-equivalent control of the corpus and service.
- **Real user queries and responses must stay confidential from the agency.** The resolution has
  two parts:
  1. **No on-box persistence of raw queries.** The on-box query log is disabled or redacted, the
     feedback log is off or redacted, retention is bounded, and no ingress component (Caddy) is
     allowed to log query-bearing URLs. This removes passive/accidental exposure: an operator who
     pokes around the box finds no historical record of what users asked.
  2. **Query observability moves off-box to a principal-controlled passthrough.** A thin reverse
     proxy that the principal owns fronts all requests, terminates ingress, and is the only place
     `{query, ts, latency, endpoint}` is recorded — for the principal, not the agency.

### Residual risk — state this plainly to the agency and the principal

The two measures above stop **passive and accidental** exposure. They do **not** stop a determined
malicious operator. A root operator on the box can:

- attach to the live process or sniff loopback (`127.0.0.1:7777`) traffic and read plaintext
  queries in flight (`tcpdump -i lo`, `strace`/`bpftrace` on the process, an `LD_PRELOAD` shim, etc.);
- wiretap or replace the local LLM / embed calls the service makes;
- add their own logging drop-in and restart the service.

The GH200 still receives **plaintext queries** because it has to search them. Technical hardening
(no logs, localhost-only admin, redaction) raises the cost and removes the easy/accidental paths,
but the gap against an active malicious root operator is **closed by contract and trust, not by
technical means.** Put this in writing in the operating agreement. Do not represent the technical
controls as making queries cryptographically invisible to the operator — they do not.

---

## 1. Secret-ownership matrix

Three ownership classes:

- **principal-only** — only the principal holds the value. Never enters CI, never enters a handover
  tree, never handed to the agency. (The agency can still *operate* the box; they just don't get the
  literal secret in their own control plane.)
- **runtime-on-box** — lives on the box for the service/harvesters to read at runtime. The agency,
  having SSH, can read these; that is accepted. Keep them out of git and CI.
- **agency** — the agency owns/rotates it as part of operating the system.

| Secret | Owner | Location | Rotation |
|---|---|---|---|
| **OpenAI API key** | principal-only **+** runtime-on-box | **MUST be rotated at handover.** Currently sits in `cosift/.env` (`OPENAI=sk-proj-…`) — that value is compromised by being in the working tree and must be revoked. Move the new key to a systemd `EnvironmentFile` at `/etc/cosift/cosift.env`, owner `root:cosift`, mode `0640`, referenced from `cosift-serve.service`. **Never** in CI, **never** in any handover/git tree, **never** in `.env`. | Rotate now (at handover). Rotate again on any suspected exposure. Principal issues the key; the box reads it. |
| **`cluster.peer_auth_token`** | runtime-on-box | `/home/ubuntu/cosift.json` (`cluster.peer_auth_token`). Gates **all** `/admin/*` and `/admin/query-log` etc. on the production `pebble-serve` path. **Currently EMPTY → admin gate is skipped.** See §3. | Set at handover; rotate on operator turnover. Same value must be provisioned to the harvesters (§3). |
| **`server.admin_token`** | runtime-on-box | `/home/ubuntu/cosift.json` (`server.admin_token`). **Note:** this gates only the legacy `serve` path (`internal/server/http.go`). It does **NOT** gate the production `pebble-serve` path — that path uses `cluster.peer_auth_token`. Don't rely on `admin_token` for prod auth. | Low priority; keep set for the legacy path but treat `peer_auth_token` as the real prod gate. |
| **`crawler.remote_fetcher_token`** | runtime-on-box | `/home/ubuntu/cosift.json` (`crawler.remote_fetcher_token`). Shared Bearer between cosift and any remote fetcher. | On operator turnover. |
| **`rerank.api_key` / `COHERE_API_KEY` / `VOYAGE_API_KEY`** | runtime-on-box | `rerank.api_key` in `/home/ubuntu/cosift.json`, **or** env `COHERE_API_KEY` / `VOYAGE_API_KEY` (config falls back to env — see `internal/config/config.go` `APIKey`). Put env values in `/etc/cosift/cosift.env` alongside the OpenAI key. | On provider rotation or operator turnover. |
| **`cosift.pem` (SSH key to the GH200)** | principal-only | Local at `~/Downloads/cosift.pem` (chmod 600). **Never to CI.** CI uses **pull-based** deploy (§4), so the deploy pipeline never needs this key. The agency gets their **own** SSH access (separate key / authorized_keys entry) — do not hand them this exact key; provision per-operator keys so access can be revoked individually. | Rotate when an operator leaves. Per-operator keys make revocation a one-line `authorized_keys` edit. |
| **ed25519 `cosift-app` publisher key** | principal-only | Principal's machine / secret store. App publishing (the pilot app-store artifact) is **independent of backend deploy** — the agency operating the backend does **not** need this. | Principal-controlled; rotate independently of backend secrets. |
| **`CANARY_DISPATCH_TOKEN`** | principal-only → replace before repo handover | GitHub repo secret, consumed by `.github/workflows/notify-canary.yml`. **Currently a personal PAT.** Before handing the repo to the agency, **replace it with a scoped app token** (fine-grained, repo-scoped, dispatch-only) so handing over the repo doesn't hand over a personal PAT. | Replace at repo handover; rotate on operator turnover. |

**General rule:** the only secrets that ever reach git or CI are *none of the above*. CI builds and
signs; it does not hold the SSH key, the OpenAI key, or any runtime secret.

---

## 2. Confidentiality / no-persistence cutover (do this AT handover)

Concrete steps to take on the box before the agency gets the keys.

### 2.1 Disable (or redact) on-box raw query logging

The query log is controlled by the `COSIFT_QUERY_LOG` env var. On prod it is enabled via a systemd
drop-in. Two acceptable resolutions — **pick one** (open decision, see §7):

**Option A — disable entirely (simplest, recommended for clean confidentiality):**

```bash
# Remove the drop-in that sets COSIFT_QUERY_LOG.
sudo rm /etc/systemd/system/cosift-serve.service.d/zz-query-log.conf
sudo systemctl daemon-reload
sudo systemctl restart cosift-serve     # 4–5 min HNSW reload — see §4
```

With `COSIFT_QUERY_LOG` unset, `s.qlogFile == nil` and `qlog()` is a no-op (see
`cmd/cosift/querylog.go`). `/admin/query-log` then returns `501 query log disabled`. The feedback
log keys off the same var (`cmd/cosift/feedback.go`), so it goes dark too.

**Option B — keep observability but redact (if the principal wants on-box latency/empty-rate signal):**

Edit `cmd/cosift/querylog.go` so the persisted record cannot reconstruct a user's query or identity:

- hash or truncate the `Q` field (e.g. store `sha256(q)[:8]` or just the length, not the text);
- drop the `Caller` field entirely (it is `X-Forwarded-For` / `RemoteAddr` today — that is the
  user's IP).

This keeps `{ts, ep, status, ms, bytes}` for health/perf while removing the query text and caller IP.
Rebuild and deploy via §4.

> **Note:** Option B still leaves the *hashed* query on the box, which is a confirmation oracle for a
> guessed query. Prefer Option A unless the principal specifically needs on-box query observability —
> and remember the principal passthrough (§5) is the *intended* home for query observability anyway.

### 2.2 Feedback log

`COSIFT_FEEDBACK_LOG` (or the default beside the query log) records feedback rows. Ensure it is
**off** (unset) or redacted the same way as §2.1. Under Option A it is already off because the
feedback path is gated on `COSIFT_QUERY_LOG` being set.

### 2.3 Bound retention on anything that remains

The code has **no retention/rotation** for the query/feedback logs — `writeQueryLog` only appends
(`cmd/cosift/querylog.go`). If any log remains (Option B, or operational logs), impose retention at
the OS level so nothing accumulates indefinitely:

```bash
# /etc/logrotate.d/cosift
/var/log/cosift/*.log /home/ubuntu/cosift-data/*.jsonl {
  daily
  rotate 3
  compress
  missingok
  notifempty
  copytruncate
}
```

Tune `rotate`/frequency to the principal's retention policy. Under Option A there is nothing to rotate
here, but keep the rule in place as a backstop.

### 2.4 Embed-cache is a timing / confirmation oracle — decide

The embedder caches vectors to disk as `sha256(model \x00 text)` named `.vec` files
(`internal/embed/client.go`). If query embeddings share the cache directory with doc embeddings, the
**presence of a `.vec` file whose name matches `sha256(model\x00<guessed-query>)`** confirms that
query was issued, and file mtime leaks *when*. This is a confirmation oracle, not a plaintext leak.

Decision (open — §7):

- **(a) Separate caches:** point query-time embedding at a non-persistent / `tmpfs` cache (or disable
  caching on the query path) so query embeds leave no on-disk fingerprint; keep the persistent cache
  for doc/eval embeds only.
- **(b) Accept it:** if the corpus is huge and queries are short/generic, an attacker must already
  *guess* the exact query to confirm it. Document the residual oracle and move on.

Default recommendation: separate, if cheap to wire; otherwise accept-and-document.

### 2.5 Forbid a Caddy `log` directive

`GET /search?q=…` and `/answer?q=…` carry the **query in the URL**. A Caddy `log` directive writes
request URLs to disk — that is a raw query log by another name.

- **FORBID** any `log { ... }` block in the production Caddyfile.
- The repo's `scripts/Caddyfile.tls` contains a `log { output file … }` block — **do not deploy that
  variant as-is.** Use the no-log `scripts/Caddyfile` shape (or strip the `log` block) for prod.
- This applies to any front proxy, including the principal passthrough (§5): there, query logging is
  *intended* and lives with the principal — but it must not be duplicated on the box.

### 2.6 Already done — public `/admin/*` leak closed

The public exposure of `/admin/*` and `/debug/*` has **already been closed** on the production Caddy
front via a `handle` block that returns **404 for `/admin/*` and `/debug/*`** from the public
internet. Record this as **DONE** (it was the URGENT fix). Verify it still holds after any Caddy
change:

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://cosift.pilotprotocol.network/admin/query-log   # expect 404
curl -s -o /dev/null -w '%{http_code}\n' https://cosift.pilotprotocol.network/debug/pprof/        # expect 404
```

> The repo Caddyfiles (`scripts/Caddyfile*`) predate this block — the live prod Caddyfile on the box
> is the source of truth for the 404 handles. If you regenerate the Caddyfile from the repo, re-add
> the `/admin/*` + `/debug/*` 404 handles **and** keep the no-`log` rule (§2.5).

---

## 3. Admin auth SOP

**Current state:** `cluster.peer_auth_token` is **EMPTY** in `/home/ubuntu/cosift.json`. Every admin
handler on the `pebble-serve` path guards with `if want := s.cluster.PeerAuthToken; want != ""` —
**empty means the gate is skipped.** Public exposure is blocked by Caddy (§2.6), but locally
everything is open. Set the token so admin actions require a Bearer even from localhost.

### Procedure

1. **Generate a token** and set it in the config:

   ```bash
   TOKEN=$(openssl rand -hex 32)
   # Edit /home/ubuntu/cosift.json: set cluster.peer_auth_token = "$TOKEN"
   ```

2. **Provision the SAME token to the harvesters.** The harvesters already read a `TOKEN` env var and
   send `Authorization: Bearer $TOKEN` to the admin endpoints. Put the token in a shared
   `EnvironmentFile` and reference it from the harvester units:

   ```ini
   # /etc/cosift/harvester.env   (root:cosift 0640)
   TOKEN=<the same hex token>
   ```
   ```ini
   # in each deploy/systemd/cosift-*-harvest.service (and *-refresh.service):
   [Service]
   EnvironmentFile=/etc/cosift/harvester.env
   ```

3. **Fix `deploy/harvesters/cosift-compact.sh`.** It calls `POST /admin/hnsw-compact` **without** a
   token:

   ```bash
   RESULT=$(curl -s --max-time 1800 -X POST http://127.0.0.1:7777/admin/hnsw-compact)
   ```

   Once the gate is on, that call gets `401`. Patch it to send the Bearer:

   ```bash
   RESULT=$(curl -s --max-time 1800 -X POST \
     -H "Authorization: Bearer ${TOKEN}" \
     http://127.0.0.1:7777/admin/hnsw-compact)
   ```

   (and ensure the compact timer unit also pulls `EnvironmentFile=/etc/cosift/harvester.env`).

4. **Restart cosift** to pick up the new config:

   ```bash
   sudo systemctl restart cosift-serve
   # 4–5 min HNSW reload (43.9M nodes). Poll until ready:
   until curl -sf http://127.0.0.1:7777/healthz >/dev/null; do sleep 5; done
   ```

5. **Keep `/admin/*` and `/debug/pprof` localhost-only.** Caddy already 404s them publicly (§2.6).
   The Bearer token is defense-in-depth for the loopback surface; the public block is the primary
   control. Never expose either to the public internet.

---

## 4. Deploy model — signed, pull-based, key-out-of-CI

The deploy pipeline is built so the SSH key never enters CI.

```
 push to main ─► CI (GitHub Actions, .github/workflows/deploy.yml)
                   • GOOS=linux GOARCH=arm64 go build
                   • sign artifact: sha256 + minisign
                   • publish as a GitHub Release
                              │
                              ▼
 GH200 box-side self-update timer (deploy/systemd + deploy/scripts)
                   • polls Releases for a new signed artifact
                   • verifies sha256 + minisign signature
                   • atomic swap into /home/ubuntu/cosift (keeps cosift.prev)
                   • restart cosift-serve, health-gate /healthz (up to 10 min for HNSW reload)
                   • auto-rollback to cosift.prev if /healthz never goes green
```

**Why pull-based:** the box *pulls* a verified artifact rather than CI *pushing* over SSH. CI holds
no SSH key and no runtime secret — it only builds and signs. The minisign verify key is public; the
**signing** key is the principal's (or a CI secret scoped to signing only), never the deploy path's.

**File locations (target layout):**

- `.github/workflows/deploy.yml` — build + sign + publish Release.
- `deploy/scripts/` — the self-update script (pull, verify sha256+minisign, atomic swap, health-gate,
  rollback).
- `deploy/systemd/` — the self-update `.timer` + `.service` units, plus the harvester units already
  present here.

> **Status note:** today the canonical manual deploy is the build/scp/swap procedure in the GH200
> reference (build arm64 → `scp` to `/tmp/cosift-new` → `mv` into place → restart). The pull-based
> pipeline above is the **handover target** so the agency operates without ever holding `cosift.pem`
> in CI. If `deploy.yml` / `deploy/scripts` are not yet wired when you read this, wire them as part
> of cutover; until then, deploys go through the agency's own per-operator SSH key, not CI.

### Every binary deploy is a HARD 4–5 minute outage

Single box, synchronous HNSW load (43.9M nodes) on every restart. There is **no rolling deploy** —
the service is down for the duration of the HNSW reload (4–5 min, allow up to 10 for the health gate).
**Schedule and communicate every deploy.** Health-gate before declaring success:

```bash
until curl -sf http://127.0.0.1:7777/healthz >/dev/null; do sleep 5; done   # {"status":"ok"} when ready
```

---

## 5. Principal passthrough design

A thin, **principal-controlled** logging reverse proxy fronts **all** requests. This is where query
observability and feedback live for the principal — it **replaces** the on-box query log removed in §2.

### What it does

- Terminates public ingress (TLS, the `cosift.pilotprotocol.network` hostname).
- Logs, for the principal only, a minimal record per request: `{query, ts, latency, endpoint}`
  (plus status / response size if wanted). This is the principal's observability substrate.
- Forwards the request to the GH200 (`127.0.0.1:7777` via a private tunnel, or the box's private
  address) and streams the response back.
- Owns retention of *its* log per the principal's policy — this log lives on a host the **agency does
  not control.**

### Hosting options (trade-offs) — the host is an OPEN DECISION for the principal (§7)

| Option | What it is | Pros | Cons |
|---|---|---|---|
| **(a) Small principal VPS** running Caddy / nginx / a tiny Go proxy | A cheap box the principal owns, in front of the GH200 | Full control of logging + retention; arbitrary logic; no third party | Principal runs another box; a hop of latency; needs its own TLS + uptime |
| **(b) Cloudflare Worker** in front | The site is **already behind Cloudflare** — add a Worker that logs `{query, ts, latency, endpoint}` to principal-owned storage (KV / R2 / Logpush) | Zero new infra; rides existing Cloudflare ingress; global edge | Logging logic constrained to the Worker model; observability data sits in Cloudflare (principal-owned, but third-party-hosted) |
| **(c) Local Mac pilot-daemon path** for overlay traffic | For requests arriving over the pilot overlay, the principal's local pilot daemon is already the entry point and can log there | Reuses an existing principal-controlled hop; natural for overlay/app-store traffic | Only covers overlay traffic, not public-web HTTPS hits; the Mac must be up |

A combination is reasonable: (b) for public web traffic (it's already the front), (c) for overlay
traffic, with (a) as the fallback if the principal wants logging logic Cloudflare can't express.

### Hard limit — the passthrough relocates observability, it does NOT hide queries from the box

The GH200 **still receives plaintext queries** — it has to, to search them. The passthrough moves
the *observability record* to a principal-controlled host and removes it from the box; it does
**not** prevent the box from seeing each query in plaintext at request time. That is exactly the
residual-risk point in §0: confidentiality from the agency rests on (no on-box persistence) +
(observability off-box) + (contract/trust for the live-traffic gap). Don't oversell the passthrough.

---

## 6. Control-surface reference

What "the agency gets SSH" actually grants.

### Public endpoints (fronted by Caddy / the passthrough)

`/search` · `/answer` · `/research` · `/find` · `/stats` · `/healthz` · `/metrics` · `/feedback`

These are the user-facing API (`cmd/cosift/pebble_serve.go`). Note `/search?q=` and `/answer?q=`
carry queries in the URL — the reason §2.5 forbids any front-proxy `log` directive on the box.

### Admin endpoints (24, localhost-only, Bearer-gated by `peer_auth_token`)

All under `/admin/*` on `pebble-serve`, e.g.:
`crawl-enqueue`, `allow-domain`, `frontier-purge-host`, `frontier-clear`, `frontier-demote-host`,
`frontier-purge-stale-inflight`, `rss-import`, `crawl-now`, `wet-import`, `wet-import-bulk`,
`site-pack`, `site-submit`, `embed-backfill`, `host-backfill`, `eval-quick`, `hnsw-compact`,
`sitemap-import`, `recrawl-sitemap`, `pq-train`, `pq-encode`, `checkpoint`, `query-log`, `feedback`,
`domains-audit`. Plus `/debug/pprof/*`. **Public access to all of these is 404'd by Caddy** (§2.6);
local access is gated by `peer_auth_token` once §3 is done.

### Offline subcommands bypass HTTP auth entirely

`purge-domain`, `purge-adult`, `frontier-clear`, `gc`, `reembed` (and similar) are **CLI
subcommands** in `cmd/cosift/main.go` that open the Pebble store **directly on disk**
(`openPebbleOrFriendlyErr`). They **do not** go through the HTTP server, so **no token gates them** —
they need only **disk access** to the data dir (`/home/ubuntu/cosift-data/pebble`).

**Implication:** **box shell == full corpus-mutation power.** Anyone with SSH and disk access can
purge domains, wipe the frontier, garbage-collect, or re-embed the entire corpus with zero auth.
This is the core of what "agency gets SSH" grants — it is total operational control of the corpus,
not just the HTTP surface. Treat SSH access as the real trust boundary, and gate it per-operator
(§1) so it can be revoked individually.

> Pebble is single-writer: offline subcommands that mutate the store **cannot** run while
> `cosift-serve` holds the lock. Stop the service first (incurring the §4 outage), or use the
> equivalent admin endpoint against the live process where one exists (e.g. `host-backfill`).

---

## 7. Open decisions for the principal (decide before cutover)

1. **Query-log resolution (§2.1):** Option A (disable entirely) vs Option B (redact: hash/truncate
   `q`, drop `Caller`). Recommendation: A, since the passthrough (§5) is the intended observability home.
2. **Embed-cache oracle (§2.4):** separate query-embeds from doc-embeds (tmpfs / disable on query
   path) vs accept-and-document the confirmation oracle.
3. **Passthrough host (§5):** (a) principal VPS, (b) Cloudflare Worker, (c) Mac pilot-daemon — or a
   combination. This determines where the principal's query observability physically lives.
4. **Per-operator SSH provisioning (§1):** confirm the agency gets per-operator keys (revocable
   individually), not a shared copy of `cosift.pem`.

---

## 8. Cutover checklist (one pass, in order)

- [ ] **Rotate the OpenAI key**; revoke the old `sk-proj-…` from `.env`; write the new one to
      `/etc/cosift/cosift.env` (`root:cosift 0640`); point `cosift-serve.service` at it; remove the
      key from `.env`.
- [ ] Move `COHERE_API_KEY` / `VOYAGE_API_KEY` into `/etc/cosift/cosift.env` too.
- [ ] **Resolve the query log** (§2.1, Option A or B) + ensure feedback log off/redacted (§2.2).
- [ ] Install the `logrotate` backstop (§2.3).
- [ ] Decide + apply the embed-cache choice (§2.4).
- [ ] Confirm **no `log` directive** in the prod Caddyfile (§2.5).
- [ ] Verify `/admin/*` and `/debug/*` still **404 publicly** (§2.6).
- [ ] **Set `cluster.peer_auth_token`** in `cosift.json`; provision the same token to harvesters via
      `EnvironmentFile`; **patch `cosift-compact.sh`** to send the Bearer (§3).
- [ ] Stand up the **principal passthrough** on the chosen host; point it at the GH200; confirm it
      logs `{query, ts, latency, endpoint}` to principal-owned storage (§5).
- [ ] Replace `CANARY_DISPATCH_TOKEN` (personal PAT → scoped app token) **before** repo handover (§1).
- [ ] Provision **per-operator SSH keys** for the agency; do **not** hand over `cosift.pem`; keep the
      `cosift-app` publisher key and `cosift.pem` principal-only (§1).
- [ ] Wire / verify the **pull-based deploy pipeline** so the agency never holds the SSH key in CI (§4).
- [ ] Restart `cosift-serve`, health-gate `/healthz` (4–5 min HNSW reload), confirm green (§3–§4).
- [ ] Put the **residual-risk statement (§0)** in the operating agreement in writing.
