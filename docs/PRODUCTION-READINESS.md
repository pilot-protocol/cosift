# Production readiness follow-up — 2026-09-17

**Shared-account integration update:** see [SHARED-ACCOUNTS.md](SHARED-ACCOUNTS.md) and [its verification record](SHARED-ACCOUNTS-VALIDATION.md). Shared mode now connects to Andrei’s auth/MCP infrastructure. Its live staging and companion-change gates must pass before rollout; earlier standalone checks do not establish shared-mode production readiness.

This follows [the browser and CLI integration sweep](COMMUNITY-VALIDATION.md).
All candidate servers, account databases, corpus writes and load requests ran
locally. The existing production chat and embedding services were accessed
through an SSH tunnel for inference; no candidate code or configuration was
installed on production. Changes remain in PR #58; no merge, deployment,
release, updater activation or Stripe charge was performed.

## Assessment

The core flows have passed local integration, real-model spot checks and quota
concurrency checks. This is evidence for a controlled rollout, not an assertion
that every query, arbitrary contributor page, or full-corpus load is certified.
Payments remain disabled until credentials are supplied and the external Stripe
exercise below is completed. Follow [the rollout runbook](COMMUNITY-ROLLOUT.md)
for the separate activation checks and rollback procedure.

## Issues found and fixed

- Public Caddy fallback routing exposed unlisted native retrieval paths outside
  community quotas. Only explicitly listed app/API/health paths reach the portal
  now; native, operational, admin and unknown paths return 404. The legacy
  `/chat` redirect syntax also returned an empty 200; it now returns 302 to login.
  A disposable real-Caddy test reproduced both issues. CI now runs this test
  with Caddy v2.11.3, matching the installed production version.
- A Search-only member could never spend credits because both free allowance
  and the hard cap were 60/minute. The Search cap is now 120/minute, with 60 shared
  free requests. The shared free reservation is persisted and refunded on
  backend failure, just like mode reservations and charged credits. Restarting
  cannot replenish a live free allowance. Answer and Research caps are unchanged.
- Answer/Research synthesis received only the 1,200-byte display excerpt and
  missed source instructions after the introduction. Synthesis now gets up to
  10,000 bytes/source and 30,000 bytes across selected sources, preserving UTF-8
  boundaries and prompt fencing. Display excerpts and retrieval/reranking order
  are unchanged. Regression tests cover Answer, synchronous Research and streamed
  Research, plus the shared context bound. This does not restore draft PR #54.
- Each CLI command previously required another password login, hitting the
  separate 10-attempts/account/minute authentication limit before the retrieval
  allowance. Explicit `login`/`logout` and `-session-file`/`COSIFT_SESSION_FILE`
  enable session reuse without storing passwords or weakening login throttles.
  Private session files are origin-bound; symlinks, public permissions, invalid
  and expired files are rejected. Guest mode ignores saved credentials. Logout
  revokes only the CLI session. The real-binary regression performs 12 separate
  requests with no email/password after login and checks revocation afterward.
- Revoked/expired cookies previously silently became guest requests. They now
  return 401 and clear the stale browser cookie, preventing unnoticed uncredited
  contributions. Explicit requests with no cookie still receive guest access.

## Executed evidence

| Check | Result and limits |
| --- | --- |
| Real model setup | Local candidate Pebble server used `qwen3.5:9b-fp8` and `nomic-embed-text` (768 dimensions) through loopback tunnels. The small corpus contained three public Go documentation pages; this was not a copy of the 15.7-million-document production corpus. |
| Moderation | 11/11 expected decisions: allowed technical, clinical health, defensive security and historical content; rejected explicit adult material, malware, phishing, extremist recruitment, repetitive garbage and a malicious instruction embedded in malware content; held a login-only page as uncertain. These are text-policy examples, not image/video classification or antivirus certification. |
| Answer | Manually reviewed answers correctly supplied `go mod init` (2.098 s), `go get` (2.655 s), and `go run .` (1.173 s), with source citations. Term matching alone was insufficient: the initial broken answers mentioned commands while saying the sources lacked them. The corrected answers were inspected for actual source-supported instructions. |
| Research | A module/dependency question returned the correct commands and cited steps in 3.198 s. An unsupported future sports-result question returned no fabricated answer (0.718 s). Five examples establish a spot check, not a general accuracy benchmark. |
| Local contribution | Built CLI fetched a fourth Go tutorial, persisted local text/chunks and submitted metadata plus real 768-dimensional embeddings. The local candidate server independently checked the source and every vector, indexed it and awarded exactly 10 credits. Embedding inference used the tunneled model; this did not benchmark a contributor-owned CPU/GPU. |
| Concurrent quotas | 800 Search requests at concurrency 16 across four local accounts: exactly 480 successes and 320 expected quota responses, no unexpected status. Each account used 60 free and 60 paid requests, was then capped, and retained 40 of its initial 100 test credits. |
| Local latency | Successful Search p50 17.86 ms, p95 38.99 ms, max 49.98 ms; batch elapsed 0.843 s. This measured portal bookkeeping plus BM25 over the tiny local corpus, not production search throughput or LLM concurrency. |
| Restart/failure accounting | Regression tests exhaust free allowance, spend credits, reopen the portal, continue spending, hit the hard cap and verify balances; backend failures return the free reservation. |
| Account backup | A SQLite-consistent online backup restored into a separate local database with `integrity_check` passing and matching users, sessions, saved requests, submissions, ledger, quota and payment rows. This does not verify cloud bucket permissions or activate the backup timer. |
| Public routing | Real Caddy local execution exercises both public hostnames, app/API/sample/health paths, payment webhook routing, `/chat` redirect, and denied native/operational/admin/unknown routes. Production `caddy adapt` also accepted the candidate configuration without activating it. |
| Browser logic | Seven Node regressions pass for account changes, logout, cancellation and late responses. Actual browser signup, onboarding, saved requests, CSV upload/download and retrieval flows were exercised in the preceding sweep. |
| Stripe | Existing fake-transport and signed-event tests cover prices, signatures, duplicate/concurrent fulfillment, restart, rollback and refunds. No actual Stripe account or external webhook was tested. |

The full race suite and vet passed again after the final CLI session and
stale-cookie changes, as did the Linux ARM64 and local CLI builds. The real
crawl/serve smoke, seven web regressions and Caddy routing check also passed.
The PR CI runs the race suite, vet, production-target build and web/edge checks.

## Repeatable local gates

```sh
GOWORK=off go vet ./...
GOWORK=off go test -race -timeout 10m ./...
node --test internal/community/webtests/*.test.cjs
GOWORK=off make smoke
GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/cosift-candidate ./cmd/cosift
GOWORK=off GOBIN=/tmp/cosift-caddy-bin go install github.com/caddyserver/caddy/v2/cmd/caddy@v2.11.3
python3 scripts/community-edge-smoke.py --caddy /tmp/cosift-caddy-bin/caddy
```

Model checks used `https://go.dev/doc/`,
`https://go.dev/doc/tutorial/getting-started`, and
`https://go.dev/doc/modules/managing-dependencies` as local seed pages, with
BM25 and query expansion disabled. The additional local-artifact contribution
used `https://go.dev/doc/tutorial/create-module`. To repeat, point an isolated
local configuration at the intended model endpoints, crawl those seeds, and run
the cited questions through the candidate community/engine endpoints. Never run
a second full-corpus instance on the current production host.

## Remaining external activation checks

1. Supply an isolated Stripe **test-mode** API key and matching webhook secret
   through a local secret file/reference. Complete real hosted Checkout, verify
   one 50,000-credit grant, replay the event, cancel another checkout, and perform
   partial/full refunds. Both Cosift Stripe variables were absent in the inspected
   service configuration. Keep billing disabled until this check passes; do not
   infer external success from synthetic signed events.
2. During the approved rollout, compare representative candidate results and
   bounded latency against the retained v0.2.5 full-corpus baseline. The candidate
   has not been installed on that corpus. The inspected host had about 55 GB
   available RAM; upgrade the single existing engine under the runbook instead
   of opening another full index instance. Verify the actual HTTPS proxy path,
   shared account quotas, restart recovery and cloud backup/restore operations.
3. Keep engine and portal versions matched. Enable guarded contribution fetches
   and the crawler requirements in the runbook, retain durable receipt metadata
   with Pebble backups, and confirm the shared admin credential before activating
   the portal. The inspected portal admin token matched the engine's peer token;
   no secret value is recorded here.

Read-only inspection found the original engine active and the community service
and self-updater inactive. The configuration and on-disk rollback artifacts
remain separate from the reviewed candidate; this report authorizes no activation.


## Metering policy changed after this validation

The recorded free-per-minute member allowance is historical. Current policy
charges every successful authenticated Search/Answer/Research request 1/2/3
credits, including the first request, and grants each account 1,000 free credits
per UTC month. Guest retrieval keeps its separate limits. Prior tests that count
free member requests establish behavior before this change; they do not verify
current deductions. New rollout acceptance must check actual ledger debits from
the first request across web, CLI, and MCP search, no debit after backend failure,
and refusal before backend work when the balance is insufficient. See
[the current operator policy](COMMUNITY-ROLLOUT.md#default-quotas-and-public-api-compatibility).
