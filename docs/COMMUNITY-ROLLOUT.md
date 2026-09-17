# Community release: operator handoff

**2026-09-17 rollout update:** [PR #58](https://github.com/pilot-protocol/cosift/pull/58)
merged at `c8ea845`, and the authorized v0.2.7 release completed successfully in
[workflow 35255963898](https://github.com/pilot-protocol/cosift/actions/runs/35255963898).
Andrei's auth/MCP companions are merged and serving healthy production Cloud Run
origins; the installer is public at v0.4.0/v1. The signed engine is healthy with
HNSW ready. Caddy switched the public service to the shared community gateway
at 18:23:08 UTC, with login at the public entry and operational routes blocked.
The community unit uses the shared binary and its backup timer is enabled. See
[SHARED-ACCOUNTS.md](SHARED-ACCOUNTS.md) and its
[verification record](SHARED-ACCOUNTS-VALIDATION.md) for confirmed revisions,
direct URLs, verified real email/shared-account and public request/MCP checks,
and the remaining acceptance scope.

Deploy only an explicitly approved commit after its review and release gates
pass. Reviewing or testing this PR does not itself authorize a release tag,
published assets, updater activation, or production changes. Use the controlled
sequence below for an authorized rollout.

## Retained rollback baseline (2026-09-16)

Before the September 17 rollout, production was restored to the original v0.2.5
engine and original Caddy routing. The community service/backup timer and engine
updater were stopped/disabled. This records the rollback baseline, not the state
of an in-progress or completed v0.2.7 cutover. Account data and rollback backups
were retained; record each subsequent service/routing change during rollout.

The engine binary and config were compared byte-for-byte with their original
backups. Engine PID was unchanged during the public-routing rollback. The public
health endpoint returned `{"status":"ok"}`. v0.2.6 is withdrawn/prerelease and must
not be selected as a release candidate.

## Review and validation

The merged tree contains the exact revert of the draft ranking changes from #54, the
public authentication entry and operational-route restrictions from #57, and
mode-specific limits plus contribution quality screening. The community/CLI,
local artifacts and credit ledger implementation already merged through #55 is
part of the release's complete tree. The shared-account integration adds the
auth/MCP connections and CLI session handoff. Compare the resulting tree and
representative query behavior against v0.2.5 when accepting the release.

Run with Go 1.26.8 and `GOWORK=off` when a parent workspace uses an older Go version:

```sh
GOWORK=off go vet ./...
GOWORK=off go test -race -timeout 10m ./...
GOWORK=off make smoke
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOWORK=off go build -o /tmp/cosift-community-candidate ./cmd/cosift
node --check internal/community/web/app.js
node --test internal/community/webtests/*.test.cjs
```

The [validation report](COMMUNITY-VALIDATION.md) records the real browser/CLI
flows, regression fixes, fixture boundaries and remaining activation checks.

Normal PR CI runs formatting, web logic regressions, vet, Linux ARM64 compilation, full race tests and
coverage. It does not deploy. After approval, the release workflow builds and
signs five platform binaries. Pin the approved commit and verify SHA256 and
minisign against the installed public key before installing any binary. Never
reuse the withdrawn v0.2.6 artifacts.

## Deployment sequence after explicit approval

1. Keep the engine updater disabled. Record the approved commit, current binary
   version and service configuration. Preserve the current binary, engine JSON,
   Caddyfile and community unit/drop-ins in a timestamped private directory.
   Take a SQLite-consistent community backup, including the ledger and pending
   artifacts; use the existing corpus snapshot/checkpoint procedure.
2. Verify the signed artifact's checksum, signature and version. Validate the
   proposed Caddyfile before installing it. Confirm that the backend has a
   configured chat model, the matching embedding model/dimension, and the admin
   token. Keep tokens out of logs, command arguments and the repository.
3. Install the approved engine binary and restart only the existing engine
   process. Do not run a second full corpus instance on the production host.
   Wait for `/healthz` and inspect loopback `/stats` until HNSW loading is ready.
   Compare representative lexical, dense, Answer and Research queries to the
   retained v0.2.5 baseline before routing users to the candidate.
4. Verify the authenticated `/admin/community-moderate` and
   `/admin/community-enqueue` routes exist. A safe fixture must pass moderation;
   a blocked/uncertain fixture must never reach indexing or earn credits. Do not
   route failed moderation to the old unguarded crawl endpoint. v0.2.5 lacks
   these guarded routes, so deploying only a portal cannot activate ingestion.
5. Install the community service with its private database directory and
   root-owned environment file. The repository unit uses `/home/ubuntu/cosift`.
   The prior experiment left a `standalone.conf` drop-in that instead points to
   `/home/ubuntu/cosift-community`; remove that override for the shared-binary
   strategy, or deliberately update and version-check both binaries. Run
   `systemctl daemon-reload` before starting the portal. Never leave it using an
   old standalone binary while upgrading the engine.
6. Check `/api/limits`, email-code login/logout, interest persistence, saved
   requests, sample CSV, and CLI guest/member requests over loopback first.
   Enable the reviewed Caddy routing only after these checks pass. Confirm the
   root routes to signup/login, public operational/admin/debug routes are
   blocked, and both public hostnames use the same portal quotas.
7. Verify a controlled safe URL contribution and a local artifact submission
   using matching text, metadata and embeddings. Credit only newly indexed
   content; repeat the same contribution and confirm no duplicate reward.
   Retry a completed submission with its original ID and confirm that the engine
   replays its durable receipt. Include Pebble receipt metadata in backups;
   upgrade the engine before the portal to preserve rewards on delivery retries.
   Remove only explicitly created QA accounts/data according to retention
   requirements. Reject garbage and unsafe fixtures without sending harmful
   material to the corpus. Classifier uncertainty must remain held.
   Verify repeated requests using a saved CLI session, then revoke it and check
   that it cannot be reused. Keep session files out of source control and backups
   shared with other users.
8. If payments are part of the approved rollout, complete the Stripe test-mode
   checks in `docs/STRIPE.md` before supplying live credentials. Verify webhook
   fulfillment, replay protection and refund reconciliation.
9. Enable and test the community backup timer. Inspect logs and service restart
   counts, confirm credit refunds on backend failure, and run the same client
   flows through the public hostname. Leave automatic engine updates disabled
   until the rollout is accepted; enabling them is a separate operator choice.

## Default quotas and public API compatibility

| Operation | Credits per successful authenticated request | Member hard cap | Shared guest cooldown after success |
| --- | --- | --- | --- |
| Search | 1 | 120/minute | 30 minutes |
| Answer | 2 | 20/minute | 60 minutes |
| Research | 3 | 3/10 minutes | 90 minutes |

Every successful authenticated retrieval spends credits from its first request.
Each account receives 1,000 free credits per UTC calendar month; there is no
free per-minute member bypass. Unused credits carry over. The old
`-member-free-rpm` option is deprecated and ignored. Guests share one persistent
IP cooldown across all retrieval modes. The command's `-guest-interval` defaults
to `30m`; Search/Answer/Research multiply it by 1/2/3 after success. For example,
a guest Answer also blocks Search for 60 minutes. Changing modes does not bypass
the outstanding cooldown. Backend failures release guest reservations. Web and
CLI guests share this server policy. Contributions always require authentication.

Reserve credits and a mode slot atomically before dispatch. A failed backend
request must release its slot and refund its credit reservation; an insufficient
balance must prevent backend work. Credit balance never bypasses a hard cap, and
mode counters and the ledger persist across restarts. For rollout acceptance,
verify actual balance changes of -1/-2/-3 for a member's first Search/Answer/Research
request, and -1 for MCP search under that same account. A prior free-quota test is
not evidence that the new metering policy works. Repeat a failure and verify no
net debit; confirm the next UTC monthly grant happens only once per account.
Use the installer's private saved CLI session for repeated commands. Verify
guest cooldown duration for all three modes, cross-mode rejection, restart
persistence, and no consumption after failed backend work.

Public `/search`, `/answer` and `/research` now use the portal and accept GET
with `q`, matching the app/CLI. Existing public POST, streaming, or advanced
native parameters require a client migration; loopback engine access retains
its original interface. All unlisted public routes (including `/query`,
`/find_similar` and `/contents`) return 404; there is no engine fallback. `/chat`
redirects to `/login`. Check consumers before approving this routing change.

## Rollback

Keep the prior binary and configuration available locally throughout rollout.
If engine verification fails, stop the new portal and restore the prior engine
binary/config, then restart the existing engine service and verify its health
and search baseline. Restore the prior Caddyfile and reload Caddy. If only
portal verification fails while the engine is healthy, restore public routing
and stop the portal without restarting the engine unnecessarily.

Retain the community database rather than overwriting it with an old snapshot:
restoring stale credits or account data can lose activity. Current schema
changes are additive; retain the matching binary/schema backup if a database
restore is necessary. Keep the updater disabled during rollback. The restored
v0.2.5 engine cannot process guarded community contributions, so keep the portal
worker stopped with that baseline.

## Known scope

Safety and quality checks are text-based and can make mistakes. Images/video
are not classified. Unreadable, oversized or uncertain pages remain unverified.
Obvious junk is screened before the model; the model handles broader spam and
content judgments. Local embeddings are checked against server computation,
so this first version does not promise server-compute savings. New content earns
10 credits, globally deduplicated by content hash. Stripe subscriptions and
subscriber-only top-ups require live billing configuration, verified webhooks,
and the dedicated restricted portal configuration. See [Stripe activation and
test-mode checks](STRIPE.md). Shared mode verifies email codes through
`cosift-auth`; optional password setup/reset requires a fresh email code.
Standalone local mode still lacks email verification and self-service password
reset. Article authoring and rewards for article views remain outside this release.
