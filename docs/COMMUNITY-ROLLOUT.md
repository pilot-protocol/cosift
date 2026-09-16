# Community release: operator handoff

This is a deployment plan, not an instruction to deploy automatically. The user
requested that the implementation remain in a PR until reviewed. Do not create a
release tag, publish assets, enable the updater, or modify production as part of
reviewing or testing this PR.

## Current production baseline (2026-09-16)

Production has been restored to the original v0.2.5 engine and original Caddy
routing. The community service and its backup timer are stopped/disabled. The
engine updater timer is also disabled so another release cannot roll out without
an explicit decision. Account data and rollback backups have been retained.

The engine binary and config were compared byte-for-byte with their original
backups. Engine PID was unchanged during the public-routing rollback. The public
health endpoint returned `{"status":"ok"}`. v0.2.6 is withdrawn/prerelease and must
not be selected as a release candidate.

## Review and validation

The PR contains the exact revert of the draft ranking changes from #54, the
public authentication entry and operational-route restrictions from #57, and
mode-specific limits plus contribution quality screening. The community/CLI,
local artifacts and credit ledger implementation already merged through #55 is
part of the candidate's complete tree. Review the resulting tree against v0.2.5
as well as the PR diff; reverting #54 must receive the normal owner review.

Run with Go 1.26 and `GOWORK=off` when a parent workspace uses an older Go version:

```sh
GOWORK=off go vet ./...
GOWORK=off go test -race -timeout 10m ./...
GOWORK=off make smoke
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOWORK=off go build -o /tmp/cosift-community-candidate ./cmd/cosift
node --check internal/community/web/app.js
```

Normal PR CI runs formatting, vet, Linux ARM64 compilation, full race tests and
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
6. Check `/api/limits`, registration/login/logout, interest persistence, saved
   requests, sample CSV, and CLI guest/member requests over loopback first.
   Enable the reviewed Caddy routing only after these checks pass. Confirm the
   root routes to signup/login, public operational/admin/debug routes are
   blocked, and both public hostnames use the same portal quotas.
7. Verify a controlled safe URL contribution and a local artifact submission
   using matching text, metadata and embeddings. Credit only newly indexed
   content; repeat the same contribution and confirm no duplicate reward.
   Remove only explicitly created QA accounts/data according to retention
   requirements. Reject garbage and unsafe fixtures without sending harmful
   material to the corpus. Classifier uncertainty must remain held.
8. Enable and test the community backup timer. Inspect logs and service restart
   counts, confirm credit refunds on backend failure, and run the same client
   flows through the public hostname. Leave automatic engine updates disabled
   until the rollout is accepted; enabling them is a separate operator choice.

## Default quotas and public API compatibility

| Operation | Member hard cap | Guest hard cap |
| --- | --- | --- |
| Search | 60/minute | 1/minute |
| Answer | 20/minute | 1/5 minutes |
| Research | 3/10 minutes | 1/30 minutes |

Guests also share one request/minute across retrieval and contributions. Members
have 60 shared free requests/minute; one credit pays for each extra request
within the hard caps. Credit balance never bypasses a cap. Backend failures
release mode slots and refund guest allowances/credits. Mode quotas persist
across restarts; the shared member free-attempt counter is process-local.

Public `/search`, `/answer` and `/research` now use the portal and accept GET
with `q`, matching the app/CLI. Existing public POST, streaming, or advanced
native parameters require a client migration; loopback engine access retains
its original interface. Check consumers before approving this routing change.

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
10 credits, globally deduplicated by content hash. Payments, email verification
and self-service password reset are not enabled in this version.
