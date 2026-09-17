# Shared-account integration verification — 2026-09-17

Cosift PR #58 merged at `c8ea845913829d5eece571c6facbb4462ab313c9`. The
auth/MCP/installer companions have also merged and released; this record
separates the original local tests from the verified deployment checks below.
The v0.2.7 engine/CLI release finished and the existing-host public cutover
completed on 2026-09-17 at 18:23:08 UTC. Real email and authenticated service
checks are recorded below; final public-client acceptance remained in progress
at that checkpoint. No Stripe charge is claimed.

## Checks completed

| Check | Result / scope |
|---|---|
| `GOWORK=off go test -race -timeout 10m ./...` | Full Cosift regression suite, including account linking, quota isolation, saved data, moderation and CLI regressions |
| `GOWORK=off go vet ./...` | Static checks |
| `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./cmd/cosift` | Production-architecture compilation; signed release tracked separately below |
| `node --test internal/community/webtests/*.test.cjs` | 11 passing tests, including OTP state transitions and stale shared-topic responses |
| `GOWORK=off COSIFT_SMOKE_PORT=17983 bash scripts/smoke-test.sh` | Passed: real public crawl, ingestion, health, search, contents, admin authorization; disposable local index |
| Official auth token vectors | Canonical parsing/trailing bits and HMAC over full token agree with `cosift-auth` fixtures |
| Firestore SDK protocol fixture | Real Google SDK over local gRPC: collection-group lookup, typed verified/revoked timestamps, bans, last-used writes, ambiguous/missing token and IAM-denied errors |
| Auth lifecycle / HTTP contracts | Auth start/verify/revoke, HttpOnly cookies, Google-vs-user credential headers, forwarded client IP, bounded JSON, redirects and upstream error handling |
| Additional failure regressions | Cloud Run IAM failures versus Cosift credential rejection; OTP validation and cleanup after browser cancellation; account linking rollback; CLI credential conflicts and retryable logout; per-client login and per-account MCP limits |
| Installer → real CLI handoff | Installer passes its in-memory token to the compiled CLI, which verifies it and writes a 0600 origin-bound session; a separate CLI process reads credits with no token/server/session flags. All 11 installer handoff cases and CLI race regressions pass; 255 onboarding checks and ShellCheck pass |
| MCP release suite | 266 offline tests plus 18 final deployment tests passed, with Ruff/shell checks and a local AMD64 container embedding inference (384 dimensions, UID 65532); no production search claimed by these fixtures |
| MCP → community → engine | Real MCP ASGI app and engine client against local Go gateway: concurrent accounts receive separate free allowances; repeat over-quota call does not reach the engine; `k=20`, BM25 preserved; account credential stops at the gateway |
| Community client → MCP tools | Real FastMCP protocol: follow/list/unfollow, request/idempotent repeat, missing article coverage; local fake identity/topic store and topic-resolution fixture |
| Auth proxy configuration companion | `bash -n infra/deploy.sh`; existing client-IP resolver and configuration tests pass |
| Companion patch applicability | Each patch matches its pinned base checkout (`git apply --reverse --check` against the patched checkout) |

## Security follow-up

The resumed failure sweep passed the full race/coverage suite and all 11 web
logic tests. Shared-account package coverage is 90.0%; the community package is
79.7%. It found and fixed a Cloud Run error-classification issue: an IAM rejection
must return a retryable service error, rather than declaring the user's token
revoked or account banned. The regression checks preserve both upstream services'
actual JSON authentication contracts and reject HTML/Google origin failures.

An independent official `govulncheck` scan found affected gRPC v1.82.1
call paths ([GO-2026-6348](https://pkg.go.dev/vuln/GO-2026-6348)) and Go 1.26.0
standard-library paths with later security fixes. The PR now requires Go 1.26.8
and gRPC v1.83.2; CI also runs pinned `govulncheck` v1.8.0. Merged auth uses the
same Go/gRPC versions and passed its package scan and CI. The final Cosift
package-level scan reports **0 vulnerabilities in imported packages**. It still
lists GO-2026-5932 for the unused OpenPGP package within the required x/crypto
module; the application does not import that package. CI gates imported packages,
not just the call paths detected by static analysis.

## Manual browser check

The shipped HTML/JavaScript and community handlers were opened in the browser
against a disposable local account database. Auth used fabricated credentials;
topic calls went through the real MCP application/tool handlers with in-memory
storage. Search used a labelled fixture backend. No real emails were sent.

Verified: shared mode shows email-code login rather than password registration;
request/verify opens onboarding; selecting Open source follows it in MCP; the
Topics view lists it; requesting Rust async runtimes records demand; repeating
that request reports it was already requested; authenticated search renders a
source; Save request updates the saved count; sign-out returns to login and clears
private UI state. The automated UI tests additionally check code retry/reset,
concurrent login controls, and cross-account stale-response suppression.

The manual fixture is test-only and never included in the shipped binary. Its
URL is disposable. See `integrations/cosift-mcp/README.md` to reproduce the
cross-repository and browser checks.

## Deployment checks completed

| Component | Evidence |
|---|---|
| Cosift engine/CLI | [PR #58](https://github.com/pilot-protocol/cosift/pull/58) merged at `c8ea845`; [v0.2.7 workflow](https://github.com/pilot-protocol/cosift/actions/runs/35255963898) completed successfully. The public release contains five platform binaries with checksums/signatures and `cosift-minisign.pub` |
| Auth | [PR #1](https://github.com/pilot-protocol/cosift-auth/pull/1) merged at `e6ce919`; revision `cosift-auth-00001-k6b` serves 100%, using production Firestore `(default)` and runtime identity `cosift-auth-runtime` |
| MCP | [PR #1](https://github.com/pilot-protocol/cosift-mcp/pull/1) merged at `fa60d32`; revision `cosift-mcp-00002-lf7` serves 100%, using `(default)` and `COSIFT_ENGINE_BASE_URL=https://cosift.pilotprotocol.network` |
| Public ingress | Auth `https://cosift-auth-udik5erlkq-uw.a.run.app/health` and MCP `https://cosift-mcp-udik5erlkq-uw.a.run.app/health` both returned JSON 200 without Google credentials. Production invoker checks are disabled with no `allUsers` grant; staging checks remain enabled. No organization-policy override was made |
| Credential boundaries | Public auth revoke without a credential returned Cosift JSON 401; public MCP calls with missing or fabricated canonical tokens returned application JSON 401, not infrastructure 503. Production debug routes returned 404; staging debug routes were disabled again after attribution checks |
| Proxy attribution | The actual gateway egress was measured. Private staging kept two different forwarded client addresses in separate buckets; a direct forged forwarding chain resolved to the actual caller. Production trusts only the measured gateway `/32` and Cloud Run peer `/32` |
| Real shared account | Production email-code login succeeded. Against the gateway on host loopback, anonymous `/api/me` returned 401; repeated authenticated identity was stable, credits returned the expected shape, gateway/direct-production-MCP topic lists matched, and authenticated BM25 `k=20` search returned 20 hits. This exercised live Firestore, Secret Manager and MCP with the same real account; no topic mutation was required |
| Signed engine upgrade | Official v0.2.7 Linux ARM64 artifact passed SHA-256/minisign verification against the host trust anchor. One graceful restart completed; HNSW finished loading and the existing engine returned healthy responses. The full-corpus process was not duplicated |
| Retrieval comparison | Both BM25 baseline queries preserved all six results and their order. Hybrid TLS preserved all six URLs, with ranks 3/4 swapped. Search returned 200 in 122–265 ms, Answer in 3.506 s, Research in 2.031 s. The existing weak Go evidence remained visible: Answer declined and Research noted missing specific evidence |
| Live moderation | Useful text was allowed, garbage rejected as low quality, and harmful promotion with embedded prompt injection rejected as phishing. Anonymous moderation returned 401. These moderation fixtures were not indexed |
| Public cutover | Caddy reloaded successfully at 18:23:08 UTC; public/origin `/api/auth/config` report shared mode, root opens login, and operational routes are blocked. The community unit uses the same v0.2.7 binary path as the engine with its old standalone override removed. Community backup upload succeeded and its timer is enabled |
| Local contribution | An authenticated Rust text/metadata/local-embedding artifact passed validation and reached Indexed. The account balance remained zero for this existing-page check; new-content credit fulfillment and durable receipt acceptance are separate checks. Repeating the URL through CSV reported one duplicate and zero accepted |
| Public installer | [PR #1](https://github.com/pilot-protocol/cosift-install/pull/1) merged at `d18edef`. Full Git history and all issue/PR/release content had zero Gitleaks findings; custom-token inspection found only deterministic fixtures. The repository is public. `v0.4.0` and `v1` resolve to that merge; unauthenticated downloads returned 200 and the exact tested SHA-256 `fca1e4887b98e4b18957e48932203e3f0510e249b37baeaddd9292bcc4f8945b` |

The original Google-login blocker is resolved. These checks establish deployed
services, invocation boundaries and measured proxy behavior. They do not by
themselves establish a completed authenticated web/CLI/MCP workflow.

## Acceptance still recorded separately

Complete the shared-account checks through the public web app, CLI and MCP;
exercise revocation, saved requests, quota/credit handling and new-content
rewards. Observe credential
refresh over its real lifetime. Retain the existing model and capacity gates;
the pre-upgrade search/Answer/Research baseline records relevance limitations,
so a healthy endpoint is not evidence of strong answer quality.

Stripe purchases remain disabled until the API key and webhook signing secret
are supplied and the test-mode checks pass. Article authoring, the distributed
contributor network and rewards for article views are unfinished upstream work.

The first full test run flagged the new operator-controlled auth/MCP HTTP client
in the repository's outbound-client inventory. It now has an explicit documented
exception, matching the existing operator-configured backend clients; contribution
fetches retain their separate public-only DNS-pinned dialer.
