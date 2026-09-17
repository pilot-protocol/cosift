# Shared-account integration verification — 2026-09-17

All changes remain proposed in Cosift PR #58. No production code, service,
organization policy, DNS, updater, Stripe charge, or Andrei repository was changed.
Companion patches were exercised in local audit checkouts only.

## Checks completed

| Check | Result / scope |
|---|---|
| `GOWORK=off go test -race -timeout 10m ./...` | Full Cosift regression suite, including account linking, quota isolation, saved data, moderation and CLI regressions |
| `GOWORK=off go vet ./...` | Static checks |
| `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./cmd/cosift` | Production-architecture compilation; no release artifact published |
| `node --test internal/community/webtests/*.test.cjs` | 11 passing tests, including OTP state transitions and stale shared-topic responses |
| `GOWORK=off COSIFT_SMOKE_PORT=17983 bash scripts/smoke-test.sh` | Passed: real public crawl, ingestion, health, search, contents, admin authorization; disposable local index |
| Official auth token vectors | Canonical parsing/trailing bits and HMAC over full token agree with `cosift-auth` fixtures |
| Firestore SDK protocol fixture | Real Google SDK over local gRPC: collection-group lookup, typed verified/revoked timestamps, bans, last-used writes, ambiguous/missing token and IAM-denied errors |
| Auth lifecycle / HTTP contracts | Auth start/verify/revoke, HttpOnly cookies, Google-vs-user credential headers, forwarded client IP, bounded JSON, redirects and upstream error handling |
| Patched MCP suite | 248 passed, 2 skipped, 5 integration tests deselected; no cloud calls or model downloads |
| MCP → community → engine | Real MCP ASGI app and engine client against local Go gateway: concurrent accounts receive separate free allowances; repeat over-quota call does not reach the engine; `k=20`, BM25 preserved; account credential stops at the gateway |
| Community client → MCP tools | Real FastMCP protocol: follow/list/unfollow, request/idempotent repeat, missing article coverage; local fake identity/topic store and topic-resolution fixture |
| Auth proxy configuration companion | `bash -n infra/deploy.sh`; existing client-IP resolver and configuration tests pass |
| Companion patch applicability | Each patch matches its pinned base checkout (`git apply --reverse --check` against the patched checkout) |

## Security follow-up

The first integration commit's Snyk check failed; the Snyk report was gated by
login. An independent official `govulncheck` scan found affected gRPC v1.82.1
call paths ([GO-2026-6348](https://pkg.go.dev/vuln/GO-2026-6348)) and Go 1.26.0
standard-library paths with later security fixes. The PR now requires Go 1.26.8
and gRPC v1.83.1; CI also runs pinned `govulncheck` v1.8.0. The independent scan
is not a substitute for a successful Snyk check on the final commit. The updated
scan reports **0 vulnerabilities called by the application**; it also reports
non-called package/module advisories, which must not be described as a completely
empty advisory inventory.

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

## Not established by these tests

These results do not certify a live shared-account deployment. The available
Google CLI credentials required reauthentication, so this sweep did not exercise
live Firestore/Secret Manager IAM, an actual Cloud Run service identity, real mail
delivery, public DNS/ingress, or credential refresh over its real lifetime.

Before rollout: provision the gateway identity; confirm the exact project,
database, index, accepted Cloud Run URLs/audiences and IAM grants; verify safe
proxy attribution with two real client addresses and a spoofed direct request;
land the MCP token-forwarding and auth deployment-configuration companions;
exercise real email login, revocation and ban handling in staging. Retain the
existing load, model and Stripe acceptance gates. Article generation and the
distributed contributor network remain outside the implemented upstream scope.

The first full test run flagged the new operator-controlled auth/MCP HTTP client
in the repository's outbound-client inventory. It now has an explicit documented
exception, matching the existing operator-configured backend clients; contribution
fetches retain their separate public-only DNS-pinned dialer.
