# Connect the community web app and CLI to Andrei's services

The integration merged in [Cosift PR #58](https://github.com/pilot-protocol/cosift/pull/58)
at `c8ea845913829d5eece571c6facbb4462ab313c9` on 2026-09-17. The authorized
v0.2.7 release completed successfully in [workflow 35255963898](https://github.com/pilot-protocol/cosift/actions/runs/35255963898).
All five signed platform binaries and the public verification key are published.
The existing-host engine/gateway cutover completed at 18:23:08 UTC: Caddy
reloaded successfully, both public/origin auth-config endpoints report shared
mode, the public entry opens login, and operational routes are blocked.
Production email-code login, authenticated gateway/Firestore/MCP account checks,
and public Search/Answer/Research and MCP checks passed. Remaining acceptance
scope is recorded separately below.
The companion services and public installer are released as recorded below.

The web UI and CLI add saved requests, contribution moderation, quotas and a
credit ledger to Andrei's shared identity, MCP and onboarding services. They use
the same account UID; the existing engine continues to serve source-page search.

## Verified source contracts

| Component | Merged change | Infrastructure / contract |
|---|---|---|
| [cosift-auth PR #1](https://github.com/pilot-protocol/cosift-auth/pull/1) | `e6ce91911df8670e6913dd18ca8084236af428a5` | Go service, Firestore accounts/tokens, versioned Secret Manager peppers; email-code login and trusted-proxy configuration |
| [cosift-mcp PR #1](https://github.com/pilot-protocol/cosift-mcp/pull/1) | `fa60d320acc5adaa3a7112a38afaa7ac8d168932` | Python FastMCP at `/v1/mcp`; topics/article demand in Firestore; caller-token search forwarding to the community gateway |
| [cosift-install PR #1](https://github.com/pilot-protocol/cosift-install/pull/1) | `d18edefe257439987fa5f49c0a384cc039a65e90` | Public v0.4.0 and v1 installer; MCP/skills, email login, optional installed-CLI session handoff |

Production auth is `https://cosift-auth-udik5erlkq-uw.a.run.app`; production MCP
is `https://cosift-mcp-udik5erlkq-uw.a.run.app/v1/mcp`. Their `/health` endpoints
returned JSON 200 without Google credentials after deployment. Cloud Run
revisions `cosift-auth-00001-k6b` and `cosift-mcp-00002-lf7` each serve 100% of
traffic. Both disable the invoker IAM check; neither has an `allUsers` binding.
Staging retains its invoker check. No organization-policy override or custom
auth/MCP DNS is required for these direct origins.

Project: `telepat-cosift-5214`, number `301038218064`, region `us-west1`.
Firestore databases: `staging` and `(default)` for production. Account UID is the
16-character ID in `accounts/{uid}`. Community never derives it or accesses the
OTP pepper. Its local `shared_identities` table maps that UID to the existing
SQLite user ID, preserving saved requests, submissions, balances and payments.
A verified email can link an existing standalone account; all its old sessions
and password hash are invalidated. Subsequent requests resolve the same UID.
The database is permanently bound to the chosen project/database namespace;
switching to a different environment or disabling shared auth is refused.

```mermaid
flowchart LR
  Web[Community web app] --> Gateway[Community gateway on existing host]
  CLI[Community CLI with installed ck_ token] --> Gateway
  Agent[Installed agent] --> MCP[cosift-mcp on Cloud Run]
  MCP -->|caller token and search parameters| Gateway
  Gateway -->|email start, verify, revoke| Auth[cosift-auth on Cloud Run]
  Gateway -->|verify account and token| DB[Shared Firestore]
  Gateway -->|versioned token pepper| SM[Secret Manager]
  Gateway -->|topics, lookup, article requests| MCP
  Gateway -->|search, answer, research, validated contributions| Engine[Existing Cosift engine]
  Gateway --> Local[SQLite saves, credits, quotas, contribution queue]
```

The gateway does not send user tokens to the engine. Search stays on the existing
engine, with authenticated `k` bounded to 1–20 and `retriever` restricted to
`bm25`, `dense`, or `hybrid`. MCP's current `k=20&retriever=bm25` contract is
preserved. No new ranking change is included.

## What the connected app does

- Shared mode uses email-code login through `cosift-auth`. The browser receives
  an HttpOnly, SameSite cookie, not a token in JavaScript or local storage.
  Logout revokes the browser's token upstream. `/auth/start` deliberately returns
  the same envelope when mail is refused; the UI never promises mail delivery.
- Every authenticated request checks Firestore token revocation and account ban
  state. Only versioned pepper bytes are cached. Unknown/revoked credentials
  fail with 401, suspended accounts with 403, and infrastructure failures with
  503. An invalid credential never becomes a guest request.
  Cloud Run IAM rejections are distinguished from Cosift's JSON auth errors;
  a gateway permission failure does not label the user banned or revoked.
- Onboarding adds explicitly selected interests to the same followed topics
  used by agents. It does not erase existing agent topics. Editing local interest
  suggestions does not implicitly unfollow topics; use the topic controls.
- The Topics view lists up to 100 topics/requests, adds/removes followed topics,
  checks article coverage, and explicitly records article requests through MCP.
  MCP keeps requested-topic history after unfollowing. Duplicate article requests
  are idempotent upstream.
- Search, Answer, Research, saved requests, URL/CSV contributions, local text /
  metadata / embedding contributions, moderation, credits and disabled-by-default
  Stripe purchases retain the existing community implementation.
- Web, CLI and MCP searches share each account's local allowance and
  credit ledger. MCP retains its own upstream daily call cap (currently 1,000),
  including topic tools. Credits do not bypass that cap or buy an article.

Andrei's current MCP uses `NullArticleStore` with articles disabled. This work
connects article lookup/demand; it does **not** implement article generation,
Gemini research, a distributed contributor network, or rewards for article views.
Lookups can return no coverage while regular source-page search remains usable.
No local agent history is collected or uploaded by this integration.

## Runtime configuration

Keep standalone installations on `COSIFT_AUTH_MODE=local` (the default), with
email/password login and no Google credentials. The shared production gateway
uses the following service origins and account namespace in its private env file:

```dotenv
COSIFT_AUTH_MODE=shared
COSIFT_SHARED_PROJECT=telepat-cosift-5214
COSIFT_SHARED_DATABASE=(default)
COSIFT_AUTH_URL=https://cosift-auth-udik5erlkq-uw.a.run.app
COSIFT_MCP_URL=https://cosift-mcp-udik5erlkq-uw.a.run.app/v1/mcp
COSIFT_AUTH_AUDIENCE=
COSIFT_MCP_AUDIENCE=
GOOGLE_APPLICATION_CREDENTIALS=/etc/cosift/google-credentials.json
```

Resolve Cloud Run URLs with `gcloud run services describe` for a different
deployment. Public origins leave audiences empty. Private staging needs the
service's accepted canonical audience, without the MCP path, and refreshing
Google credentials.
Never mix staging services with `(default)` Firestore, or share a local SQLite
account database across the two environments. Start staging with a fresh data
directory. Shared mode requires all four project/database/auth/MCP settings.

The existing engine and local community service can stay on the current GH200
host. The Google clients use ADC; a Google service-account credential or a
supported impersonated ADC configuration must be provisioned for the Unix user
running the community unit. No runtime `gcloud` subprocess or hour-long pasted
Google JWT is used. Private Cloud Run calls put refreshing Google identity tokens
in `X-Serverless-Authorization`; the user's `ck_` stays in `Authorization`.
The browser never receives the Google credential. Existing systemd sandboxing
must permit reading the private ADC file and outbound HTTPS/gRPC.

The gateway runtime identity requires:

- Firestore account/token reads, collection-group token queries and token
  `last_used_at` updates in the selected database. The existing collection-group
  index on `tokens.tid` must be present. The application does not create accounts
  or issue tokens directly in Firestore.
  The minimal permission set is retained in
  [`deploy/community-account-role.yaml`](../deploy/community-account-role.yaml);
  it grants reads/query and update, without document creation/deletion. Apply
  the intended project/database scope when binding it to the runtime identity.
- Secret accessor on **`cosift-token-pepper` only**, including the active version
  IDs carried by tokens. Never grant this application the OTP or mail secrets.
- Cloud Run invoker on the chosen auth/MCP services when private. For an
  impersonation setup, the caller also needs the relevant identity-token minting
  permission on its designated service account.

Public production `run.app` origins now allow the installer and gateway to reach
the same auth/MCP services directly. Application-level authentication is still
required for MCP tools and token revocation. The public-ingress choice was
verified by reading back the invoker-check annotation and IAM bindings; private
staging remains available for isolated tests.

## Email-code proxy attribution

Auth currently has a 10-codes/hour/IP cap. Proxying without correct attribution
collapses every web user into the community host's one bucket. The gateway now
sends **its resolved client IP**, not the caller's raw forwarding headers, to auth
in `X-Forwarded-For`. Caddy/community trusted-proxy configuration remains required.

Use auth's existing `AUTH_XFF_MODE=cidr` resolver with explicit trusted platform
peer CIDRs and the community gateway's **actual egress IP**. The last observed
host IP was `192.222.56.72`; verify egress rather than assuming it. Cloud Run uses
link-local container peers; inspect the real staging forwarding chain to identify
the appropriate trust set. Do not blindly trust arbitrary private networks, all
addresses, or caller-supplied IP headers. A direct run.app client must still resolve
to its actual address, even if it forges a forwarding chain.

The auth companion has merged and been deployed. Production uses
`AUTH_XFF_MODE=cidr`, `AUTH_TRUSTED_PROXIES=192.222.56.72/32,169.254.169.126/32`
and `AUTH_DEBUG_ROUTES=false`. These addresses reflect the measured gateway
egress and Cloud Run peer for this deployment, not a portable trust-all default.
The retained `integrations/cosift-auth/proxy-configuration.patch` reproduces the
change against its historical base; do not reapply it to current auth main.

Private-staging checks verified that two forwarded client addresses retained
separate buckets, and a direct request with forged forwarding headers still
resolved to its actual caller. Production debug routes returned 404; staging
debug routes were disabled again after the checks. Production email delivery
and OTP verification subsequently passed. Refresh across a credential's real
lifetime remains a separate observation; health probes do not establish it.

## CLI usage

The connected installer can provision an already installed, compatible Cosift
CLI with the same token used by the agent. It writes a 0600, origin-bound session
to `${XDG_CONFIG_HOME:-$HOME/.config}/cosift/community-session.json`. Subsequent
`cosift request -query 'Rust async runtimes'` or `cosift contribute -credits`
commands discover that one session and its server automatically. Explicit
credentials and session selection take precedence; an explicit different server
is rejected before any credential is sent. Guest mode never reads the installed
session. Existing installer sessions are preserved rather than overwritten.

The installer repository is public. Both `v0.4.0` and the compatible `v1` ref
point at the reviewed merge above; unauthenticated downloads were checked
byte-for-byte against the tested script. Follow its
[signed v0.2.7 CLI installation instructions](https://github.com/pilot-protocol/cosift-install/blob/v0.4.0/docs/CLI-INSTALL.md)
before running `install.sh --cli`. The script does not download a CLI binary.
The published release key asset is `cosift-minisign.pub`, key ID `6184E2C01CA477A6`.

Reuse the `ck_` credential already issued by Andrei's installer by providing it
as `COSIFT_TOKEN` in the calling process. Do not put it in shell command arguments
or commit it to config. The community CLI does not automatically scan harness
files or export credentials from other applications.

```sh
# COSIFT_TOKEN is already supplied securely in the process environment.
cosift contribute -server https://cosift.pilotprotocol.network -request -query 'Rust async runtimes'
cosift contribute -server https://cosift.pilotprotocol.network -credits
cosift contribute -server https://cosift.pilotprotocol.network -index-locally https://example.org/article
# Or explicitly persist the verified credential in an origin-bound 0600 file:
cosift login -server https://cosift.pilotprotocol.network -session-file "$HOME/.cosift-community-session"
```

Use `cosift request` for the dedicated request subcommand, or `contribute -request`
as above. Local indexing still needs configured embeddings and is revalidated
by the server before earning credits. Direct token commands never mint or revoke
a temporary login session. `-guest` ignores the token. After saving a session,
unset `COSIFT_TOKEN` before selecting that session file; conflicting credentials
are rejected. Explicit CLI logout revokes the saved credential upstream, so a
saved installer token is also revoked for any agent still using that same token.
Standalone email/password and existing session-file workflows remain supported.
`cosift logout` discovers and revokes the installed session too, including an
expired one; a temporary upstream failure retains the file so logout can be
retried. Revoking that token also invalidates agents using the same token.

## MCP routing and rollout boundary

The merged MCP companion carries the already-verified token in request-scoped state,
passes it to the engine client for each search, requires HTTPS except loopback,
and refuses credential redirects. It never changes shared client default headers.
Point `COSIFT_ENGINE_BASE_URL` at the community origin, not the raw engine port.
The included tests cover concurrent accounts and credential leakage boundaries.

Production MCP now sets `COSIFT_ENGINE_BASE_URL=https://cosift.pilotprotocol.network`.
The gateway calls the raw engine directly on loopback; its MCP proxy allowlist
contains topic/lookup/request tools, not search, so this route does not loop.
The repository patch files remain historical reproduction artifacts. The live
MCP image includes the merged forwarding/runtime changes; its subsequent
deployment-script-only commit selected the project explicitly.
The signed v0.2.7 engine is healthy with HNSW ready; the existing community unit
uses the same binary path, and its old standalone override has been removed.
The community backup completed an upload and its timer is enabled. The
[community rollout gates](COMMUNITY-ROLLOUT.md) remain the acceptance and
rollback procedure for this deployment.
Back up the local database before linking existing users; do not downgrade a
linked database to standalone password auth or restore a stale credit ledger.

## Evidence and remaining work

See [the integration verification record](SHARED-ACCOUNTS-VALIDATION.md) for exact
commands, deployment evidence and remaining acceptance scope. The earlier Google
login blocker has been resolved; auth/MCP deployment and ingress checks used
authenticated cloud access. Their public health and invalid-token contracts pass.
Production email login passed, followed by stable account identity and credits
responses, matching gateway/direct-MCP topic lists, and a gateway BM25 search
returning 20 hits. Public authenticated Search/Answer/Research then passed, and
direct MCP search was verified to consume the same account's quota. A temporary
followed topic propagated both ways and was removed after the check. These used
a real account and production services. Revocation, saved requests and
new-content credit fulfillment remain distinct acceptance work. A real
local Rust artifact passed validation and reached Indexed; a repeated CSV URL
was deduplicated. Do not count synthetic moderation fixtures as
real production content. Stripe remains disabled pending its API key and
webhook signing secret plus the checks in [STRIPE.md](STRIPE.md).

Both the gateway and merged auth now use Go 1.26.8 and gRPC v1.83.2. The older
gRPC dependency warning belongs to the historical audit baseline, not the
released auth change. Article generation, Gemini authoring, a distributed
contributor network and article-view rewards remain unfinished upstream work.
