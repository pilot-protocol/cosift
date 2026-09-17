# Community app and contributions

The production web app, CLI, and agent MCP use the same email-verified Cosift account. See [the connected architecture](SHARED-ACCOUNTS.md), [the rollout record](COMMUNITY-ROLLOUT.md), and [agent setup](AGENT-SETUP.md). Standalone email/password authentication remains available for local installations; production uses the shared email-code service.

`cosift community` runs a small web app alongside the search backend. It ships
inside the existing binary, with no JavaScript build step. Stripe webhook verification uses the official Go SDK.

People can:

- Use Search, Research, or Answer as a guest, or sign in using an email verification code. All three call the corresponding Cosift endpoint and preserve its retrieval defaults.
- Choose interests during onboarding and use them as search starting points.
- Save, rerun, and remove requests in their own account. Each saved request retains its Search, Research, or Answer mode; older saved searches migrate automatically.
- Submit public webpage URLs in a multiline field or a CSV upload after signing in.
- See their most recent 200 contributions, indexing status and credit balance.
- Submit the same URLs or CSV files using `cosift contribute`, optionally extracting text and computing embeddings locally.
- Follow topics across the web and MCP, check article coverage, and record requests for missing articles. Automatic article authoring is still in development.
- View monthly credits and manage an optional paid subscription or subscriber top-up when live payments are configured.

Guests share **one successful Search, Research, or Answer request per minute per IP**.
Guest Answer is additionally capped at one per 5 minutes and Research at one per
30 minutes. Reading pages or checking the allowance is free. Invalid input and
failed backend requests do not consume the allowance. HTTP 429 includes
`Retry-After`, `retry_at`, and `retry_after_seconds`. People on a shared public IP
share this persistent, atomic guest allowance. **Contributions require login.**

Members receive **60 shared free requests per minute plus 1,000 free credits each
UTC calendar month**, 1,000 new contributed URLs per rolling 24 hours, and 200
saved searches. The monthly grant is applied once per account for the current
month on authenticated use; it does not accumulate grants for inactive past
months. All unused credits carry over. Mode caps and credit costs are described
below.

## Start the services

Build the current code:

```sh
GOWORK=off go build -o cosift ./cmd/cosift
```

Use an existing Pebble backend with its in-process crawler enabled, an embedding
provider, and a nonempty seeds file. Configure `chat.model` and its provider for
Answer, Research and semantic content checks. Without a chat model, submissions
remain pending.

The backend creates a separate contribution crawler sharing the corpus and
embedding budget. It always uses direct public-only HTTP, robots checks, adult
filtering, and no link/sitemap discovery. The bulk crawler may retain its remote
fetcher and its existing policies. No global `crawler.public_only` change is
required for the community service.

```sh
./cosift -config /etc/cosift/cosift.json pebble-serve \
  -dir /srv/cosift/pebble -crawl-seeds-file /etc/cosift/seeds.txt
```

Set `COSIFT_COMMUNITY_ADMIN_TOKEN` in the community service environment to the
backend's `cluster.peer_auth_token`. The community service uses it only for
contribution checks and delivery; it never sends it to the browser or with searches.

```sh
./cosift community \
  -addr 127.0.0.1:7780 \
  -public-url http://127.0.0.1:7780 \
  -backend http://127.0.0.1:7777 \
  -data-dir ./community-data
```

Open `http://127.0.0.1:7780`. For a public deployment, put the app behind an HTTPS
reverse proxy and set `-public-url https://community.example.com`. This exact
origin is used for browser request validation and secure cookies. If the proxy
connects from loopback and appends or overwrites `X-Forwarded-For`, add
`-trusted-proxies 127.0.0.1/32,::1/128`. Only configure networks actually used by
your trusted proxies. Without this flag, quotas use the direct connection IP;
client-supplied forwarded headers are ignored. Keep the app bound to loopback
when a same-host reverse proxy is used.

The standalone community listener exposes only the app and `/api/*`. It does
not proxy arbitrary backend paths or expose backend administration.

## Contribution delivery

The app immediately rejects known adult domains, private/non-web URLs, and executable download links. A durable queue in the community database holds submissions while a worker checks public page content and calls the authenticated `/admin/community-moderate` endpoint. Obvious parked/placeholder domains, error/bot/login pages, and extreme repetitive filler are stopped before model classification. The classifier additionally rejects spam, link farms, SEO doorway pages, incoherent scraps and content without useful information. It must preserve useful code, non-English pages, medical education and academic research; authorship alone is not a rejection signal. These checks reduce junk but do not guarantee perfect classification. Only an explicit safe result permits delivery to `/admin/community-enqueue`.

The receiving backend performs guarded direct indexing through a separate crawler. Bulk crawling retains its remote fetcher. Contributions never trigger link or sitemap discovery; existing domain inclusion/exclusion policy still applies. At most two contributions index concurrently, sharing the bulk crawler's embedding throttle. The delivery call is bounded to two minutes, with durable retries on transient failures.

`pending` appears as **Checking**; `rejected` and `unverified` stay outside indexing. `indexed` means the backend acknowledged an indexable document. Older queue acknowledgements remain `queued`. An acknowledgement does not guarantee successful embedding of every passage; standard crawler embedding errors still apply.

Checks reject adult material, malware/phishing, graphic violent abuse, extremist promotion and serious illegal harm while allowing neutral education, medicine, news and defensive security research. Malformed decisions cannot authorize indexing. Production combines URL/network/content checks with semantic review using `qwen3.5:9b-fp8`. These automated text and metadata checks do not guarantee image/video moderation or act as an antivirus scan. Unreadable, unsupported, or inconclusive pages remain unverified; unavailable services retry. The backend binds approval to the checked content and rejects a contribution when the fetched content no longer matches its approval. No indexing or credits occur before approval.

## Local indexing and credits

Authenticated CLI users can fetch, parse, chunk and embed webpages locally, save them in their local SQLite index, and submit text, metadata and vectors:

```sh
# Configure data_dir plus embeddings.url, model and dim for your local embedder.
# The model and dimensions must match the destination index.
./cosift -config local.json contribute -server https://cosift.pilotprotocol.network \
  -index-locally https://go.dev/doc/
./cosift contribute -server https://cosift.pilotprotocol.network -credits
./cosift request -server https://cosift.pilotprotocol.network -mode research -query "How does Raft work?"
```

`-index-locally` also accepts `-csv`. It requires login, limits an artifact to
32,000 text bytes and 64 chunks, and retains the total 1 MB request limit.
The backend fetches the source independently, compares title/text, checks the
model/dimensions, and verifies **every vector** against its own embedding model
before reuse. Failed verification cannot inject vectors. This first version
spends server compute on full verification; it does not claim compute savings.
If chunk boundaries differ, the backend computes the missing vectors normally.

A newly indexed member contribution earns **10 credits**. Rewards are globally
idempotent by content hash, so retrying or mirroring the same content cannot earn
multiple rewards. Existing corpus URLs and rejected/unverified submissions do
not earn credits. A submission acknowledgement is not a reward: the backend must
confirm approved new content was indexed.

After the shared free 60 requests/minute, successful extra requests spend credits:

| Mode | Credits per extra request | Hard cap per account |
| --- | --- | --- |
| Search | 1 | 120/minute |
| Answer | 2 | 20/minute |
| Research | 3 | 3/10 minutes |

The monthly 1,000 free credits, contribution rewards, and purchased credits use
one balance. They are spent, rather than granting permanent rate-limit tiers.
Credits cannot bypass hard caps. Mode limits persist across restarts and are
shared by sessions and public endpoint aliases. Failed backend requests release
reservations and refund debits. `GET /api/credits` returns the balance, current UTC
month's free/earned/purchased/spent activity, and policy. MCP search uses this same
gateway ledger; the MCP service also has a separate daily tool-call cap.

## Plans and payments

The **Free plan requires no subscription** and includes the monthly 1,000 credits
and 60 free requests/minute. An optional **$5/month subscription adds 50,000 credits
per paid month** and unlocks one-time **$5/50,000-credit top-ups**. Subscribers still
receive the free monthly credits. Unused credits carry over; cancellation does
not erase remaining earned or purchased credits. Refunds revoke the corresponding
purchased credits.

At this pack price, 1,000 credit-funded Search requests cost $0.10, Answer $0.20,
and Research $0.30. Rate caps still apply. Live purchases remain unavailable until
live Stripe keys and verified webhooks are configured. Browser redirects cannot
grant credits: signed paid invoices grant subscription credits, and verified paid
Checkout events grant top-ups. See [Stripe setup and validation](STRIPE.md),
including the restricted billing portal and isolated test-mode requirements.

The production Caddy configuration routes public `/search`, `/answer` and `/research`
through the same portal policy as `/api/*`. These public aliases support GET with
`q`; POST and advanced native engine parameters are not supported on the public
portal. Unlisted native routes return 404 to prevent quota bypasses. The internal
loopback engine remains available to trusted operators.
`GET /api/limits` publishes current limits. Operators can configure
`-guest-interval`, `-member-free-rpm`, `-search-rpm`, `-answer-rpm`, and
`-research-per-10m` on the community command. A guest interval change preserves
the original request time instead of resetting all allowances. In-flight requests
reserve a mode slot; backend failures release it and refund charged credits.
The shared free member allowance also persists across restarts. Failed backend
requests release both free and mode reservations. At the defaults, Search-only
usage can consume 60 free requests and then 60 credit-funded requests per minute.

## CLI and CSV

Install the signed CLI and run the [shared-account installer](AGENT-SETUP.md).
When a compatible CLI is already on PATH, the installer can save its session
without asking you to copy a token from agent configuration. The CLI discovers
`$XDG_CONFIG_HOME/cosift/community-session.json`, or
`~/.config/cosift/community-session.json` when `XDG_CONFIG_HOME` is unset.

```sh
cosift request -query "Go modules"
cosift contribute -csv sources.csv
cosift contribute https://go.dev/doc/ https://www.rust-lang.org/learn
cosift contribute -credits
```

Guest retrieval is explicit when a saved account is present:

```sh
cosift request -server https://cosift.pilotprotocol.network -guest -query "Go modules"
```

Contributions always require an account. An existing shared token may be supplied
through `COSIFT_TOKEN`; keep its value out of command arguments, shell history,
logs, and repositories. To persist such a token, use `cosift login -server URL
-session-file FILE` in a private directory. The CLI validates the token at that
origin before writing the session file. See [signed CLI installation and session
setup](https://github.com/pilot-protocol/cosift-install/blob/main/docs/CLI-INSTALL.md).

`-session-file FILE` overrides `COSIFT_SESSION_FILE`; explicit token, password,
server, and guest choices are never replaced by an implicit saved session. The
session is origin-bound, mode 0600, and contains no password. Login refuses to
overwrite an existing file. A saved origin cannot override an explicit `-server`.
Invalid files and expired/revoked sessions fail instead of silently changing to
guest access. `-guest` ignores saved credentials, and `-csv -` reads stdin. Flags
precede positional URLs.

`cosift logout` revokes the selected token and deletes its saved file. In shared
mode, revoking an installer-issued token also signs out agents using that same
token; separately issued browser tokens remain independent. A failed logout
keeps the file for retry unless the service confirms it is already invalid.
Never commit, share, or upload a session file.

Local standalone installations can instead use `COSIFT_EMAIL` and
`COSIFT_PASSWORD`, or an explicitly selected session file. Temporary password
logins are revoked after each command; use a saved session for repeated requests.
Those local password routes are disabled in shared production mode.

CSV accepts a single headerless URL column, or a column called `url`, `urls`,
`webpage`, or `website`. Other columns are ignored when a recognized header is
present. Quoted fields, commas in titles, and UTF-8 BOMs are supported:

```csv
title,url
Go documentation,https://go.dev/doc/
"Rust, getting started",https://www.rust-lang.org/learn
```

Limits: 100 rows/URLs per request, 1 MB request body, 2,048 characters per URL, and
1,000 new contributed URLs per account per rolling 24 hours.
Duplicate URLs are normalized and collapsed; members also receive a duplicate
count for URLs they previously contributed. An invalid row rejects the whole
batch without saving partial input. Duplicates do not consume the new-URL allowance.

## API

Browser and CLI mutation requests carry `X-Cosift-Client: community`. JSON mutations use
`Content-Type: application/json`; CSV uses multipart field `file`. Browser
requests must originate from `-public-url`. No cross-origin CORS access is
enabled. CLI clients may omit Origin. Login returns an HttpOnly session cookie. The exact Stripe webhook path is exempt from browser CSRF headers and instead requires a valid signature over the raw body.

| Method and path | Access | Body / behavior |
| --- | --- | --- |
| `GET /api/auth/config` | Public | Reports whether shared email-code authentication is enabled |
| `POST /api/auth/start` | Public | `{email}`; starts email verification |
| `POST /api/auth/verify` | Public | `{request_id,code}`; verifies email and establishes a session |
| `POST /api/register` | Local mode only | `{email,password,name}`; creates account and session |
| `POST /api/login` | Local mode only | `{email,password}`; creates session |
| `POST /api/logout` | Member | Revokes current session |
| `GET /api/me` | Member | Profile and interests |
| `PUT /api/interests` | Member | `{interests:[...]}`; completes onboarding, including an empty list |
| `GET /api/guest` | Public | Current IP's allowance and next available time |
| `GET /api/search?q=...` | Guest or member | Cosift `/search`, preserving backend defaults |
| `GET /api/research?q=...` | Guest or member | Cosift `/research`; plan, synthesized answer and cited sources |
| `GET /api/answer?q=...` | Guest or member | Cosift `/answer`; direct answer and cited sources |
| `GET /api/saved` | Member | Own saved searches |
| `POST /api/saved` | Member | `{query,mode}`; mode defaults to `search`; idempotent per account/query/mode |
| `DELETE /api/saved/{id}` | Member | Removes an owned saved search |
| `GET /api/credits` | Member | Balance, monthly activity, weighted request costs, subscription state, top-up eligibility, and payment mode |
| `POST /api/payments/checkout` | Member | `{kind:"subscription"\|"topup",idempotency_key}`; returns a hosted Stripe Checkout URL |
| `POST /api/payments/portal` | Member | `{}`; returns an existing subscriber's restricted billing portal URL |
| `POST /api/payments/webhook` | Stripe signature | Paid invoice/top-up fulfillment, subscription state, and refund reconciliation |
| `POST /api/shared` | Member, shared mode | Allowlisted topic, article lookup, or article request tool; search uses the gateway retrieval routes |
| `GET /api/submissions` | Member | Own recent contributions |
| `POST /api/submissions` | Member | `{urls:[...]}`, authenticated `{artifacts:[...]}`, or multipart CSV; returns HTTP 202 |

## Account data and operational scope

Account and submission data lives in `community-data/community.db`, separate
from the corpus. The file is created with mode 0600, the directory with 0700,
and SQLite uses WAL. Back up the entire directory while the community service
is stopped, or use a SQLite-consistent backup tool. Run one dispatcher process
per community database.

Shared production accounts and tokens are verified against the auth service's
Firestore and Secret Manager records. The gateway binds its local database to
the shared project/database namespace and maps the verified UID to one account,
preserving its saves and credits. Email identity comes from the shared verifier.
Local standalone passwords use PBKDF2-HMAC-SHA256 with independent random salts
and 600,000 iterations; local sessions store only token hashes and expire after
30 days. Shared token lifetimes and revocation are controlled by the auth service.

Account queries scope saved searches and submissions to their owner. Guest
allowance records use a salted IP hash rather than a raw IP. Interests provide
clickable search suggestions and shared topic follows; they do not alter retrieval
ranking. Queries go to the configured backend, whose logging policy applies.
Shared production verifies email with a code; standalone mode has no email
verification or self-service password reset. Automatic article authoring and the
distributed contributor network remain separate, unfinished work.

Search returns ordinary result cards; Answer and Research render the returned answer, source citations, and research plan. Their synchronous backend timeout is three minutes; configure reverse proxies to allow at least four minutes. Missing LLM configuration is reported clearly and does not consume the guest allowance.

Tests cover account isolation, mode-aware saved-request migration, endpoint parity, session expiry/logout, CSV atomicity, durable prevalidation/delivery, strict moderation decisions, guest cooldown/restart/concurrency, trusted proxies, public-network egress, and authenticated CLI submissions/guest retrieval. No production deployment is performed
by building or running the app locally.

## Production service and release

See [the operator rollout and rollback record](COMMUNITY-ROLLOUT.md) for the connected production rollout. A release is not ready merely because it builds: verify shared login, source retrieval, contribution receipts, quota persistence, and backups after an upgrade. Billing additionally needs the live Stripe setup below.

`deploy/systemd/cosift-community.service` runs the portal on loopback port 7780.
Create its private data directory before starting it and supply
`COSIFT_COMMUNITY_ADMIN_TOKEN` through root-owned `/etc/cosift/community.env`.
`deploy/Caddyfile.community` routes the root, static assets and `/api/*` to the
portal, and routes public `/search`, `/answer`, and `/research` through the same quotas. All unlisted routes, including `/query`, `/find_similar` and `/contents`, return 404. It trusts only loopback and
Cloudflare networks, then overwrites the forwarded client IP.

The community backup timer snapshots SQLite consistently into the existing GCS
bucket. Restore the account database as a unit, including credits and pending
artifacts. The release updater restarts the portal after backend health passes;
check `/stats.hnsw_load.state` separately for dense retrieval readiness. Initial
rollout also requires preserving the old binary, backend config and Caddy config.

Signed release assets include Linux ARM64/AMD64, macOS ARM64/AMD64 and Windows
AMD64. Install the matching binary and use the same public server URL for both
`contribute` and `request`. Subscriptions and subscriber top-ups are available only when live Stripe billing is configured and verified.


## Public entry and operations visibility

Anonymous visitors land on the signup/sign-in screen. `/login` opens sign-in,
`/signup` opens account creation, and an existing session opens the workspace.
Guest browsing remains an explicit choice with the configured guest allowance.
Signing out returns to authentication.

The production proxy denies public access to `/stats`, `/metrics`, `/queue`,
`/domains`, `/verify`, and `/sla` (including subpaths). Operators can still use
these endpoints over SSH on the loopback engine listener. The old `/chat` UI
redirects to `/login`. `/healthz` retains its minimal health response.
