# Community app and contributions

`cosift community` runs a small web app alongside the search backend. It ships
inside the existing binary, with no JavaScript build step or new Go dependency.

People can:

- Use Search, Research, or Answer as a guest, or create an account with email and password. All three call the corresponding Cosift endpoint and preserve its retrieval defaults.
- Choose interests during onboarding and use them as search starting points.
- Save, rerun, and remove requests in their own account. Each saved request retains its Search, Research, or Answer mode; older saved searches migrate automatically.
- Submit public webpage URLs in a multiline field or a CSV upload.
- See their most recent 200 contributions, indexing status and credit balance.
- Submit the same URLs or CSV files using `cosift contribute`.

Guests share **one successful Search, Research, Answer, or submission per minute per IP**.
The allowance is persistent and atomic across concurrent requests. Invalid input
and failed backend searches do not consume it. Reading pages or checking the
allowance is free. A submission may contain up to 100 URLs, just like a member
submission. HTTP 429 includes `Retry-After`, `retry_at` and
`retry_after_seconds`. Guest Answer is capped at one per 5 minutes and Research at one per 30 minutes.
Members receive 60 shared free requests/minute, 500 new contributed URLs per rolling 24 hours, and 200 saved searches.
People on a shared public IP share the guest allowance.

## Start the services

Build the current code:

```sh
go build -o cosift ./cmd/cosift
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

Checks reject adult material, malware/phishing, graphic violent abuse, extremist promotion and serious illegal harm while allowing neutral education, medicine, news and defensive security research. Malformed decisions cannot authorize indexing. These are automated URL/text checks, not a guarantee or antivirus scan. Images/video are not visually classified, and pages can change between validation and indexing. Unreadable or inconclusive pages remain unverified; unavailable services retry.

## Local indexing, credits and future payments

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
not earn credits. Guests do not earn credits. After the shared free 60 requests/minute,
each additional Search, Answer or Research costs **1 credit**, with a ceiling of
60 Search/minute, 20 Answer/minute and 3 Research/10 minutes per account. Credits cannot bypass these hard caps. Mode caps are persisted across restarts and shared by all sessions and native endpoint aliases. Backend failures refund the debit. Credits are
spent rather than granting permanent tiers. `GET /api/credits` returns the
balance and policy; the web app displays the balance.

The ledger and an idempotent payment-event table leave room for paid credit
purchases. Payment checkout, payment-provider credentials and webhook handling
are **not enabled**. No money is charged in this release. A future integration
must verify signed provider events and credit the ledger transactionally.

The deployed Caddy configuration routes public `/search`, `/answer` and `/research`
through the same portal policy as `/api/*`. These public aliases support GET with
`q`; POST and advanced native engine parameters are not supported on the public
portal. The internal loopback engine remains available to trusted operators.
`GET /api/limits` publishes current limits. Operators can configure
`-guest-interval`, `-member-free-rpm`, `-search-rpm`, `-answer-rpm`, and
`-research-per-10m` on the community command. A guest interval change preserves
the original request time instead of resetting all allowances. In-flight requests
reserve a mode slot; backend failures release it and refund charged credits.
The shared free member allowance counts attempts and is process-local; persisted
mode caps still bound actual work after a restart.

## CLI and CSV

Guest contribution (no account or token needed):

```sh
./cosift contribute -server https://community.example.com -guest \
  https://go.dev/doc/ https://www.rust-lang.org/learn

./cosift contribute -server https://community.example.com -guest -csv sources.csv
```

For member submissions, create an account in the web app and set `COSIFT_EMAIL`
and `COSIFT_PASSWORD` in your shell environment. Keep the password out of command
arguments and shell history. Omit `-guest`:

```sh
./cosift contribute -server https://community.example.com -csv sources.csv
```

Without either credential, the CLI defaults to guest access. `-guest` explicitly
ignores configured credentials. `-email` overrides `COSIFT_EMAIL`; `-csv -` reads
stdin. Flags precede positional URLs. The CLI logs out its temporary session
after an authenticated submission.

CSV accepts a single headerless URL column, or a column called `url`, `urls`,
`webpage`, or `website`. Other columns are ignored when a recognized header is
present. Quoted fields, commas in titles, and UTF-8 BOMs are supported:

```csv
title,url
Go documentation,https://go.dev/doc/
"Rust, getting started",https://www.rust-lang.org/learn
```

Limits: 100 rows/URLs per request, 1 MB request body, 2,048 characters per URL.
Duplicate URLs are normalized and collapsed; members also receive a duplicate
count for URLs they previously contributed. An invalid row rejects the whole
batch without saving partial input or using a guest allowance.

## API

All mutation requests carry `X-Cosift-Client: community`. JSON mutations use
`Content-Type: application/json`; CSV uses multipart field `file`. Browser
requests must originate from `-public-url`. No cross-origin CORS access is
enabled. CLI clients may omit Origin. Login returns an HttpOnly session cookie.

| Method and path | Access | Body / behavior |
| --- | --- | --- |
| `POST /api/register` | Public | `{email,password,name}`; creates account and session |
| `POST /api/login` | Public | `{email,password}`; creates session |
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
| `GET /api/credits` | Member | Credit balance, free allowance and extra-request cost |
| `GET /api/submissions` | Member | Own recent contributions |
| `POST /api/submissions` | Guest or member | `{urls:[...]}`, authenticated `{artifacts:[...]}`, or multipart CSV; returns HTTP 202 |

## Account data and operational scope

Account and submission data lives in `community-data/community.db`, separate
from the corpus. The file is created with mode 0600, the directory with 0700,
and SQLite uses WAL. Back up the entire directory while the community service
is stopped, or use a SQLite-consistent backup tool. Run one dispatcher process
per community database.

Passwords use PBKDF2-HMAC-SHA256 with independent random salts and 600,000
iterations. Only hashes of session tokens are stored; sessions expire after 30
days. Account queries always scope saved searches and submissions to the session
owner. Guest allowance records use a salted IP hash, not a raw IP; expired
records are removed as allowances are reserved. Guest submissions have no
account history and are not retroactively attached after signup.

Interests provide clickable search suggestions; they do not alter retrieval
ranking. Queries still go to the configured backend, whose logging policy
applies. The app has no automatic email sending, email verification, or
self-service password reset in this first version. It does not implement the
older distributed-compute contribution-network proposal.

Search returns ordinary result cards; Answer and Research render the returned answer, source citations, and research plan. Their synchronous backend timeout is three minutes; configure reverse proxies to allow at least four minutes. Missing LLM configuration is reported clearly and does not consume the guest allowance.

Tests cover account isolation, mode-aware saved-request migration, endpoint parity, session expiry/logout, CSV atomicity, durable prevalidation/delivery, strict moderation decisions, guest cooldown/restart/concurrency, trusted proxies, public-network egress, and CLI member/guest submissions. No production deployment is performed
by building or running the app locally.

## Production service and release

`deploy/systemd/cosift-community.service` runs the portal on loopback port 7780.
Create its private data directory before starting it and supply
`COSIFT_COMMUNITY_ADMIN_TOKEN` through root-owned `/etc/cosift/community.env`.
`deploy/Caddyfile.community` routes the root, static assets and `/api/*` to the
portal while retaining existing engine endpoints. It trusts only loopback and
Cloudflare networks, then overwrites the forwarded client IP.

The community backup timer snapshots SQLite consistently into the existing GCS
bucket. Restore the account database as a unit, including credits and pending
artifacts. The release updater restarts the portal after backend health passes;
check `/stats.hnsw_load.state` separately for dense retrieval readiness. Initial
rollout also requires preserving the old binary, backend config and Caddy config.

Signed release assets include Linux ARM64/AMD64, macOS ARM64/AMD64 and Windows
AMD64. Install the matching binary and use the same public server URL for both
`contribute` and `request`. Payment purchase flows remain disabled.


## Public entry and operations visibility

Anonymous visitors land on the signup/sign-in screen. `/login` opens sign-in,
`/signup` opens account creation, and an existing session opens the workspace.
Guest browsing remains an explicit choice with the configured guest allowance.
Signing out returns to authentication.

The production proxy denies public access to `/stats`, `/metrics`, `/queue`,
`/domains`, `/verify`, and `/sla` (including subpaths). Operators can still use
these endpoints over SSH on the loopback engine listener. The old `/chat` UI
redirects to `/login`. `/healthz` retains its minimal health response.
