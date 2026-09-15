# Community app and contributions

`cosift community` runs a small web app alongside the search backend. It ships
inside the existing binary, with no JavaScript build step or new Go dependency.

People can:

- Use Search, Research, or Answer as a guest, or create an account with email and password. All three call the corresponding Cosift endpoint and preserve its retrieval defaults.
- Choose interests during onboarding and use them as search starting points.
- Save, rerun, and remove requests in their own account. Each saved request retains its Search, Research, or Answer mode; older saved searches migrate automatically.
- Submit public webpage URLs in a multiline field or a CSV upload.
- See their most recent 200 contributions and delivery status.
- Submit the same URLs or CSV files using `cosift contribute`.

Guests share **one successful Search, Research, Answer, or submission per 30 minutes per IP**.
The allowance is persistent and atomic across concurrent requests. Invalid input
and failed backend searches do not consume it. Reading pages or checking the
allowance is free. A submission may contain up to 100 URLs, just like a member
submission. HTTP 429 includes `Retry-After`, `retry_at` and
`retry_after_seconds`. Logging in uses the member limits instead: 30 Search/Research/Answer requests per
minute, 500 new contributed URLs per rolling 24 hours, and 200 saved searches.
People on a shared public IP share the guest allowance.

## Start the services

Build the current code:

```sh
go build -o cosift ./cmd/cosift
```

Use an existing Pebble backend with its in-process crawler enabled. Merge these
fields into its configuration, retaining its corpus paths and embedding setup:

```json
{
  "crawler": {
    "public_only": true,
    "filter_adult": true,
    "respect_robots": true,
    "proxies": [],
    "remote_fetcher_url": "",
    "remote_fetcher_urls": []
  },
  "cluster": {
    "peer_auth_token": "REPLACE_WITH_A_RANDOM_OPERATOR_TOKEN"
  }
}
```

The existing in-process crawler requires an embedding provider and a nonempty
seeds file. Configure `chat.model` and its provider for Answer, Research, and the
semantic contribution safety check. With no chat model, content checks remain
pending and submissions do not reach the crawler. Start it using your normal configuration, for example:

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

The app immediately rejects known adult domains, private/non-web URLs, and executable download links. Valid URL batches are stored for prevalidation. A background worker fetches each public webpage using restricted network egress and checks the destination, title, body text, image alt text, and metadata. It reuses Cosift’s adult-content classifier, then calls the authenticated `POST /admin/community-moderate` endpoint for a contextual safety decision. Only an explicit `allow/safe` result can be delivered to `POST /admin/community-enqueue`, which requires an active crawler with both `crawler.public_only=true` and `crawler.filter_adult=true`. An older backend or an unguarded
crawler cannot accept community submissions through this endpoint.

Public-only crawling resolves DNS, rejects private and special-purpose
addresses, and connects to the checked IP on port 80 or 443. The same transport
covers redirects, robots, and sitemap discovery. It uses direct HTTP egress;
the in-process crawler rejects proxy/remote-fetcher configurations in this mode.
For a cluster, enable it on every receiving shard. Forwarding from a guarded
shard also uses the guarded endpoint.

Content checks reject explicit adult material, malware/phishing, graphic violent abuse, extremist promotion, and serious illegal harm. The classifier policy distinguishes harmful promotion from neutral news, medical education, academic work, and defensive security research. Raw webpage text is treated as untrusted data, and malformed or contradictory classifier responses cannot authorize delivery.

`pending` is shown as **Checking**; `rejected` and `unverified` remain out of the crawl queue and include a reason in contribution history. Unavailable services retry; unsupported media, login walls, insufficient text, excessive text, and inconclusive content decisions remain unverified. The page limit is 2 MB, with at most 32,000 bytes of readable text and 4,000 bytes of metadata for contextual classification.

These are automated URL/text safety checks, not a guarantee or an antivirus scan. Images and video are not visually classified; image-only pages cannot pass based on empty text. A site can also change after validation. The crawler independently checks adult content again before indexing.

Delivery uses the submitted frontier lane. Failed delivery remains `pending`
with exponential retry delay, capped at roughly 43 minutes. Successful delivery
becomes `queued`. A restart resumes pending work. A crash after enqueue can
cause a duplicate delivery; frontier insertion is idempotent.

**Queued means delivered to the crawl queue, not indexed.** Existing robots,
domain allow/exclude rules, fetch failures, and crawler policy still apply.
The portal does not silently expand an operator's domain allowlist. Keep those
rules aligned with the public sources you intend to accept.

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
| `GET /api/submissions` | Member | Own recent contributions |
| `POST /api/submissions` | Guest or member | `{urls:[...]}` or multipart CSV; returns HTTP 202 |

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
