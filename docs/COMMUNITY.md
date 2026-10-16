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
- Fetch pages that sites block for the crawler, from their own network, with `cosift contribute -worker` (see [Fetch work](#fetch-work)).
- Open **My contributions** to see the articles written with their agent, fetched-page counters, credits earned and submitted pages.
- Follow topics across the web and MCP, check article coverage, and record requests for missing articles.
- View monthly credits and manage an optional paid subscription or subscriber top-up when live payments are configured.

Guests share one persistent cooldown per public IP address (per /64 for IPv6). A successful **Search holds
it for 30 minutes, Answer for 60 minutes, or Research for 90 minutes**. The
cooldown applies across all three modes: after Answer, even Search must wait
60 minutes. Changing modes does not create another allowance. People sharing a
public IP also share this cooldown. Invalid input and failed backend requests
do not consume it, but a request the caller cancels after it reaches the backend
keeps its cooldown; reading the app or checking the allowance is free. HTTP 429
includes `Retry-After`, `retry_at`, and `retry_after_seconds`.
**Contributions require login.**

Members receive **1,000 free credits each UTC calendar month**,
1,000 new contributed URLs per rolling 24 hours, and 200
saved searches. The monthly grant is applied once per account for the current
month on authenticated use; it does not accumulate grants for inactive past
months. All unused credits carry over. Every successful authenticated Search,
Answer, or Research request spends credits, including the first request. There
is no free per-minute member bypass. Mode caps and credit costs are described below.

## Start the services

Build the current code:

```sh
GOWORK=off go build -o cosift ./cmd/cosift
```

Use an existing Pebble backend with its in-process crawler enabled, an embedding
provider, and a nonempty seeds file. Configure `chat.model` and its provider for
Answer, Research and semantic content checks. Without a chat model the moderation
route answers 503, and a submission that needs the model check ends as
`unverified` after 12 attempts.

The backend creates a separate contribution crawler sharing the corpus and
embedding budget. It always uses direct public-only HTTP, robots checks, adult
filtering, and no link/sitemap discovery. The bulk crawler may retain its remote
fetcher and its existing policies. No global `crawler.public_only` change is
required for the community service.

```sh
./cosift -config /etc/cosift/cosift.json pebble-serve \
  -dir /srv/cosift/pebble -crawl-seeds-file /etc/cosift/seeds.txt
```

The community service reaches the backend's loopback `/v1` listener (default
`http://127.0.0.1:7779`) with its own scoped key, not the peer token. Create a
key principal in the backend's service-auth file with the `community:intake`
scope (add `work:broker` when fetch work is on; these two scopes combine only with
each other, and require `"env": "prod"`),
then set `COSIFT_COMMUNITY_BROKER_KEY` to its `csk_` key in the community service
environment. Set `COSIFT_COMMUNITY_ENGINE_URL` only when the listener is elsewhere;
it must be a loopback `http` origin. The service uses the key only for contribution
checks and delivery (and, with fetch work on, the work routes); it never sends it to the browser or with searches. It refuses
to start while `COSIFT_COMMUNITY_ADMIN_TOKEN` is set, and the backend's
`cluster.peer_auth_token` must never be given to the community service.

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

The app immediately rejects known adult domains, private/non-web URLs, and executable download links. A durable queue in the community database holds submissions, and the dispatch loop takes them in rotation across accounts, oldest first within an account, so one account's backlog delays another's submission by one job at most. A worker checks public page content and calls the engine's `POST /v1/community/moderate`, authenticated with the broker key. Obvious parked/placeholder domains, error/bot/login pages, and extreme repetitive filler are stopped before model classification. The classifier additionally rejects spam, link farms, SEO doorway pages, incoherent scraps and content without useful information. It must preserve useful code, non-English pages, medical education and academic research; authorship alone is not a rejection signal. These checks reduce junk but do not guarantee perfect classification. Only an explicit safe result permits delivery to `POST /v1/community/enqueue`.

The engine caps the model calls of the moderation route per UTC day. Set
`work.community_moderations_per_day` in `cosift.json` (default and maximum 1000;
`0` halts the route):

```json
{ "work": { "community_moderations_per_day": 500 } }
```

Past the ceiling the route answers 503 `moderation_unavailable` with `ceiling: true`
and `Retry-After` set to the next 00:00 UTC. A page decided before the model (adult,
obvious junk) takes nothing from the allowance. On that answer the community app sends
no further content check until 00:00 UTC: submissions that need the model stay
`pending` with the reason "Waiting for the daily content-check allowance to renew."
and do not use up attempts. Each account also has a daily share of the content checks,
`COSIFT_COMMUNITY_MODERATIONS_PER_ACCOUNT` (default 100; an integer from 1 to 10000,
anything else stops startup): once an account has used it, that account's submissions wait
until 00:00 UTC without using attempts, while other accounts' submissions go on. Any other
model failure still counts as an attempt, and a submission ends as `unverified` after 12. `/metrics` counts the answers as
`cosift_community_moderations_total{result="ok|ceiling|error"}`. Each community
route also accepts at most 30 calls a minute per principal; lower them with
`COSIFT_V1_ENQUEUE_PER_MIN` and `COSIFT_V1_MODERATE_PER_MIN` on the engine (an
integer from 1 to 30; anything else stops the engine at startup).

The receiving backend performs guarded direct indexing through a separate crawler. Bulk crawling retains its remote fetcher. Contributions never trigger link or sitemap discovery; the crawler's domain inclusion/exclusion policy still applies to the submitted URL and to every redirect hop. An entry matches a host exactly or as a dot-boundary suffix (`example.com` covers `blog.example.com`, not `evilexample.com`); a port on an entry is ignored. At most two contributions index concurrently, sharing the bulk crawler's embedding throttle. The delivery call is bounded to two minutes, with durable retries on transient failures.

`pending` appears as **Checking**; `rejected` and `unverified` stay outside indexing. `indexed` means the backend acknowledged an indexable document. Older queue acknowledgements remain `queued`. A submission waits as `config_error`, retried hourly, while the broker key is missing or the engine refuses it. While the engine's listener is unreachable, submissions stay pending, are retried every five minutes and their pages are not fetched. A submission that cannot be checked or delivered after 12 attempts ends as `unverified`. An acknowledgement does not guarantee successful embedding of every passage; standard crawler embedding errors still apply.

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

`-index-locally` also accepts `-csv`. It fetches each page the way a browser does
(browser request headers, a public-only dialer, each redirect hop checked)
and extracts it with the same code as `cosift contribute -worker`. It requires login,
limits an artifact to 32,000 text bytes and 64 chunks, and retains the total 1 MB
request limit.
The backend fetches the source independently, compares title/text, checks the
model/dimensions, and verifies **every vector** against its own embedding model
before reuse. Failed verification cannot inject vectors. This first version
spends server compute on full verification; it does not claim compute savings.
If chunk boundaries differ, the backend computes the missing vectors normally.

A newly indexed member contribution earns **10 credits** when contribution
rewards are enabled. They are off by default; set
`COSIFT_COMMUNITY_CONTRIBUTION_REWARD=on` in the community service environment
(`GET /api/credits` then reports `verified_contribution_reward` and
`verified_contribution_daily_cap`). A reward is paid for new content only: no
document is stored at the submitted or final URL, and none carries the same
content hash. The check reads the backend's content-hash index, and the reward
is not paid while that index is incomplete. Every store, including a new one,
needs it built once, with the backend stopped:

```sh
./cosift backfill-content-sha -dir /srv/cosift/pebble
```

The run is resumable and marks the index complete when it finishes (`-rescan`
walks the corpus again); documents written afterwards are indexed as they are
stored. Each account earns at most `COSIFT_COMMUNITY_CONTRIBUTION_REWARD_DAILY_CAP`
rewards per rolling 24 hours (default 10; an integer from 1 to 10000). Rewards are
globally idempotent by content hash, so retrying or mirroring the same content
cannot earn multiple rewards. Existing corpus URLs and rejected/unverified
submissions do not earn credits. A submission acknowledgement is not a reward:
the backend must confirm approved new content was indexed.

Every successful authenticated retrieval spends credits:

| Mode | Credits per successful request | Hard cap per account |
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
gateway ledger; the MCP service also has a separate daily tool-call cap. Credits
are reserved atomically before retrieval and retained for a successful
backend response, and also when the caller cancels a request after it reaches the
backend. Insufficient credit rejects the request before backend work with HTTP 429 and
`code: "insufficient_credits"`, `credits_required`, `balance` and `topup_url`;
add credits when billing is available, contribute approved new content when
rewards are enabled, or wait for the next monthly grant. Reading a balance or
managing saved requests does not itself spend retrieval credits. In the monthly
activity, `earned` counts contribution rewards, awards, royalties and page-fetch credits, and
`spent` counts retrieval debits, article reads and clawbacks.

In shared mode, `POST /api/meter` charges a covered article read to the bearer
token's account: 2 credits, reported as `article` in `request_credit_costs`. A
`request_id` is charged once; repeating it answers `replayed:true` without a
second charge, and a charge is never refunded. Without an `Authorization: Bearer`
token it answers 401 `bearer_required`, outside shared mode 404, and with too
little credit 402 `insufficient_credits`; like every other POST it answers 403 without the `X-Cosift-Client: community` header. It is limited to 60 calls a minute per
account.

`COSIFT_COMMUNITY_AWARDS=1` (default `0`) starts a poller that every 30 seconds
applies pending credit awards (contribution awards, royalties and clawbacks)
from the shared account store to the ledger, once per award. A clawback is applied
to the account's linked community user even while the account is suspended or
unverified, and it is never given up on; other awards that stay unresolvable for 7 days
are skipped. It needs shared
mode and `COSIFT_WIKI_ENGINE_KEY`, which it uses to confirm each article with
the engine, and the service refuses to start when that key equals
`COSIFT_COMMUNITY_BROKER_KEY`.

## Fetch work

Some sites turn the crawler away: HTTP 403 or 429, a bot-check page, a timeout. Fetch
work lets signed-in contributors fetch such pages from their own network and has the
engine index what is verified. It is off until the engine runs with
`COSIFT_WORK_ENABLED=1` (inside the in-process crawler, so with `-crawl-seeds-file`)
and the community app with `COSIFT_COMMUNITY_WORK=1`; see
[ENV.md](ENV.md#fetch-work). While the engine's switch is off its `/v1/work/*` routes
answer 503 `work_disabled`; while the app's is off its `/api/work/*` routes answer 404.

**The pool.** The engine offers workers URLs from three sources, in this order: URLs the
community app pushes (`pushed`), crawl-frontier rows whose last error was a block (HTTP
403 or 429, a rate limit, challenge or captcha, a bot check, a timeout; `errored`), and
queued frontier rows (`frontier`; off with `COSIFT_WORK_FRONTIER=0`, held to half of
`pool_max`). Hosts take turns within a source, and a host is leased at most once per
`host_spacing_ms`. The crawler's domain policy and the adult filter apply to every URL,
and a URL already in the corpus is never offered. With fetch work on, the app also
pushes, every 5 minutes, up to 100 pending or unverified submissions whose page its own
check could not fetch because the site blocked it, none of them offered in the last 24
hours.

**Leases.** A claim returns up to 32 leases (`max`, default 1). A lease lasts
`lease_ttl_s`. A URL is leased to at most `distinct_leases_per_url` workers at once, and a
worker and a network each get a URL once per `attestation_ttl_h`. The app derives the
worker id from the account, and the network id from the client address (its IPv4 /24 or
IPv6 /48), with salts kept in the community database; a client never sends either. A URL
that `max_attempts` worker and network pairs failed to deliver, or that three pairs
reported as failures, is parked for `attestation_ttl_h`.

**Attestations.** For a 200 the worker submits the page's title, text, language, content
hash and simhash with the extraction version, and the engine recomputes both hashes. Any
other `http_status` is a failure report that returns the lease. Two attestations agree
when their simhashes differ by at most `simhash_max` bits and they come from different
workers on different networks. A page from an `errored` URL is verified by two agreeing
attestations that also agree on title, language and enough of the text, and the text
they agree on is what gets indexed. The engine also fetches the page itself when
`quorum_window_s` ends, and at the next sweep when two attestations of a `pushed` or
`frontier` URL agree. If that copy is within `simhash_max` of an attesting group, the page counts as
verified by the engine; otherwise every attesting worker is a mismatch and the engine's
own copy is used instead. Either way the page then goes through the checks below. A
server fetch that fails, or returns an error or bot-check page, is retried after an hour,
doubling to a day. A record ends after `attestation_ttl_h`.

**Checks and the ceiling.** A verified text passes the adult filter, the quality
prefilter and the content-safety model before it is indexed. Each model call takes one
unit of `work_moderations_per_day`. At that ceiling a claim returns no leases and
`moderation_ceiling: true` with `retry_after_s` (at most an hour), and verified pages
wait as `deferred` until after it; a page the model fails on `moderation_attempts_max` times is
rejected. Indexed pages report `source: "attested"` and `verified_by` (`quorum` or
`server`) in `/v1/contents`; crawled pages report `source: "crawl"`.

**Routes.** All four need a key principal with the `work:broker` scope (add it to the
community principal; see [Start the services](#start-the-services)):

| Method and path | Body / behavior |
| --- | --- |
| `POST /v1/work/claim` | `{worker, net, max}` (ids are 32 hex characters); returns `{leases:[{lease_id,url,source,expires_at,extract_version}], retry_after_s?, moderation_ceiling?}` |
| `POST /v1/work/submit` | A lease's attestation or failure report, up to 2 MiB; returns `{verdict, agreeing, indexed, window_expires_at?, reason?}` with `verdict` one of `awaiting_quorum`, `verified`, `mismatch`, `deferred`, `rejected`, `released` |
| `POST /v1/work/push` | `{urls:[...]}`, 1 to 100; returns `{accepted, duplicates, rejected:[{index,rule}]}` with `rule` one of `url`, `policy`, `pool_full` |
| `GET /v1/work/settled` | `?after=SEQ&limit=1-200` (default 100); returns settled outcomes (`verified`, `mismatch`, `rejected`, `expired`) with the workers each concerns, for the app to credit |

A refusal carries the `/v1` error envelope: 409 `lease_expired`, `lease_not_owner` or
`extract_version_mismatch` (with the engine's `extract_version`), 422 `invalid_field`
with the `field` and `rule`.

**The `work` block of `cosift.json`.** Every key is optional; a value outside its range
stops `pebble-serve` at startup naming the key.

| Key | Default | Range | Effect |
| --- | --- | --- | --- |
| `lease_ttl_s` | 600 | 60-3600 | Seconds a lease lasts |
| `max_leases_per_worker` | 32 | 1-32 | Leases one worker holds at once |
| `max_leases_total` | 2048 | 32-8192 | Leases held across all workers |
| `distinct_leases_per_url` | 3 | 2-3 | Workers a URL is leased to at once |
| `host_spacing_ms` | 2000 | 0-60000 | Minimum gap between leases of one host |
| `quorum_window_s` | 1800 | 60-86400 | Wait after the first attestation before the engine fetches the page itself |
| `simhash_max` | 6 | 0-14 | Largest simhash distance that counts as agreement |
| `attestation_ttl_h` | 168 | 24-168 | Hours a record, a park and a worker or network exclusion last |
| `work_moderations_per_day` | 1000 | 0-1000 | Daily model calls for fetched pages; 0 halts them |
| `community_moderations_per_day` | 1000 | 0-1000 | The community moderation ceiling above |
| `max_attempts` | 6 | 1-20 | Failed worker and network pairs before a URL is parked |
| `moderation_attempts_max` | 5 | 1-10 | Failed model checks before a page is rejected |
| `pool_max` | 20000 | 100-200000 | URLs in the pool |
| `errored_scan_interval_s` | 60 | 5-3600 | Seconds between feeder passes |
| `errored_scan_batch` | 500 | 10-5000 | Frontier rows read per feeder pass |
| `refetch_concurrency` | 2 | 1-8 | Server fetches at once |
| `moderation_concurrency` | 2 | 1-4 | Model checks at once |
| `moderation_queue_max` | 256 | 8-2048 | Verified texts waiting for a check; at the limit, new verifications pause |
| `sweep_interval_s` | 30 | 5-600 | Seconds between sweeps of leases, records and due work |

**The app.** `POST /api/work/claim` (`{max}`, 1 to 32) and `POST /api/work/submit` take
only a bearer token (`Authorization: Bearer ck_...`; no header answers 401
`bearer_required`) and, like every bearer POST, `X-Cosift-Client: community`. The account
must have joined the contributor network and accepted the current
[contributor terms](#public-article-pages): otherwise 403 `not_joined`, `banned` or
`terms_outdated`. An account is limited to 6 claims and 64 submissions a minute and to
`COSIFT_COMMUNITY_FETCH_DAILY_CAP` credited pages per UTC day (429 `daily_cap`, with
`Retry-After` to 00:00 UTC). The submit body may carry only the attestation members; it is
capped at 2 MiB. A verified page whose content was new earns the account 2 credits, once
per page, until its daily cap. The app reads the settled feed every 10 seconds, writes
each contributor's claimed, submitted, verified, mismatch, rejected and credit counters
every 60 seconds (they show under **My contributions**), and raises a
`work_moderation_ceiling` alert, labelled with `COSIFT_ENV`, the first time in a UTC day
that the engine reports each ceiling (fetch work, community moderation).

**The CLI worker.**

```sh
cosift contribute -server https://cosift.pilotprotocol.network -worker -max-pages 100 -duration 30m -concurrency 4
```

`-max-pages` (default 100) and `-duration` (default 30m) stop the run; 0 removes a limit
but at least one must remain. `-concurrency` (default 4, 1 to 32) is the number of leases
in flight. These three flags need `-worker`, and `-worker` cannot be combined with
another operation. It authenticates with the bearer token from `COSIFT_TOKEN` or a saved
session (the installer's, or `-session-file`) that holds one; a cookie-only session or a
password login is refused before any request. Each leased page is fetched as a browser
would (browser request headers, a public-only dialer, each redirect hop checked) and extracted with the same code as
`-index-locally`; a non-200 answer is reported as a failure, and a page too short, too
long or unreadable is skipped. The worker prints one JSON object per line on stdout (events
`claimed`, `submitted`, `reported`, `skipped`, `abandoned`, `waiting`, `lease_lost` and
`stopped`, then a `summary`) and stops at its limits, when no work is offered after 10 polls, at a
moderation ceiling or the daily cap, or on a refusal it cannot get past (a rejected
token, an account that has not joined, is suspended or has outdated terms, or an
extraction version the server does not share, which needs a newer `cosift`). A stop for
a refusal exits non-zero, and so does `unavailable` (a network error or a 5xx answer,
including `work_disabled` when the engine's work switch is off, that persists through 5
tries) and `not_available` (a server without worker mode); the other stops exit normally.

**Operator commands.** `cosift work purge --url URL` and `cosift work void --worker ID`
call the engine's `POST /admin/work-purge` and `POST /admin/work-void`, behind the admin
gate (`cluster.peer_auth_token`). Both are dry runs until `-y`; `-server`, `-token` (default
`COSIFT_ADMIN_TOKEN`) and `-json` are as for `cosift admin`. Purge deletes the attested
document stored under the URL and its final URL (a crawled document is kept and
counted), the URL's record and text, parks the URL for `attestation_ttl_h` and settles every
attesting worker as rejected. Void takes one worker's attestations out of every record
still awaiting, regroups the rest, and parks a URL left without an agreeing pair; the
worker id is the one in the app's `work_workers` table. The engine's `/stats` gains a
`work` block (pool by source, leased, awaiting, verified, mismatch, rejected, expired,
deferred, parked, `moderations_today`, `moderation_ceiling`, `settled_seq`) and
`/metrics` the `cosift_work_*` series, while work is on.

## Article contributions

The engine takes article writes from a contributing service principal. Give an OIDC
principal in the service-auth file the `articles:contribute` scope in place of
`articles:write`; it cannot be combined with `articles:write` or `articles:stub`, and a
key principal cannot hold it. Its `PUT /v1/articles/{id}` calls draw on the same
`writes_per_hour`, `writes_burst` and `writes_per_day` budget as any writer (defaults 30,
10 and 150). A contribution must carry `contributor` (the 16-hex contributor id), have
`status` `published` and `sensitivity.class` `none`, and carry no `topic_ids` or
`cluster_ids`. Updating an existing article also needs `expected_version` and a quality
tier no lower than the stored one (409 `tier_regression`); an article that is held,
rejected or tombstoned refuses it, and one article takes at most 64 contributors (409
`contributors_full`).

Article records and their public view carry `origin` (`synth`, or `community` once a
contribution has been written), `contributors` (the ids, in the order they first
contributed) and, in the full record, `version_contributor` (who wrote the current
version). A community article keeps its last 20 versions, other articles 5, and
`GET /v1/article-versions/{id}` adds `status`, `sensitivity_class`, `version_contributor`
and `reverts_to` to each version.

A principal with `articles:moderate` can call `POST /v1/articles/{id}/revert` with
`{to_version, reason_code, by, request_id?}`. It writes the content of an earlier,
published, sensitivity-`none` version as a new version of a published article, and
answers `{result, article, to_version, reverted_versions, reverted_contributors}` with
`result` `reverted` or, when the content is already current, `unchanged`; repeating a
`request_id` replays the answer with `replayed: true`. A `tombstone` through
`POST /v1/articles/{id}/status` returns the article's `contributors`.
`GET /v1/articles` accepts `tier=` (comma-separated `strong`, `ok`, `thin`) and, for
`articles:read`, `status=pending` to list stubs (`status` takes a comma-separated list;
`articles:read` may name only `published` and `pending`).

## Public article pages

With `COSIFT_WIKI_PUBLIC=1` (see `deploy/community.env.example`) the app serves the
engine's published articles under `/wiki`, with the sitemaps and `robots.txt`, and:

- `/wiki/<slug>.md`: the article as Markdown. Only published articles have one.
- `/llms.txt`: the promoted articles as links to their Markdown form, with links to the
  style guide, the terms and the wanted list.
- `/wiki/style` and `/wiki/style.md`: the contributor style guide; `/wiki/terms`: the
  contributor terms, which a contributor must have accepted (see [Fetch work](#fetch-work));
  `/wiki/wanted`: up to 200 pending stubs (generic title and vertical only) and up to 200
  published articles of `thin` quality, cached for 5 minutes. The slugs `style`,
  `terms` and `wanted` are reserved.

Every article page and Markdown file carries the CC BY-SA 4.0 licence line. A
community-origin article also names its contributors, as `@handle` from the shared
account service; a banned contributor, one the service does not know, and every
contributor when handles cannot be read (local mode, or the lookup failing) appear as
"a community member". A page built while the lookup failed is cached only briefly.
`deploy/Caddyfile.community` routes `/llms.txt` to the app with the other public pages.

## Plans and payments

The **Free plan requires no subscription** and includes the monthly 1,000 credits.
Those credits pay for requests at the same 1/2/3 rates. An optional
**$5/month subscription adds 50,000 credits
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
`-guest-interval`, `-search-rpm`, `-answer-rpm`, and `-research-per-10m` on
the community command. `-guest-interval` defaults to `30m`: successful Search,
Answer, and Research hold the shared guest cooldown for 1, 2, and 3 times that
base interval, respectively. The web app and CLI use the same server policy. `-member-free-rpm` is deprecated and ignored: it cannot
restore the old uncharged member allowance. A guest interval change preserves
the original request time instead of resetting allowances. In-flight requests
reserve a mode slot and, for authenticated retrieval, the mode's credit cost.
Failed backend requests release the mode reservation and refund credits. Mode
limits and balances persist across restarts. At the defaults, an account with
sufficient balance can make up to 120 Search requests/minute, spending one credit
for each successful response.

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

Browser and CLI mutation requests carry `X-Cosift-Client: community`, as do Search, Answer and Research requests authenticated by the session cookie, because they debit credits. Bearer-token and guest Search, Answer and Research (GET) requests do not need it; every other mutation does, including bearer POSTs such as `/api/meter` and `/api/submissions`, which answer 403 without it. JSON mutations use
`Content-Type: application/json`; CSV uses multipart field `file`. Browser
requests must originate from `-public-url`. No cross-origin CORS access is
enabled. CLI clients may omit Origin. Login returns an HttpOnly session cookie named `__Host-cosift_session` when `-public-url` is HTTPS, and `cosift_session` otherwise; requests may still present the legacy name, and login expires it. The CLI reads whichever cookie the login response sets and sends both names to HTTPS origins. The exact Stripe webhook path is exempt from browser CSRF headers and instead requires a valid signature over the raw body.

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
| `GET /api/guest` | Public | Current IP's (per /64 for IPv6) shared cooldown and next available time |
| `GET /api/search?q=...` | Guest or member | Cosift `/search`, preserving backend defaults |
| `GET /api/research?q=...` | Guest or member | Cosift `/research`; plan, synthesized answer and cited sources |
| `GET /api/answer?q=...` | Guest or member | Cosift `/answer`; direct answer and cited sources |
| `GET /api/saved` | Member | Own saved searches |
| `POST /api/saved` | Member | `{query,mode}`; mode defaults to `search`; idempotent per account/query/mode |
| `DELETE /api/saved/{id}` | Member | Removes an owned saved search |
| `GET /api/credits` | Member | Balance, monthly activity, `all_authenticated_requests_metered:true`, weighted request costs, subscription state, top-up eligibility, and payment mode |
| `POST /api/payments/checkout` | Member | `{kind:"subscription"\|"topup",idempotency_key}`; returns a hosted Stripe Checkout URL |
| `POST /api/payments/portal` | Member | `{}`; returns an existing subscriber's restricted billing portal URL |
| `POST /api/payments/webhook` | Stripe signature | Paid invoice/top-up fulfillment, subscription state, and refund reconciliation |
| `GET /api/contributions` | Member | `{joined}`; for a contributor also `handle`, the newest 50 `contributions` (articles with status, credits and article link, refusals, lost reason, sources to fetch), `more`, the `fetch` counters and the `earned` credit rows; 30 calls a minute |
| `POST /api/work/claim` | Bearer token, fetch work on | `{max}`; leases pages to fetch ([Fetch work](#fetch-work)); 6 calls a minute |
| `POST /api/work/submit` | Bearer token, fetch work on | A lease's attestation or failure report, up to 2 MiB; 64 calls a minute |
| `POST /api/meter` | Bearer token, shared mode | `{kind:"article",ref,request_id}`; charges one covered article read, once per `request_id` |
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
allowance records use a salted hash of the client network (the IP address, or the /64
for IPv6) rather than a raw IP. Unauthenticated limits are counted per client
network; limits on submissions, checkout, the billing portal, shared tools, metering,
work claims, work submissions and the contributions list are counted per account, in a bounded store that evicts its oldest
entry instead of refusing new ones. Failed bearer tokens are throttled per /24
(IPv4) or /64 (IPv6) network after 30 a minute; a token that authenticates is
never throttled. Interests provide
clickable search suggestions and shared topic follows; they do not alter retrieval
ranking. Queries go to the configured backend, whose logging policy applies.
Shared production verifies email with a code; standalone mode has no email
verification or self-service password reset. Automatic article authoring remains
separate, unfinished work.

Search returns ordinary result cards; Answer and Research render the returned answer, source citations, and research plan. Their synchronous backend timeout is three minutes; configure reverse proxies to allow at least four minutes. Missing LLM configuration is reported clearly and does not consume the guest allowance.

Tests cover account isolation, mode-aware saved-request migration, endpoint parity, session expiry/logout, CSV atomicity, durable prevalidation/delivery, strict moderation decisions, guest cooldown/restart/concurrency, trusted proxies, public-network egress, and authenticated CLI submissions/guest retrieval. No production deployment is performed
by building or running the app locally.

## Production service and release

See [the operator rollout and rollback record](COMMUNITY-ROLLOUT.md) for the connected production rollout. A release is not ready merely because it builds: verify shared login, source retrieval, contribution receipts, quota persistence, and backups after an upgrade. Billing additionally needs the live Stripe setup below.

`deploy/systemd/cosift-community.service` runs the portal on loopback port 7780.
Create its private data directory before starting it and supply
`COSIFT_COMMUNITY_BROKER_KEY` through root-owned `/etc/cosift/community.env`;
`deploy/community.env.example` lists the variables.
`deploy/Caddyfile.community` routes the root, static assets, `/api/*` and the public
article pages (`/wiki`, `/sitemap.xml`, `/robots.txt`, `/llms.txt`) to the portal, and
routes public `/search`, `/answer`, and `/research` through the same quotas. All unlisted routes, including `/query`, `/find_similar` and `/contents`, return 404. `/v1/*` goes to the engine's service listener, but a `csk_` bearer is answered 403 at the edge, so the broker key works only over loopback. It trusts only loopback and
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
