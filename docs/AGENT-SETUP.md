# Use Cosift from your agent

[Open Cosift](https://cosift.pilotprotocol.network/) to sign in with an email code,
search, get a cited answer, run research, save requests, or contribute public pages.
The corpus is growing and incomplete. Try any topic; a coverage miss is useful
feedback, and good public sources from any field are welcome.

## Install and connect

Start on the [Cosift website](https://cosift.pilotprotocol.network/): sign in and
open **Agents** for connection instructions. Then run the public installer in your
own terminal, using the same email:

```sh
curl -fsSL https://raw.githubusercontent.com/pilot-protocol/cosift-install/v1/install.sh | sh
```

It detects supported agents, confirms your email, configures their MCP connection,
and installs the onboarding skill. Onboarding can suggest interests from local
agent history; review what you share. Never submit raw private history as a
contribution. See the [installer documentation](https://github.com/pilot-protocol/cosift-install)
for supported harnesses and options.

For a manual connection, use streamable HTTP:

| Setting | Value |
| --- | --- |
| MCP URL | `https://cosift-mcp-udik5erlkq-uw.a.run.app/v1/mcp` |
| Authentication | `Authorization: Bearer <your Cosift token>` |

Use the token obtained by the installer in your agent's private configuration.
Do not paste it into chats, commit it, or send it to another origin. Public
transport still requires a Cosift account token for every MCP tool. Website
login establishes a browser session; it does not automatically configure an MCP
client or expose a copy-token button. The installer verifies your email and
saves the agent token in private client configuration.

## Tools your agent can use

The examples below are tool arguments, not shell commands.

| Tool | Example arguments | Result |
| --- | --- | --- |
| `cosift_search` | `{"query":"Go modules tutorial","k":3}` | Source URLs, titles and excerpts; fewer hits or no hits are possible. |
| `cosift_lookup` | `{"topic":"Go dependency management"}` | Checks curated article coverage; a miss can include retry guidance. |
| `cosift_request` | `{"topic":"Go dependency management","why":"Compare module versioning approaches"}` | Records an explicit request for coverage. |
| `cosift_topics` | `{"action":"list"}` | Lists this account's followed/requested topics. |
| `cosift_topics` | `{"action":"add","topics":["Go dependency management"]}` | Follows interests; use `remove` with the same shape to unfollow. |

Search returns sources, not a synthesized answer. Use the web app or CLI for
Answer and Research. Curated article generation is not enabled in the current
release: lookup/request connects the coverage and demand workflow without
promising an article or delivery date. Following a topic does not request an
article, and unfollowing does not erase previously requested-topic history.

## Add the general Cosift skill

The installer already supplies onboarding. The separate
[Cosift usage skill](../skills/cosift/SKILL.md) teaches agents how to search,
handle coverage misses, follow topics, and contribute with the CLI.
[Download its SKILL.md](https://raw.githubusercontent.com/pilot-protocol/cosift/main/skills/cosift/SKILL.md).
Place that file at `cosift/SKILL.md` inside your agent's configured skill directory,
then reload the agent. This does not require uploading a token to the skill file.

## Authenticated CLI

Use [Cosift v0.2.7 or a newer stable release](https://github.com/pilot-protocol/cosift/releases/latest)
for your OS and architecture; verify its signature using the [signed CLI guide](https://github.com/pilot-protocol/cosift-install/blob/main/docs/CLI-INSTALL.md). Put the binary on your PATH before running the installer so it can connect the CLI. If you installed the binary later, rerun the installer with `--cli` to save the shared session. After that handoff, the CLI discovers the private `cosift/community-session.json` under
`$XDG_CONFIG_HOME`, or `~/.config` when unset. An explicit `-session-file` can select
another saved session. Keep session files private (mode `0600`).

```sh
cosift request -server https://cosift.pilotprotocol.network -query "Go modules tutorial"
cosift request -server https://cosift.pilotprotocol.network -mode answer -query "What is a Go module?"
cosift request -server https://cosift.pilotprotocol.network -mode research -query "How does Go module versioning work?"
cosift contribute -server https://cosift.pilotprotocol.network https://go.dev/doc/modules/
cosift contribute -server https://cosift.pilotprotocol.network -csv sources.csv
cosift contribute -server https://cosift.pilotprotocol.network -credits
```

Contributions require login. A CSV can contain one URL per row or a `url`, `urls`,
`webpage`, or `website` column. A request accepts at most 100 URLs and 1 MB;
an account may submit 1,000 new URLs per rolling 24 hours.
Put flags before positional URLs. For example:

```csv
url
https://go.dev/doc/modules/
https://www.rust-lang.org/learn
```

Submit useful public pages you are authorized to share. URL and content checks
screen adult material, harmful content, malware/phishing, spam and low-quality
filler; useful educational, medical and defensive-security material is allowed.
Accepted submissions enter validation; acceptance alone does not mean indexing
or a credit award. Check contribution status in the web app. Checks are automated
and do not guarantee that every unsafe or poor-quality page is detected.
Production combines URL/network/content checks with semantic review using
`qwen3.5:9b-fp8`. Uncertain or unsupported material stays unverified. Text and
metadata review does not guarantee moderation of images or video.

## Index and embed locally

The CLI can fetch, extract, chunk and embed a page locally, retain its local index,
then submit its text, metadata and vectors. Configure a local embedding service
matching the destination model and dimensions. Production currently uses
`nomic-embed-text` with 768 dimensions; confirm compatibility before a large run.

Example `local.json` for an OpenAI-compatible local embedding endpoint:

```json
{"data_dir":"./cosift-local-data","embeddings":{"url":"http://127.0.0.1:11434/v1","model":"nomic-embed-text","dim":768}}
```

```sh
cosift -config local.json contribute -server https://cosift.pilotprotocol.network -index-locally https://go.dev/doc/modules/
```

The server independently checks the source, content safety and quality, model,
dimensions and every vector before reuse. Current limits are 32,000 text bytes,
64 chunks per page and the same 1 MB submission limit. This verification still
uses server compute; local embeddings do not bypass validation.

## Allowance and credits

Web, CLI, and MCP searches share the account's gateway allowance and credit ledger.
Every account gets **60 shared free requests per minute plus 1,000 free credits
per UTC calendar month**, with no subscription required. The monthly grant is
applied once on authenticated use in the current month; inactive past months are
not backfilled. Unused credits carry over.

A verified new contribution earns **10 credits** once per unique content.
Rejected, unverified, duplicate, or already-indexed pages earn none. After the
shared free requests, an extra successful request spends:

| Mode | Credits | Hard cap per account |
| --- | --- | --- |
| Search | 1 | 120/minute |
| Answer | 2 | 20/minute |
| Research | 3 | 3/10 minutes |

Failed backend requests release reservations and refund credits. Credits do not
bypass mode limits or MCP's separate daily call cap. Check
[`/api/limits`](https://cosift.pilotprotocol.network/api/limits) and the authenticated
credits view for current policy. Respect retry guidance after a rate limit.

## Optional paid plan

The Billing page offers a **$5/month subscription for 50,000 additional credits
per paid month**. Subscribers keep their 1,000 free monthly credits and can also
buy **$5/50,000-credit one-time top-ups**. Top-ups require a paid current
subscription period. At this price, 1,000 credit-funded requests cost $0.10 for
Search, $0.20 for Answer, or $0.30 for Research.

Unused credits carry over. Cancel through the billing portal; cancellation does
not remove your remaining earned or purchased balance. Refunds revoke the
corresponding purchased credits. Live purchases stay unavailable until the
operator configures live Stripe billing. An existing credit balance does not
mean payment is enabled. The app displays the payment mode, and test payments
belong only on an isolated test ledger. See [Stripe configuration](STRIPE.md) for
webhook events, portal restrictions, and activation checks.
