# Use Cosift from your agent

[Open Cosift](https://cosift.pilotprotocol.network/) to sign in with an email code,
search, get a cited answer, run research, save requests, or contribute public pages.
The corpus is growing and incomplete. Try any topic; a coverage miss is useful
feedback, and good public sources from any field are welcome.

## Install and connect

Run the public installer in your own terminal:

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
transport still requires a Cosift account token for every MCP tool.

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
for your OS and architecture. Put the binary on your PATH. After installer login,
the CLI discovers the private `cosift/community-session.json` under
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
`webpage`, or `website` column. A request accepts at most 100 URLs and 1 MB.
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

Web, CLI and MCP searches share the account's gateway allowance and credit ledger.
A verified new contribution earns 10 credits once per unique content; rejected,
unverified, duplicate or already-indexed pages earn none. After the free allowance,
an extra successful Search, Answer or Research request spends one credit. Credits
do not bypass mode limits or MCP's separate daily call cap. Check
[`/api/limits`](https://cosift.pilotprotocol.network/api/limits) and the authenticated
credits view for current policy. Respect retry guidance after a rate limit.

Credit purchasing is available only when payments are enabled in the app.
There are no automatic charges or subscriptions. The integration supports a
one-time $5 purchase of 50,000 credits; payment availability is not implied by
having a balance.
