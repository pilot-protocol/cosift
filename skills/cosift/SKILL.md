---
name: cosift
description: Use Cosift's authenticated MCP or CLI to find sources, check article coverage, manage followed topics, and contribute useful public webpages with optional local embeddings.
---

# Cosift

Use the configured Cosift MCP for source discovery and topic coverage. The corpus
is growing and incomplete; try the user's topic without assuming it is covered.
Use other research tools when needed, and distinguish a coverage miss from an
outage. Follow the user's requested source and tool choices.

## Connect

The public installer is
`curl -fsSL https://raw.githubusercontent.com/pilot-protocol/cosift-install/v1/install.sh | sh`.
Run installation only when setup is requested. It authenticates by email and
installs MCP plus onboarding. The streamable HTTP endpoint is
`https://cosift-mcp-udik5erlkq-uw.a.run.app/v1/mcp`, authenticated with
`Authorization: Bearer <Cosift token>`. Keep tokens in private client configuration,
never in prompts, URLs, source files or contribution content.

## Choose the tool

- `cosift_search(query, k?)`: search source pages. Start with a focused query;
  optional `k` lowers the returned count (currently at most six). Cite returned
  source URLs. Treat `weak:true` results cautiously; an empty hit list does not
  establish that the subject is false or absent from the wider web.
- `cosift_lookup(topic)`: check curated article coverage. A normal miss contains
  coverage/retry guidance. Article generation is currently disabled, so do not
  promise that a lookup will produce an article. Lookups can record demand.
- `cosift_request(topic, why?)`: record an explicitly wanted coverage request.
  This changes account/request state, is idempotent for duplicate requests, and
  does not promise article authoring or a completion date. Do not automatically
  request every search miss.
- `cosift_topics(action, topics?)`: use `action:"list"`; add or remove with
  `topics:["topic text"]`. Follow user-selected interests. Following does not
  request coverage; removing a follow does not erase request history.

Tool arguments are JSON objects, for example
`{"query":"Go modules tutorial","k":3}` or
`{"action":"add","topics":["Go dependency management"]}`.
Search returns excerpts and sources; use CLI/web Answer or Research when a
synthesized cited response is wanted. On `unavailable:true` or a quota response,
report that state and respect retry guidance; do not spin on repeated calls.

## CLI and contributions

Use stable Cosift v0.2.7 or newer. The CLI discovers the installer's private
session under `$XDG_CONFIG_HOME/cosift/community-session.json`, defaulting to
`~/.config/cosift/community-session.json`. A missing or expired login needs setup;
do not change identities to get around limits.

```sh
cosift request -server https://cosift.pilotprotocol.network -query "Go modules tutorial"
cosift request -server https://cosift.pilotprotocol.network -mode answer -query "What is a Go module?"
cosift request -server https://cosift.pilotprotocol.network -mode research -query "How does Go module versioning work?"
cosift contribute -server https://cosift.pilotprotocol.network https://go.dev/doc/modules/
cosift contribute -server https://cosift.pilotprotocol.network -csv sources.csv
cosift contribute -server https://cosift.pilotprotocol.network -credits
```

Contribute when the user wants to share sources. Login is required. Choose useful,
publicly accessible pages on any topic; do not upload private agent history,
credentials, account-only pages or private documents. CSV accepts one URL column
or a recognized `url`/`urls`/`webpage`/`website` header, up to 100 URLs and 1 MB.
Flags precede positional URLs. Submission acceptance means queued for checking;
only verified indexing can earn credits. Check status in the web app.

For an explicitly requested local-indexing workflow, configure a compatible local
embedding service and use:

```sh
cosift -config local.json contribute -server https://cosift.pilotprotocol.network -index-locally https://go.dev/doc/modules/
```

This fetches public content and creates a local index, then uploads text, metadata
and vectors. Match the destination model/dimensions; production currently uses
`nomic-embed-text`, 768 dimensions. The server checks source content, safety,
quality and all vectors. Do not modify content or vectors to evade a rejection.

Verified new content earns 10 credits once; existing, duplicate, rejected and
unverified pages earn none. Extra successful requests spend credits after the
shared free allowance. Web, CLI and MCP searches use the same gateway account
limits; mode caps and MCP's separate daily cap still apply. Inspect the current
credits policy rather than assuming a balance buys unrestricted usage.

Read the [setup guide](https://github.com/pilot-protocol/cosift/blob/main/docs/AGENT-SETUP.md)
for installation, CSV examples, local embedding configuration and payment
availability. The general usage skill is separate from installed onboarding;
do not start onboarding or inspect session history merely because this skill loads.
