# MCP companion for community PR #58

`forward-account-token.patch` applies to cosift-mcp
`e7477f21cf5439e1c55a53ddd7d1b0af080342c5`. It is a review artifact, not a deployment.

In a separate clean checkout of that revision:

```sh
git apply --check /absolute/path/to/cosift/integrations/cosift-mcp/forward-account-token.patch
git apply /absolute/path/to/cosift/integrations/cosift-mcp/forward-account-token.patch
uv sync --frozen
uv run --frozen pytest -m 'not integration' -q
```

From the Cosift repository, run the cross-repository search contract:

```sh
COSIFT_MCP_CHECKOUT=/absolute/path/to/patched/cosift-mcp GOWORK=off go test -race ./internal/community -run TestMCPGatewayContract -v
```

For the real MCP protocol/topic-tool contract, run this fixture with the patched
checkout's Python runtime and `PYTHONPATH` pointing at that checkout:

```sh
COSIFT_MCP_FIXTURE_PORT=17981 PYTHONPATH=/absolute/path/to/cosift-mcp /absolute/path/to/cosift-mcp/.venv/bin/python /absolute/path/to/cosift/integrations/cosift-mcp/remote_contract_server.py
```

In another terminal in Cosift:

```sh
COSIFT_MCP_CONTRACT_URL=http://127.0.0.1:17981/v1/mcp GOWORK=off go test ./internal/sharedaccount -run TestMCPRemoteWireContract -v
# Optional manual UI fixture: prints a disposable loopback URL, expires in 8 minutes.
COSIFT_BROWSER_MCP_FIXTURE=http://127.0.0.1:17981/v1/mcp GOWORK=off go test ./internal/community -run '^TestSharedBrowserFixture$' -v
```

The fixtures have fabricated identity and in-memory topic state, use no cloud
credentials, and are never compiled into the production binary. Stop the Python
fixture after testing. The complete integration configuration and outstanding
staging checks are in `docs/SHARED-ACCOUNTS.md`.
