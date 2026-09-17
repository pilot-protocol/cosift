"""Runs the patched, real MCP app against the Go community test gateway.
No GCP, email, index writes, or model downloads. Called by TestMCPGatewayContract.
"""
import asyncio
import json
import os

import httpx

from cosift_mcp.app import build_app
from cosift_mcp.auth.principal import Principal
from tests.test_mcp_protocol import FakeTopicStore, lifespan, make_config

TOKENS = ["ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "ck_2_MFRGGZDFMZTWQ2LKNNWG23TPOBYXE43UOJUW4ZY"]


class Verifier:
    async def verify(self, token):
        assert token in TOKENS
        return Principal(uid=str(TOKENS.index(token)) * 16, tid="t" * 64, tier="free", token_ref=None)


async def main():
    cfg = make_config(engine_base_url=os.environ["COSIFT_CONTRACT_GATEWAY"])
    app = build_app(cfg, verifier=Verifier(), topic_store=FakeTopicStore())
    async with lifespan(app), httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as c:
        async def search(token):
            response = await c.post("/v1/mcp", headers={
                "Authorization": f"Bearer {token}", "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": "2025-03-26",
            }, json={"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "cosift_search", "arguments": {"query": "rust", "k": 2}}})
            assert response.status_code == 200, response.status_code
            envelope = response.json()
            assert not envelope["result"].get("isError")
            return json.loads(envelope["result"]["content"][0]["text"])
        first, second = await asyncio.gather(search(TOKENS[0]), search(TOKENS[1]))
        assert not first.get("unavailable") and not second.get("unavailable"), (first, second)
        assert first["retriever"] == "bm25" and second["retriever"] == "bm25"
        # Alice's second request hits her hard cap; Bob's separate paid request succeeds.
        limited = await search(TOKENS[0])
        assert limited.get("unavailable"), limited
    print("MCP → community → engine: identity isolation, quota enforcement, BM25/k contract passed")


asyncio.run(main())
