"""Loopback fixture for the Go remote client and optional browser checks.
Runs real MCP protocol/tools with fake identity/topic storage. No cloud access.
"""
import os
from cosift_mcp.app import build_app
from tests.test_mcp_protocol import FakeTopicStore, OkVerifier, make_config

from cosift_mcp.tools import lookup
from cosift_mcp.topics.resolve import NO_MATCH

async def no_model_resolution(cfg, text):
    return NO_MATCH

lookup._resolve_topic = no_model_resolution

class Topics(FakeTopicStore):
    def __init__(self):
        super().__init__()
        self.rows = {}
    async def is_requested(self, uid, tid):
        return bool(self.rows.get(uid, {}).get(tid, {}).get("requested_at"))
    async def list_topics(self, uid, limit=100):
        return list(self.rows.get(uid, {}).values())[:limit]
    async def add_topic(self, uid, tid, text, source):
        row = self.rows.setdefault(uid, {})
        if tid in row:
            return False
        row[tid] = {"topic_id": tid, "topic_text": text}
        return True
    async def remove_topic(self, uid, tid):
        return self.rows.setdefault(uid, {}).pop(tid, None) is not None
    async def request_topic(self, uid, tid, text, why):
        row = self.rows.setdefault(uid, {})
        if tid in row and row[tid].get("requested_at"):
            return False, row[tid]
        from datetime import datetime, timezone
        row[tid] = {"topic_id": tid, "topic_text": text, "requested_at": datetime.now(timezone.utc)}
        return True, row[tid]

app = build_app(make_config(allowed_hosts=("127.0.0.1:*", "localhost:*")), verifier=OkVerifier(), topic_store=Topics())
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ["COSIFT_MCP_FIXTURE_PORT"]), log_level="warning")
