#!/bin/bash
# cosift-wet-refresh — rotating CommonCrawl WET ingest.
#
# Walks a CC release's ~100k-file manifest one slice per run so each run
# ingests NEW files instead of re-reading 0-9 (the old script hardcoded
# skip=0,count=10 and dedup'd to ~zero net docs every week). State (release +
# next offset) persists in STATE_FILE. When a newer CC release appears, the
# offset resets to 0 so we migrate to the freshest crawl automatically.
#
# Tunables (env):
#   COSIFT_WET_COUNT         files per run        (default 100 ≈ 2.1M docs)
#   COSIFT_WET_CONCURRENCY   parallel imports     (default 4, handler caps 8)
#   COSIFT_WET_LEXICAL_ONLY  skip embeddings      (default true — fast BM25 ingest)
set -u

ADDR="http://127.0.0.1:7777"
STATE_FILE="/home/ubuntu/cosift-wet-state.txt"
COUNT="${COSIFT_WET_COUNT:-50}"
CONCURRENCY="${COSIFT_WET_CONCURRENCY:-2}"
LEXICAL_ONLY="${COSIFT_WET_LEXICAL_ONLY:-true}"
MANIFEST_FILES=100000   # CC releases ship ~100k WET files

TOKEN=$(python3 -c "import json; c=json.load(open('/home/ubuntu/cosift.json')); print(c.get('cluster',{}).get('peer_auth_token',''))" 2>/dev/null || echo "")

# Latest CC release id (e.g. CC-MAIN-2026-21).
LATEST=$(curl -s --max-time 30 https://data.commoncrawl.org/crawl-data/index.html \
  | grep -oE 'CC-MAIN-[0-9]{4}-[0-9]{2}' | sort -u | tail -1)
if [ -z "$LATEST" ]; then
  echo "cosift-wet-refresh: could not determine latest release; aborting"
  exit 1
fi

# Persisted (release, offset). Reset to 0 when the release rolls over.
STATE_RELEASE=""
OFFSET=0
if [ -f "$STATE_FILE" ]; then
  read -r STATE_RELEASE OFFSET < "$STATE_FILE" || true
  [ -z "${OFFSET:-}" ] && OFFSET=0
fi
if [ "$STATE_RELEASE" != "$LATEST" ]; then
  echo "cosift-wet-refresh: release rolled '${STATE_RELEASE:-none}' -> $LATEST, resetting offset to 0"
  OFFSET=0
fi
if [ "$OFFSET" -ge "$MANIFEST_FILES" ]; then
  echo "cosift-wet-refresh: walked entire $LATEST manifest, wrapping offset to 0"
  OFFSET=0
fi

JSON=$(printf '{"manifest_url":"https://data.commoncrawl.org/crawl-data/%s/wet.paths.gz","count":%d,"skip":%d,"concurrency":%d,"lexical_only":%s}' \
  "$LATEST" "$COUNT" "$OFFSET" "$CONCURRENCY" "$LEXICAL_ONLY")
echo "cosift-wet-refresh: release=$LATEST skip=$OFFSET count=$COUNT concurrency=$CONCURRENCY"

# Advance the offset BEFORE the import. wet-import-bulk is idempotent (URL
# dedup), so the worst case from advancing early is skipping a slice on a hard
# failure — acceptable in a 100k-file manifest. The failure we actually hit was
# the opposite: a successful server-side import whose response the loaded box
# dropped, leaving the offset stuck and re-reading the same slice forever.
# Advancing first makes the walk monotonic regardless of response delivery.
NEXT=$((OFFSET + COUNT))
echo "$LATEST $NEXT" > "$STATE_FILE"

RESP=$(curl -s --retry 2 --retry-delay 30 --max-time 7200 -X POST "$ADDR/admin/wet-import-bulk" \
  -H 'Content-Type: application/json' \
  ${TOKEN:+-H "Authorization: Bearer $TOKEN"} \
  -d "$JSON")

TOTAL=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_indexed',''))" 2>/dev/null || echo "")
if [ -z "$TOTAL" ]; then
  # Import may have succeeded server-side even if we couldn't read the response
  # (heavy write bursts can drop the client connection). Offset already advanced,
  # so the walk continues; check the cosift-serve journal for the real result.
  echo "cosift-wet-refresh: response unreadable (import may have succeeded server-side); offset already advanced $OFFSET -> $NEXT. Check 'journalctl -u cosift-serve' for 'wet-import-bulk: indexed'. Response head:"
  echo "$RESP" | head -c 500
  exit 0
fi

echo "cosift-wet-refresh: indexed=$TOTAL, advanced offset $OFFSET -> $NEXT, done at $(date -u +%FT%TZ)"
