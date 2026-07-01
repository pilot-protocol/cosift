#!/bin/bash
# cosift-arxiv-harvest — enqueue the latest arXiv papers per category into the
# crawl frontier. THE always-new anchor for the curated corpus.
#
# Why this exists: cosift runs link-following OFF (curated ingester), so it only
# indexes URLs explicitly fed to it. arXiv's RSS endpoints are dead (0 items),
# but the export API (Atom) reliably returns recent submissions. arXiv publishes
# ~500-1000 papers/day across cs.* — all in the quality allowlist — so feeding
# each new paper's abs URL guarantees a continuous stream of new documents.
#
# Frontier dedup means re-running is safe: only genuinely-new papers (the day's
# submissions) actually enter the queue; the rest are no-ops.
set -u

ADDR="http://127.0.0.1:7777"
API="https://export.arxiv.org/api/query"
TOKEN=$(python3 -c "import json;c=json.load(open('/home/ubuntu/cosift.json'));print(c.get('cluster',{}).get('peer_auth_token',''))" 2>/dev/null || echo "")
CATS="${COSIFT_ARXIV_CATS:-cs.AI cs.LG cs.CL cs.CV cs.DC cs.SE cs.CR cs.IR cs.NE cs.DS cs.DB cs.RO stat.ML}"
MAXR="${COSIFT_ARXIV_MAX:-100}"

total=0
for cat in $CATS; do
  # arXiv asks for >=3s between API hits; -L follows the http->https redirect.
  xml=$(curl -sL --max-time 90 "${API}?search_query=cat:${cat}&sortBy=submittedDate&sortOrder=descending&max_results=${MAXR}")
  urls=$(printf '%s' "$xml" | python3 -c "
import sys,re
ids=re.findall(r'<id>(https?://arxiv\.org/abs/[^<]+)</id>', sys.stdin.read())
for u in ids: print(u)
")
  n=0
  while IFS= read -r u; do
    [ -z "$u" ] && continue
    curl -s -o /dev/null --max-time 30 -X POST "$ADDR/admin/crawl-enqueue" \
      -H "Content-Type: application/json" \
      ${TOKEN:+-H "Authorization: Bearer $TOKEN"} \
      -d "{\"url\":\"$u\"}"
    n=$((n+1))
  done <<< "$urls"
  echo "cosift-arxiv-harvest: $cat -> enqueued $n"
  total=$((total + n))
  sleep 3
done
echo "cosift-arxiv-harvest: total enqueued=$total at $(date -u +%FT%TZ)"
