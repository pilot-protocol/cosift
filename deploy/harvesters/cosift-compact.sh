#!/bin/bash
# cosift-compact.sh — invoke hnsw-compact when zombies exceed a threshold.
# Run from cosift-compact.timer (weekly recommended).
set -e
TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
THRESHOLD_PCT=30
# /admin/hnsw-compact is gated by cluster.peer_auth_token — read it from the
# same config the server uses so this keeps working once the token is set.
TOKEN=$(python3 -c "import json; print(json.load(open('/home/ubuntu/cosift.json')).get('cluster',{}).get('peer_auth_token',''))" 2>/dev/null || echo "")
STATS=$(curl -s --max-time 30 http://127.0.0.1:7777/stats)
if [ -z "$STATS" ]; then
  echo "${TS} stats fetch failed" | sudo tee -a /var/log/cosift-compact.log >/dev/null
  exit 0
fi
PCT=$(echo "$STATS" | python3 -c "
import json,sys
d=json.load(sys.stdin)
t=d[\"pq\"][\"nodes_total\"]
z=d[\"pq\"][\"zombie_nodes\"]
if t==0: print(0); exit()
print(int(100*z/t))
")
echo "${TS} zombies=${PCT}% threshold=${THRESHOLD_PCT}%" | sudo tee -a /var/log/cosift-compact.log >/dev/null
if [ "$PCT" -lt "$THRESHOLD_PCT" ]; then
  echo "${TS} below threshold, skipping compact" | sudo tee -a /var/log/cosift-compact.log >/dev/null
  exit 0
fi
RESULT=$(curl -s --max-time 1800 -X POST ${TOKEN:+-H "Authorization: Bearer $TOKEN"} http://127.0.0.1:7777/admin/hnsw-compact)
echo "${TS} compact result: ${RESULT}" | sudo tee -a /var/log/cosift-compact.log >/dev/null
