#!/bin/bash
# cosift-compact.sh — start hnsw-compact when zombies exceed a threshold and
# follow the async job through /stats.hnsw_compact. Run from cosift-compact.timer.
set -u
TS() { date -u +%Y-%m-%dT%H:%M:%SZ; }
LOG=/var/log/cosift-compact.log
log() { echo "$(TS) $*" | sudo tee -a "$LOG" >/dev/null; }
THRESHOLD_PCT=${THRESHOLD_PCT:-15}
POLL_SEC=${POLL_SEC:-30}
MAX_WAIT_SEC=${MAX_WAIT_SEC:-28800}
STATS_URL=http://127.0.0.1:7777/stats
TOKEN=$(python3 -c "import json; print(json.load(open('/home/ubuntu/cosift.json')).get('cluster',{}).get('peer_auth_token',''))" 2>/dev/null || echo "")

STATS=$(curl -s --max-time 30 "$STATS_URL")
if [ -z "$STATS" ]; then
  log "stats fetch failed"
  exit 0
fi
PCT=$(echo "$STATS" | python3 -c "
import json,sys
d=json.load(sys.stdin)
pq=d.get('pq')
if not pq: print(-1); exit()
t=pq['nodes_total']; z=pq['zombie_nodes']
print(int(100*z/t) if t else 0)
")
if [ "$PCT" -lt 0 ]; then
  log "graph not loaded (no pq in /stats), skipping"
  exit 0
fi
log "zombies=${PCT}% threshold=${THRESHOLD_PCT}%"
if [ "$PCT" -lt "$THRESHOLD_PCT" ]; then
  log "below threshold, skipping compact"
  exit 0
fi

START=$(curl -s --max-time 30 -o /dev/stderr -w '%{http_code}' -X POST ${TOKEN:+-H "Authorization: Bearer $TOKEN"} http://127.0.0.1:7777/admin/hnsw-compact 2>&1)
CODE=${START: -3}
if [ "$CODE" != "202" ]; then
  log "compact start returned HTTP ${CODE}: ${START%???}"
  exit 0
fi
log "compact started"
DEADLINE=$(( $(date +%s) + MAX_WAIT_SEC ))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  sleep "$POLL_SEC"
  JOB=$(curl -s --max-time 30 "$STATS_URL" | python3 -c "
import json,sys
j=json.load(sys.stdin).get('hnsw_compact',{})
print(j.get('state','?'), json.dumps(j, sort_keys=True))
" 2>/dev/null)
  STATE=${JOB%% *}
  case "$STATE" in
    running) log "running: ${JOB#* }" ;;
    done|error) log "compact ${STATE}: ${JOB#* }"; exit 0 ;;
    *) log "stats unavailable (${STATE}), still waiting" ;;
  esac
done
log "gave up waiting after ${MAX_WAIT_SEC}s; job may still be running"
