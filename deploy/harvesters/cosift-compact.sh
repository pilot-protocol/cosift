#!/bin/bash
# cosift-compact.sh — start hnsw-compact when zombies exceed a threshold and
# follow the async job through /stats.hnsw_compact. Run from cosift-compact.timer.
set -u -o pipefail
TS() { date -u +%Y-%m-%dT%H:%M:%SZ; }
LOG=/var/log/cosift-compact.log
log() { local line; line="$(TS) $*"; echo "$line"; echo "$line" | sudo tee -a "$LOG" >/dev/null; }
THRESHOLD_PCT=${THRESHOLD_PCT:-15}
POLL_SEC=${POLL_SEC:-30}
MAX_WAIT_SEC=${MAX_WAIT_SEC:-43200}
MIN_FREE_FACTOR=${MIN_FREE_FACTOR:-2.5}
DATA_DIR=${DATA_DIR:-/home/ubuntu/cosift-data}
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
" 2>/dev/null)
case "$PCT" in ''|*[!0-9-]*) log "stats parse failed (pct=${PCT:-empty}), skipping"; exit 0 ;; esac
if [ "$PCT" -lt 0 ]; then
  log "graph not loaded or busy (no pq in /stats), skipping"
  exit 0
fi
log "zombies=${PCT}% threshold=${THRESHOLD_PCT}%"
if [ "$PCT" -lt "$THRESHOLD_PCT" ]; then
  log "below threshold, skipping compact"
  exit 0
fi

# The swap holds both node slots on disk at once; refuse without headroom.
USED_KB=$(du -sk "$DATA_DIR" 2>/dev/null | cut -f1)
FREE_KB=$(df -Pk "$DATA_DIR" 2>/dev/null | awk 'NR==2{print $4}')
if [ -n "$USED_KB" ] && [ -n "$FREE_KB" ]; then
  NEED_KB=$(python3 -c "print(int($USED_KB*$MIN_FREE_FACTOR))")
  if [ "$FREE_KB" -lt "$NEED_KB" ]; then
    log "insufficient disk: free=${FREE_KB}K need=${NEED_KB}K (${MIN_FREE_FACTOR}x store), skipping compact"
    exit 1
  fi
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
    done) log "compact done: ${JOB#* }"; exit 0 ;;
    error) log "compact ERROR: ${JOB#* }"; exit 1 ;;
    *) log "stats unavailable (${STATE:-empty}), still waiting" ;;
  esac
done
log "gave up waiting after ${MAX_WAIT_SEC}s; job may still be running"
exit 1
