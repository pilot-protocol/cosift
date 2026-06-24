#!/bin/bash
# cosift-sitemap-refresh — re-discover + queue sitemaps for curated quality
# hosts (cosift-sites.txt) so newly-published pages enter the crawl frontier.
# Companion to cosift-rss-refresh.sh: RSS catches the latest-N article links
# fast (every 30min); this sweeps each host's full sitemap a few times a day to
# catch pages RSS missed. The crawler's refetch window (COSIFT_REFETCH_AFTER_*)
# + frontier dedup mean re-submitting mostly queues genuinely-new URLs.
#
# Runs SEQUENTIALLY with spacing — site-submit pulls full sitemaps (can be
# large), so we keep it gentle to avoid a crawl/index burst.
set -u

SITES=/home/ubuntu/cosift-sites.txt
ADDR="http://127.0.0.1:7777"
LANE="${COSIFT_SITEMAP_LANE:-refresh}"
SPACING="${COSIFT_SITEMAP_SPACING:-3}"   # seconds between hosts
TOKEN=$(python3 -c "import json; c=json.load(open('/home/ubuntu/cosift.json')); print(c.get('cluster',{}).get('peer_auth_token',''))" 2>/dev/null || echo "")

if [ ! -f "$SITES" ]; then
  echo "cosift-sitemap-refresh: $SITES not found; nothing to do"
  exit 0
fi

total=0
while IFS= read -r HOST; do
  case "$HOST" in ''|\#*) continue;; esac
  HOST=$(echo "$HOST" | tr -d '[:space:]')
  [ -z "$HOST" ] && continue
  RESP=$(curl -s --max-time 300 -X POST "$ADDR/admin/site-submit" \
    -H "Content-Type: application/json" \
    ${TOKEN:+-H "Authorization: Bearer $TOKEN"} \
    -d "{\"host\":\"$HOST\",\"lane\":\"$LANE\"}")
  Q=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_queued','?'))" 2>/dev/null || echo "?")
  echo "cosift-sitemap-refresh: $HOST -> queued $Q (lane=$LANE)"
  [ "$Q" != "?" ] && [ -n "$Q" ] && total=$((total + Q))
  sleep "$SPACING"
done < "$SITES"

echo "cosift-sitemap-refresh: done, total queued=$total at $(date -u +%FT%TZ)"
