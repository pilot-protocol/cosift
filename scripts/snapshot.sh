#!/usr/bin/env bash
# Tarball + gzip a Pebble checkpoint + config, upload to GCS, prune old.
# Uses /admin/checkpoint to avoid racing the compactor.
#
# Env / defaults:
#   COSIFT_ADMIN_URL    http://127.0.0.1:7777
#   COSIFT_ADMIN_TOKEN  (optional Bearer for /admin/checkpoint)
#   COSIFT_CONFIG       /home/ubuntu/cosift.json
#   COSIFT_GCS_BUCKET   gs://pilot-cosift-index
#   COSIFT_KEEP         14   (latest N snapshots; older are deleted)
set -euo pipefail

ADMIN_URL="${COSIFT_ADMIN_URL:-http://127.0.0.1:7777}"
ADMIN_TOKEN="${COSIFT_ADMIN_TOKEN:-}"
CONFIG="${COSIFT_CONFIG:-/home/ubuntu/cosift.json}"
BUCKET="${COSIFT_GCS_BUCKET:-gs://pilot-cosift-index}"
KEEP="${COSIFT_KEEP:-14}"

stamp="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
tmp="$(mktemp -d)"
ckpt=""
cleanup() {
  rm -rf "$tmp"
  if [[ -n "$ckpt" && -d "$ckpt" ]]; then
    rm -rf "$ckpt"
  fi
}
trap cleanup EXIT

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) snapshot: $*"; }

log "requesting checkpoint via $ADMIN_URL/admin/checkpoint"
auth_args=()
if [[ -n "$ADMIN_TOKEN" ]]; then
  auth_args=(-H "Authorization: Bearer $ADMIN_TOKEN")
fi
resp="$(curl -fsS -X POST "${auth_args[@]}" "$ADMIN_URL/admin/checkpoint")"
ckpt="$(printf '%s' "$resp" | python3 -c 'import sys,json; print(json.load(sys.stdin)["path"])')"
if [[ -z "$ckpt" || ! -d "$ckpt" ]]; then
  echo "snapshot: checkpoint did not return a valid path (response: $resp)" >&2
  exit 1
fi
log "checkpoint dir: $ckpt"

archive="$tmp/cosift-snapshot.tar.gz"
log "tarring $ckpt + $CONFIG → $archive"
tar -C "$(dirname "$ckpt")" -czf "$archive" \
    "$(basename "$ckpt")" \
    -C "$(dirname "$CONFIG")" "$(basename "$CONFIG")"
size=$(stat -c%s "$archive")
log "$((size / 1024 / 1024)) MiB"

dest="$BUCKET/snapshots/$stamp/cosift-snapshot.tar.gz"
log "uploading to $dest"
gcloud storage cp "$archive" "$dest"

log "pruning to last $KEEP"
mapfile -t all < <(gcloud storage ls "$BUCKET/snapshots/" 2>/dev/null | sort)
count=${#all[@]}
if (( count > KEEP )); then
  to_delete=$((count - KEEP))
  for path in "${all[@]:0:$to_delete}"; do
    log "deleting old $path"
    gcloud storage rm --recursive "$path"
  done
fi
log "done"
