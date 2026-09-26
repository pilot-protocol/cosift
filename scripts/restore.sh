#!/usr/bin/env bash
# Usage: restore.sh <gs://…/cosift-snapshot.tar.gz | file>   (env: COSIFT_DATA_DIR COSIFT_CONFIG COSIFT_SERVICE_AUTH COSIFT_RESTORE_PRINCIPALS COSIFT_RESTORE_EXTRA_PRINCIPAL COSIFT_BIN COSIFT_RESTORE_UNIT COSIFT_RESTORE_START)
set -euo pipefail

src="${1:?usage: restore.sh <gs://…/cosift-snapshot.tar.gz | file>}"
DATA_DIR="${COSIFT_DATA_DIR:-/home/ubuntu/cosift-data/pebble}"
CONFIG="${COSIFT_CONFIG:-/home/ubuntu/cosift.json}"
SERVICE_AUTH="${COSIFT_SERVICE_AUTH:-/etc/cosift/service-auth.json}"
KEEP="${COSIFT_RESTORE_PRINCIPALS:-dash-prod,dash-staging}"
EXTRA="${COSIFT_RESTORE_EXTRA_PRINCIPAL:-}"
BIN="${COSIFT_BIN:-/home/ubuntu/cosift}"
UNIT="${COSIFT_RESTORE_UNIT:-cosift-serve}"
START="${COSIFT_RESTORE_START:-systemctl start $UNIT}"

stamp="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
trap 'exit 143' TERM INT

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) restore: $*"; }
die() { echo "restore: $*" >&2; exit 1; }

if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet "$UNIT"; then
  die "$UNIT is running; stop it before restoring"
fi

archive="$tmp/cosift-snapshot.tar.gz"
case "$src" in
  gs://*)
    log "downloading $src"
    gcloud storage cp "$src" "$archive"
    ;;
  *)
    [[ -f "$src" ]] || die "no such file: $src"
    cp "$src" "$archive"
    ;;
esac

log "verifying $(stat -c%s "$archive") bytes"
gzip -t "$archive" || die "archive is corrupt"
ckpt=""
conf=""
while IFS= read -r m; do
  case "$m" in
    /* | .. | ../* | */../* | */..) die "unsafe member path: $m" ;;
  esac
  top="${m%%/*}"
  if [[ "$top" == cosift-ckpt-* ]]; then
    [[ -z "$ckpt" || "$ckpt" == "$top" ]] || die "more than one checkpoint in the archive"
    ckpt="$top"
  elif [[ "$m" == "$top" && "$m" == *.json ]]; then
    [[ -z "$conf" ]] || die "more than one config in the archive"
    conf="$m"
  else
    die "unexpected member: $m"
  fi
done < <(tar -tzf "$archive")
[[ -n "$ckpt" ]] || die "no checkpoint in the archive"

mkdir "$tmp/x"
tar -xzf "$archive" -C "$tmp/x" --no-same-owner --no-same-permissions
store="$tmp/x/$ckpt"
[[ -f "$store/CURRENT" ]] || die "checkpoint has no CURRENT"
manifest="$(head -n1 "$store/CURRENT")"
[[ "$manifest" == MANIFEST-* && -f "$store/$manifest" ]] || die "CURRENT names a missing manifest"
ns="${ckpt#cosift-ckpt-}"
if [[ "$ns" =~ ^[0-9]{19}$ ]]; then
  log "checkpoint time $(date -u -d "@$((ns / 1000000000))" +%Y-%m-%dT%H:%M:%SZ) — re-apply every moderation action and the go-live purge after it"
fi
if [[ -x "$BIN" ]]; then
  log "opening the checkpoint with $BIN pebble-info"
  "$BIN" pebble-info -json -dir "$store" > "$tmp/info.json" || die "the checkpoint does not open"
  python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print("restore: documents", d.get("documents"), "indexed_docs", d.get("indexed_docs"))' "$tmp/info.json"
fi

if [[ -f "$SERVICE_AUTH" ]]; then
  python3 - "$SERVICE_AUTH" "$KEEP" "$EXTRA" > "$tmp/service-auth.json" <<'PY' || die "cannot build the restricted service-auth.json"
import json, sys
path, keep, extra = sys.argv[1], set(filter(None, sys.argv[2].split(","))), sys.argv[3]
cfg = json.load(open(path))
kept = [p for p in cfg.get("principals", []) if p.get("id") in keep]
missing = keep - {p["id"] for p in kept}
if missing:
    sys.exit("restore: principals not in the current file: " + ", ".join(sorted(missing)))
if extra:
    kept.append(json.load(open(extra)))
cfg["principals"] = kept
print(json.dumps(cfg, indent=2))
PY
  chmod 0640 "$tmp/service-auth.json"
  if [[ -x "$BIN" ]]; then
    engine_conf="$CONFIG"
    [[ -f "$engine_conf" || -z "$conf" ]] || engine_conf="$tmp/x/$conf"
    if [[ -f "$engine_conf" ]]; then
      "$BIN" -config "$engine_conf" svc-auth check --file "$tmp/service-auth.json" || die "the restricted service-auth.json fails svc-auth check"
    else
      "$BIN" svc-auth check --no-engine-config --file "$tmp/service-auth.json" || die "the restricted service-auth.json fails svc-auth check"
    fi
  fi
fi

mkdir -p "$(dirname "$DATA_DIR")"
owner_ref="$(dirname "$DATA_DIR")"
if [[ -e "$DATA_DIR" ]]; then
  owner_ref="$DATA_DIR.pre-restore-$stamp"
  log "moving the current store aside to $owner_ref"
  mv "$DATA_DIR" "$owner_ref"
fi
mv "$store" "$DATA_DIR"
sync
if [[ "$(id -u)" == 0 ]]; then
  chown -R --reference="$owner_ref" "$DATA_DIR"
fi
log "store in place at $DATA_DIR"

if [[ -n "$conf" ]]; then
  if [[ -e "$CONFIG" ]]; then
    cp "$tmp/x/$conf" "$CONFIG.snapshot-$stamp"
    log "kept $CONFIG; the snapshot's copy is $CONFIG.snapshot-$stamp"
  else
    install -m 0640 "$tmp/x/$conf" "$CONFIG"
    log "installed the snapshot's config at $CONFIG"
  fi
fi

if [[ -f "$SERVICE_AUTH" ]]; then
  cp -p "$SERVICE_AUTH" "$SERVICE_AUTH.pre-restore-$stamp"
  if [[ "$(id -u)" == 0 ]]; then
    install -o root -g "$(stat -c %G "$SERVICE_AUTH")" -m 0640 "$tmp/service-auth.json" "$SERVICE_AUTH"
  else
    cat "$tmp/service-auth.json" > "$SERVICE_AUTH"
  fi
  log "service-auth.json now holds only: $(python3 -c 'import json,sys; print(",".join(p["id"] for p in json.load(open(sys.argv[1]))["principals"]))' "$SERVICE_AUTH"); the previous file is $SERVICE_AUTH.pre-restore-$stamp"
else
  log "no $SERVICE_AUTH: the /v1 listener stays off"
fi

if [[ "$START" == "-" ]]; then
  log "not starting (COSIFT_RESTORE_START=-)"
else
  log "starting: $START"
  $START
fi
log "done"
