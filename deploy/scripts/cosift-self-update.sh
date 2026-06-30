#!/usr/bin/env bash
# cosift-self-update.sh — PULL-based self-updater for the cosift production box.
#
# Polls the latest GitHub Release for a newer cosift binary, verifies it
# (sha256 + minisign signature against a public key baked on the box),
# snapshots the index to GCS, atomically swaps the binary, restarts the
# service, and HEALTH-GATES the new process with automatic rollback.
#
# CI never holds the SSH key: the box pulls, CI only pushes signed releases.
#
# Run by cosift-self-update.timer every ~5 min. Logs to journald.
#
# Exit codes:
#   0  no update needed, or update applied and healthy
#   1  update attempted but failed (rolled back where possible)
#   2  precondition / environment error (no binary, no pubkey, missing deps)
#
# Configurable via environment (defaults target the GH200 prod layout):
#   COSIFT_REPO          pilot-protocol/cosift
#   COSIFT_BIN           /home/ubuntu/cosift
#   COSIFT_SERVICE       cosift-serve
#   COSIFT_PUBKEY        /etc/cosift/minisign.pub
#   COSIFT_HEALTH_URL    http://127.0.0.1:7777/healthz
#   COSIFT_ASSET         cosift-linux-arm64
#   COSIFT_SNAPSHOT      /home/ubuntu/scripts/snapshot.sh   (best-effort)
#   COSIFT_HEALTH_TIMEOUT_S  600   (10 min; HNSW load is ~4-5 min)
#   COSIFT_HEALTH_INTERVAL_S 10
#   GITHUB_TOKEN         (optional; raises the GitHub API rate limit)
set -euo pipefail

REPO="${COSIFT_REPO:-pilot-protocol/cosift}"
BIN="${COSIFT_BIN:-/home/ubuntu/cosift}"
SERVICE="${COSIFT_SERVICE:-cosift-serve}"
PUBKEY="${COSIFT_PUBKEY:-/etc/cosift/minisign.pub}"
HEALTH_URL="${COSIFT_HEALTH_URL:-http://127.0.0.1:7777/healthz}"
ASSET="${COSIFT_ASSET:-cosift-linux-arm64}"
SNAPSHOT="${COSIFT_SNAPSHOT:-/home/ubuntu/scripts/snapshot.sh}"
HEALTH_TIMEOUT_S="${COSIFT_HEALTH_TIMEOUT_S:-600}"
HEALTH_INTERVAL_S="${COSIFT_HEALTH_INTERVAL_S:-10}"
API="https://api.github.com/repos/${REPO}/releases/latest"

log() { echo "self-update: $*"; }
err() { echo "self-update: ERROR: $*" >&2; }

# --- Concurrency lock: never let two runs race the binary swap. ----------
# flock on a dedicated FD; -n means fail fast if another run holds it.
LOCKFILE="/run/cosift-self-update.lock"
exec 9>"$LOCKFILE" 2>/dev/null || exec 9>"/tmp/cosift-self-update.lock"
if ! flock -n 9; then
  log "another self-update run holds the lock; skipping this tick"
  exit 0
fi

# --- Preconditions -------------------------------------------------------
for dep in curl jq sha256sum minisign systemctl; do
  if ! command -v "$dep" >/dev/null 2>&1; then
    err "missing required dependency: $dep"
    exit 2
  fi
done
if [[ ! -x "$BIN" ]]; then
  err "current binary not found or not executable: $BIN"
  exit 2
fi
if [[ ! -f "$PUBKEY" ]]; then
  err "minisign public key not found: $PUBKEY (bake it on the box first)"
  exit 2
fi

# --- Determine running vs. latest version --------------------------------
running="$("$BIN" version 2>/dev/null | tr -d '[:space:]' || true)"
if [[ -z "$running" ]]; then
  err "could not read running version via '$BIN version'; aborting (fail-safe)"
  exit 2
fi
log "running version: $running"

auth_args=()
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  auth_args=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
fi

release_json="$(curl -fsSL "${auth_args[@]}" \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "$API" 2>/dev/null || true)"
if [[ -z "$release_json" ]]; then
  err "failed to query latest release from $API"
  exit 1
fi

latest_tag="$(printf '%s' "$release_json" | jq -r '.tag_name // empty')"
if [[ -z "$latest_tag" ]]; then
  err "latest release has no tag_name (response truncated below)"
  printf '%s\n' "$release_json" | head -c 400 >&2
  exit 1
fi
log "latest release tag: $latest_tag"

# Compare. We strip a leading 'v' so a binary stamped '1.2.3' matches tag
# 'v1.2.3'. If they already match, nothing to do.
norm() { printf '%s' "${1#v}"; }
if [[ "$(norm "$running")" == "$(norm "$latest_tag")" ]]; then
  log "already at latest ($running); nothing to do"
  exit 0
fi
log "update available: $running -> $latest_tag"

# --- Resolve asset download URLs ----------------------------------------
asset_url() {
  printf '%s' "$release_json" \
    | jq -r --arg n "$1" '.assets[] | select(.name == $n) | .browser_download_url' \
    | head -n1
}
bin_url="$(asset_url "$ASSET")"
sha_url="$(asset_url "${ASSET}.sha256")"
sig_url="$(asset_url "${ASSET}.minisig")"
if [[ -z "$bin_url" || -z "$sha_url" || -z "$sig_url" ]]; then
  err "release $latest_tag missing one of: $ASSET / .sha256 / .minisig"
  exit 1
fi

# --- Download into a private temp dir -----------------------------------
tmp="$(mktemp -d)"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT

log "downloading $ASSET, .sha256, .minisig"
curl -fsSL "${auth_args[@]}" -o "$tmp/$ASSET"          "$bin_url"
curl -fsSL "${auth_args[@]}" -o "$tmp/${ASSET}.sha256" "$sha_url"
curl -fsSL "${auth_args[@]}" -o "$tmp/${ASSET}.minisig" "$sig_url"

# --- VERIFY sha256 -------------------------------------------------------
# The .sha256 file is `sha256sum` output: "<hex>  <name>". Recompute and
# compare against the hex column rather than trusting the filename column.
expected_sha="$(awk '{print $1}' "$tmp/${ASSET}.sha256")"
actual_sha="$(sha256sum "$tmp/$ASSET" | awk '{print $1}')"
if [[ -z "$expected_sha" || "$expected_sha" != "$actual_sha" ]]; then
  err "sha256 mismatch (expected=$expected_sha actual=$actual_sha) — refusing"
  exit 1
fi
log "sha256 OK ($actual_sha)"

# --- VERIFY minisign signature against the box-baked public key ----------
# This is the trust anchor: only releases signed by the matching secret key
# (held as a GitHub secret in CI) are accepted. A good sha256 alone is not
# enough — an attacker who can serve assets could forge a matching checksum.
if ! minisign -V -p "$PUBKEY" -m "$tmp/$ASSET" -x "$tmp/${ASSET}.minisig"; then
  err "minisign signature verification FAILED — refusing to install"
  exit 1
fi
log "minisign signature OK (verified against $PUBKEY)"

# Sanity: the downloaded binary should report the version we expect.
chmod +x "$tmp/$ASSET"
dl_version="$("$tmp/$ASSET" version 2>/dev/null | tr -d '[:space:]' || true)"
if [[ -n "$dl_version" && "$(norm "$dl_version")" != "$(norm "$latest_tag")" ]]; then
  err "downloaded binary reports '$dl_version' but release tag is '$latest_tag' — refusing"
  exit 1
fi

# --- Pre-restart GCS snapshot (best-effort but logged) -------------------
if [[ -x "$SNAPSHOT" ]]; then
  log "taking pre-restart snapshot via $SNAPSHOT"
  if ! "$SNAPSHOT"; then
    err "snapshot failed; continuing with update (index is also recoverable from prior snapshot)"
  fi
else
  log "snapshot script not executable at $SNAPSHOT; skipping pre-restart snapshot"
fi

# --- Atomic swap ---------------------------------------------------------
# Stage alongside the live binary so the final mv is a same-filesystem
# rename (atomic). Keep the previous binary for rollback.
new="${BIN}.new"
prev="${BIN}.prev"
cp -f "$tmp/$ASSET" "$new"
chmod +x "$new"
log "staged new binary at $new"

cp -f "$BIN" "$prev"        # snapshot current binary for rollback
mv -f "$new" "$BIN"         # atomic rename over the live path
log "swapped in $latest_tag (previous kept at $prev)"

# --- Restart -------------------------------------------------------------
log "restarting $SERVICE"
systemctl restart "$SERVICE"

# --- HEALTH GATE ---------------------------------------------------------
# The listener only binds AFTER the ~4-5 min synchronous HNSW load, so a
# 200 from /healthz is a true readiness signal. Poll up to the timeout.
log "health-gating $HEALTH_URL (timeout ${HEALTH_TIMEOUT_S}s, every ${HEALTH_INTERVAL_S}s)"
deadline=$(( $(date +%s) + HEALTH_TIMEOUT_S ))
healthy=0
while (( $(date +%s) < deadline )); do
  if curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep "$HEALTH_INTERVAL_S"
done

if (( healthy == 1 )); then
  log "healthy on $latest_tag — update complete"
  exit 0
fi

# --- AUTO-ROLLBACK -------------------------------------------------------
err "new version $latest_tag did not become healthy within ${HEALTH_TIMEOUT_S}s — rolling back to $running"
if [[ -f "$prev" ]]; then
  mv -f "$prev" "$BIN"
  systemctl restart "$SERVICE"
  # Give the rolled-back process a chance to come back so the box isn't
  # left dark. Best-effort; we still exit non-zero to flag the failure.
  rb_deadline=$(( $(date +%s) + HEALTH_TIMEOUT_S ))
  while (( $(date +%s) < rb_deadline )); do
    if curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null 2>&1; then
      err "rolled back to $running and healthy again"
      exit 1
    fi
    sleep "$HEALTH_INTERVAL_S"
  done
  err "rollback restarted $SERVICE but it is not yet healthy — manual attention needed"
else
  err "no previous binary at $prev to roll back to — manual attention needed"
fi
exit 1
