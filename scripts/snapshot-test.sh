#!/usr/bin/env bash
# Exercises scripts/snapshot.sh's tar handling with stubbed curl/gcloud/pigz:
# rc 1 (file changed as we read it) is tolerated, rc 2 aborts before upload.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
snapshot="$here/snapshot.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

bin="$work/bin"
mkdir -p "$bin"
cat > "$bin/curl" <<'STUB'
#!/usr/bin/env bash
printf '{"path":"%s"}' "$SNAPTEST_CKPT"
STUB
cat > "$bin/gcloud" <<'STUB'
#!/usr/bin/env bash
echo "$*" >> "$SNAPTEST_GCLOUD_LOG"
STUB
cat > "$bin/pigz" <<'STUB'
#!/usr/bin/env bash
sleep "${SNAPTEST_PIGZ_DELAY:-0}"
exec gzip -c
STUB
chmod +x "$bin"/*
if ! command -v python3 >/dev/null 2>&1; then
  cat > "$bin/python3" <<'STUB'
#!/usr/bin/env bash
sed -n 's/.*"path": *"\([^"]*\)".*/\1/p'
STUB
  chmod +x "$bin/python3"
fi
export PATH="$bin:$PATH"

fail() { printf "FAIL: %b\n" "$*" >&2; exit 1; }

make_fixture() {
  local ckpt="$work/$1"
  mkdir -p "$ckpt"
  head -c 4194304 /dev/zero > "$ckpt/000001.sst"
  echo '{"cluster":{"peer_auth_token":"t"}}' > "$work/cosift.json"
  echo "$ckpt"
}

run_snapshot() {
  local rc=0
  COSIFT_ADMIN_TOKEN=t COSIFT_GCS_BUCKET=gs://test COSIFT_KEEP=14 \
    bash "$snapshot" > "$work/out.log" 2>&1 || rc=$?
  return $rc
}

# tar rc 1: pigz stalls so tar blocks mid-file; the file is appended to meanwhile.
ckpt="$(make_fixture ckpt-changed)"
export SNAPTEST_CKPT="$ckpt" SNAPTEST_GCLOUD_LOG="$work/gcloud.log" SNAPTEST_PIGZ_DELAY=1
: > "$SNAPTEST_GCLOUD_LOG"
export COSIFT_CONFIG="$work/cosift.json"
( sleep 0.3; echo changed >> "$ckpt/000001.sst" ) &
rc=0; run_snapshot || rc=$?
wait
(( rc == 0 )) || fail "changed-file run exited $rc (want 0):\n$(cat "$work/out.log")"
grep -q 'tar reported changed files (rc 1)' "$work/out.log" || fail "rc 1 was not observed; the mutation did not race tar:\n$(cat "$work/out.log")"
grep -q '^storage cp ' "$SNAPTEST_GCLOUD_LOG" || fail "upload did not run after rc 1"
[[ ! -d "$ckpt" ]] || fail "checkpoint dir not cleaned up"
echo "ok: tar rc 1 tolerated"

# tar rc 2: config file missing -> "Cannot stat" -> abort before upload.
ckpt="$(make_fixture ckpt-fatal)"
export SNAPTEST_CKPT="$ckpt" SNAPTEST_PIGZ_DELAY=0
: > "$SNAPTEST_GCLOUD_LOG"
export COSIFT_CONFIG="$work/missing.json"
rc=0; run_snapshot || rc=$?
(( rc == 2 )) || fail "missing-config run exited $rc (want 2):\n$(cat "$work/out.log")"
grep -q 'snapshot: tar failed (rc 2)' "$work/out.log" || fail "rc 2 message missing:\n$(cat "$work/out.log")"
[[ ! -s "$SNAPTEST_GCLOUD_LOG" ]] || fail "upload ran despite tar rc 2"
echo "ok: tar rc 2 aborts"
