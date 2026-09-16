#!/usr/bin/env bash
# Consistent backup of account data, credit ledger and pending contributions.
set -euo pipefail
source_db="${COSIFT_COMMUNITY_DB:-/home/ubuntu/community-data/community.db}"
bucket="${COSIFT_GCS_BUCKET:-gs://pilot-cosift-index}"
[[ -f "$source_db" ]] || exit 0
umask 077
backup_dir="$(mktemp -d)"
trap 'rm -rf "$backup_dir"' EXIT
python3 - "$source_db" "$backup_dir/community.db" <<'PY'
import sqlite3,sys
with sqlite3.connect('file:'+sys.argv[1]+'?mode=ro',uri=True) as source:
    with sqlite3.connect(sys.argv[2]) as destination:
        source.backup(destination)
PY
gcloud storage cp "$backup_dir/community.db" "$bucket/community/$(date -u +%Y-%m-%dT%H-%M-%SZ)/community.db"
