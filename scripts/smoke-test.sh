#!/usr/bin/env bash
# Real-runner smoke test for cosift. Run before PRs touching the crawler,
# store, or HTTP handlers. Catches WAL multi-writer races and similar
# bug classes that unit tests against OpenMemory can't reach.
#
# Cost: ~30-60s (one real crawl + one server start/stop).
# Network: requires reachable go.dev.
# Exit: 0 on success, non-zero on any check failure.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$(mktemp -d)/cosift"
DATA_DIR="$(mktemp -d)/cosift-data"
LOG="$(mktemp)"
SEED="${COSIFT_SMOKE_SEED:-https://go.dev/doc/effective_go}"
TIMEOUT="${COSIFT_SMOKE_TIMEOUT:-30}"
PORT="${COSIFT_SMOKE_PORT:-17777}"

cleanup() {
  local pid="${SERVER_PID:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$BIN" "$DATA_DIR" "$LOG" 2>/dev/null || true
}
trap cleanup EXIT

step() { printf "\033[36m▸ %s\033[0m\n" "$*"; }
ok()   { printf "\033[32m✓ %s\033[0m\n" "$*"; }
fail() { printf "\033[31m✗ %s\033[0m\n" "$*" >&2; exit 1; }

step "build cosift"
(cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/cosift)
ok "binary at $BIN"

step "write cosift.json + crawl $SEED (timeout ${TIMEOUT}s)"
cat > "$(dirname "$BIN")/cosift.json" <<EOF
{"data_dir": "$DATA_DIR",
 "server": {"addr": "127.0.0.1:$PORT", "admin_token": "smoketoken"},
 "crawler": {"user_agent": "CosiftSmokeTest/0.0", "max_concurrent": 8,
             "per_host_delay_ms": 200, "max_body_bytes": 5242880, "max_depth": 1,
             "respect_robots": true}}
EOF
cd "$(dirname "$BIN")"

step "verify check-robots subcommand on $SEED"
ROBOTS_OUT=$("$BIN" check-robots "$SEED" 2>&1 || true)
# check-robots prints one line per URL; status [OK ] / [DENY] / [ERR ].
# Smoke acceptance: returns a status line for our URL (any of the three;
# DENY would be a fine outcome for a site that blocks our UA).
# Status format: [OK  ] / [DENY] / [ERR ] — match the leading token without
# assuming column-padded width (avoids brittle whitespace count assertions).
if ! grep -qE '\[(OK|DENY|ERR)' <<<"$ROBOTS_OUT"; then
  fail "check-robots output missing status line: $ROBOTS_OUT"
fi
ok "check-robots returned status line"

step "crawl $SEED (timeout ${TIMEOUT}s)"
timeout "$TIMEOUT" "$BIN" crawl "$SEED" 2>&1 | tee "$LOG" >/dev/null || true

# WAL-contention regression check: zero "database is locked" lines.
LOCK_LINES=$(grep -c "database is locked" "$LOG" || true)
if [[ "$LOCK_LINES" -ne 0 ]]; then
  fail "found $LOCK_LINES 'database is locked' lines in crawler log"
fi
ok "no SQLite lock errors in crawl log"

step "verify stats has non-zero documents"
DOCS=$("$BIN" stats 2>&1 | awk '/^documents:/ {print $2}')
if [[ -z "$DOCS" || "$DOCS" -eq 0 ]]; then
  fail "expected documents > 0, got '$DOCS'"
fi
ok "stats shows $DOCS documents"

step "ingest a tmp corpus.json (3 docs)"
# Covers the offline-ingestion path (different code than crawl: no
# fetcher, no robots, no frontier). Uses smoke.invalid URLs so we can't
# collide with the crawled seed and can attribute the doc-count delta
# cleanly.
CORPUS="$(dirname "$BIN")/corpus.json"
cat > "$CORPUS" <<'EOF'
{"docs":[
  {"url":"https://smoke.invalid/a","title":"Smoke A","text":"alpha bravo charlie delta echo foxtrot golf hotel"},
  {"url":"https://smoke.invalid/b","title":"Smoke B","text":"india juliet kilo lima mike november oscar papa"},
  {"url":"https://smoke.invalid/c","title":"Smoke C","text":"quebec romeo sierra tango uniform victor whiskey xray"}
]}
EOF
"$BIN" ingest -corpus "$CORPUS" >/dev/null 2>&1 || fail "ingest -corpus exited non-zero"
DOCS2=$("$BIN" stats 2>&1 | awk '/^documents:/ {print $2}')
if [[ -z "$DOCS2" ]] || [[ "$DOCS2" -lt $((DOCS + 3)) ]]; then
  fail "expected docs to grow by >=3 after ingest; pre=$DOCS post=$DOCS2"
fi
ok "ingest added $((DOCS2 - DOCS)) docs (total now $DOCS2)"

step "start server in background"
"$BIN" serve >/dev/null 2>&1 &
SERVER_PID=$!
sleep 2

step "verify /healthz returns 200"
HEALTH=$(curl -sS -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT/healthz" || echo "fail")
if [[ "$HEALTH" != "200" ]]; then
  fail "/healthz: got $HEALTH want 200"
fi
ok "/healthz 200"

step "verify /search returns non-empty hits"
HITS=$(curl -sS "http://127.0.0.1:$PORT/search?q=goroutines&k=3" | grep -c '"url"' || true)
if [[ "$HITS" -eq 0 ]]; then
  fail "/search returned zero hits"
fi
ok "/search returned $HITS hits"

step "verify /contents returns the seed doc's full text"
# URL-encoded seed; jq would be cleaner but the smoke test avoids external deps.
ENC_SEED=$(printf '%s' "$SEED" | sed 's/:/%3A/g; s|/|%2F|g')
CONTENTS=$(curl -sS "http://127.0.0.1:$PORT/contents?url=$ENC_SEED")
if [[ -z "$CONTENTS" ]] || ! grep -q '"url"' <<<"$CONTENTS"; then
  fail "/contents returned empty or malformed JSON for $SEED"
fi
ok "/contents returned doc payload"

step "verify /admin/stats with bearer token"
STATS=$(curl -sS -H "Authorization: Bearer smoketoken" "http://127.0.0.1:$PORT/admin/stats")
# LLM-cache fields must be present in the response shape (value may be
# 0 for fresh deployments). Catches schema regressions silently.
for field in '"documents"' '"frontier"' '"paraphrases"' '"hyde_cache"'; do
  if ! grep -q "$field" <<<"$STATS"; then
    fail "/admin/stats missing $field — response: $STATS"
  fi
done
ok "/admin/stats schema has documents + frontier + paraphrases + hyde_cache"

step "verify /admin/stats rejects missing token"
UNAUTH=$(curl -sS -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT/admin/stats")
if [[ "$UNAUTH" != "401" ]]; then
  fail "/admin/stats without bearer: got $UNAUTH want 401"
fi
ok "/admin/stats requires bearer (401 without)"

step "all smoke checks passed"
