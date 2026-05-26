# Cosift Infrastructure Inventory

Living document — what's where, who can reach it, how to restore.

Last updated: 2026-05-26 (iter 394)

## Production-shaped (running)

### GH200 box (Lambda Labs)

| | |
|---|---|
| Host | `192.222.56.72` |
| User | `ubuntu` |
| SSH key | `/Users/calinteodor/Downloads/cosift.pem` (chmod 600 required) |
| Region | (Lambda US, unknown specific zone) |
| GPU | NVIDIA GH200 480GB (96 GB HBM3, 480 GB LPDDR5X unified) |
| RAM | 525 GB |
| Disk | 3.9 TB |
| Arch | aarch64 (linux/arm64 binary required) |
| Open ports | 22 (SSH), 443 (TLS) — bind any inside-the-box service to 0.0.0.0 to expose |

**Running services:**
- `ollama serve` (systemd, port 11434, localhost only)
  - `nomic-embed-text:latest` (137M params, 768d, 274 MB on disk, ~1.2 GB GPU)
  - `qwen2.5:7b-instruct` (7.6B params, 4.7 GB on disk, ~7 GB GPU)
- `cosift pebble-serve -dir ~/cosift-data/pebble -addr 127.0.0.1:7777` (manual, started under nohup, NOT systemd yet)
  - Logs: `~/pebble-serve.log`
  - PID file: none — use `pgrep -f "cosift.*pebble-serve"`

**Files on box:**
- `~/cosift` (linux/arm64 binary; deploy with `scp` from local cross-compile)
- `~/cosift.json` (config — has embed + chat URLs pointing at local Ollama, no API keys)
- `~/cosift-data/` (~30 MB) — sqlite + pebble subdirs
  - `~/cosift-data/pebble/` (~30 MB) — Pebble LSM with corpus + HNSW
- `~/pebble-serve.log`

**Current corpus:** ~275 indexed docs, 6155 HNSW passage vectors (dim=768) crawled from `https://go.dev/ref/spec`.

### Local dev box (laptop)

| | |
|---|---|
| Repo | `/Users/calinteodor/Development/pilot-protocol/cosift` |
| Git remote | `github.com/pilot-protocol/cosift` (private) |
| Default branch | `main` |
| Build | `make check` (build + vet + cmd/index/store unit tests, ~30s, no network) |
| Cross-compile to GH200 | `GOOS=linux GOARCH=arm64 go build -o /tmp/cosift-linux-arm64 ./cmd/cosift` |

### GCS — index snapshots

| | |
|---|---|
| Bucket | `gs://pilot-cosift-index` |
| Project | `vulture-vision-cloud` |
| Region | `us` (multi) |
| Storage class | Standard |
| Access | uniform bucket-level (IAM-only, no per-object ACLs) |
| Path layout | `snapshots/<UTC-timestamp>/cosift-snapshot.tar.gz` |
| First snapshot | `2026-05-26T17-16-37Z` (24.4 MiB; covers the 275-doc go.dev corpus + HNSW + config) |

**Restore on a fresh box:**

```bash
# 1. Pull binary (cross-compile or release artifact)
scp cosift-linux-arm64 ubuntu@<host>:~/cosift && ssh ubuntu@<host> 'chmod +x ~/cosift'

# 2. Restore data
ssh ubuntu@<host> '
  gcloud storage cp gs://pilot-cosift-index/snapshots/<stamp>/cosift-snapshot.tar.gz /tmp/ &&
  tar -xzf /tmp/cosift-snapshot.tar.gz -C ~/ &&
  ls ~/cosift-data/pebble
'

# 3. Start
ssh ubuntu@<host> '
  export COSIFT_LOAD_HNSW=true
  nohup ~/cosift -config ~/cosift.json pebble-serve -dir ~/cosift-data/pebble -addr 127.0.0.1:7777 > ~/pebble-serve.log 2>&1 &
'
```

## Snapshot policy

Currently manual. Suggested cadence once we have a real crawl loop:

- After every meaningful corpus growth (e.g., new seed batch finishes)
- Daily during active dev
- Weekly otherwise

To take one now: see `scripts/snapshot.sh` (TODO — not yet written).

## Access whitelist

Operator IPs that may bypass rate limits / hit admin endpoints:

- `104.28.216.88` (current operator IP as of 2026-05-26)

Configured via `COSIFT_RATELIMIT_WHITELIST=<csv>` env var on `pebble-serve` (iter 394).

## Next steps (deferred)

- Systemd unit for `pebble-serve` so it survives reboots.
- TLS terminator on port 443 → 7777 (caddy / nginx) so curl from outside works.
- Snapshot cron (daily) → GCS.
- vLLM swap-in for Ollama if `/answer` concurrency becomes a bottleneck.
- Multi-box deployment story (read replicas served from the same GCS snapshot).
