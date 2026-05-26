# Cosift Infrastructure Inventory

Living document — what's where, who can reach it, how to restore.

Last updated: 2026-05-26 (iter 426)

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
- `cosift pebble-serve` (`cosift-serve.service` systemd unit) — in-process continuous crawler + HTTP server on 127.0.0.1:7777
  - Logs: `journalctl -u cosift-serve -f`
  - Caddy reverse-proxies 0.0.0.0:443 → 127.0.0.1:7777 with `lb_try_duration=90s` for restart resilience.
- `cosift-snapshot.timer` (every 4 h) → `cosift-snapshot.service` → `~/snapshot.sh` → GCS upload, prune to KEEP=14.

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
| Cadence | every 4h (00,04,08,12,16,20 UTC) via `cosift-snapshot.timer` |
| Retention | last 14 snapshots (~56 h of recovery history) |
| Service account | `cosift-snapshot@vulture-vision-cloud.iam.gserviceaccount.com` (storage.objectAdmin on this bucket only) |
| Key file on GH200 | `~/.gcp-snapshot-key.json` (chmod 600) |

**Restore on a fresh box:**

The archive contains a Pebble checkpoint dir named `cosift-ckpt-<nanos>` (not `cosift-data/pebble`) plus `cosift.json`. Rename the checkpoint dir to whatever path your serve flag points at.

```bash
# 1. Pull binary (cross-compile or release artifact)
scp cosift-linux-arm64 ubuntu@<host>:~/cosift && ssh ubuntu@<host> 'chmod +x ~/cosift'

# 2. Restore data
ssh ubuntu@<host> '
  STAMP=$(gcloud storage ls gs://pilot-cosift-index/snapshots/ | sort | tail -1)
  gcloud storage cp ${STAMP}cosift-snapshot.tar.gz /tmp/ &&
  mkdir -p ~/cosift-data &&
  tar -xzf /tmp/cosift-snapshot.tar.gz -C /tmp/ &&
  mv /tmp/cosift-ckpt-* ~/cosift-data/pebble &&
  mv /tmp/cosift.json ~/cosift.json
'

# 3. Start
ssh ubuntu@<host> '
  export COSIFT_LOAD_HNSW=true
  nohup ~/cosift -config ~/cosift.json pebble-serve -dir ~/cosift-data/pebble -addr 127.0.0.1:7777 > ~/pebble-serve.log 2>&1 &
'
```

## Snapshot policy (iter 426)

Automated via systemd timer:
- `scripts/snapshot.sh` — calls `POST /admin/checkpoint` (consistent hard-linked dir, no compactor race), tars it, uploads to GCS, prunes to KEEP=14.
- `scripts/cosift-snapshot.service` — systemd oneshot running `snapshot.sh` as `ubuntu`.
- `scripts/cosift-snapshot.timer` — `OnCalendar=*-*-* 00,04,08,12,16,20:00:00`, randomized delay 60s.

Manual snapshot now: `bash ~/snapshot.sh` (uses same env as the timer).

Tune via env on the service: `COSIFT_KEEP` (history depth), `COSIFT_GCS_BUCKET`, `COSIFT_ADMIN_URL`, `COSIFT_ADMIN_TOKEN`.

## Access whitelist

Operator IPs that may bypass rate limits / hit admin endpoints:

- `104.28.216.88` (current operator IP as of 2026-05-26)

Configured via `COSIFT_RATELIMIT_WHITELIST=<csv>` env var on `pebble-serve` (iter 394).

## Next steps (deferred)

- vLLM swap-in for Ollama if `/answer` concurrency becomes a bottleneck.
- Multi-box deployment story (read replicas served from the same GCS snapshot).
- Cloudflare orange→gray (or install CF Origin Cert) for cosift.pilotprotocol.network to use Let's Encrypt end-to-end.
- Zombie HNSW slot compaction (~601 K legacy slots from pre-iter-411 partial persists).
