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

## Diagnosing memory growth

Set `COSIFT_PPROF_ADDR=127.0.0.1:6060` to expose runtime pprof endpoints
on loopback only (iter 427). Then on the box:

```bash
curl -fsS http://127.0.0.1:6060/debug/pprof/heap -o heap.pb.gz
# Pull to laptop and analyze:
scp ubuntu@<host>:heap.pb.gz . && go tool pprof -top -nodecount=15 cosift heap.pb.gz
```

The iter-427 OOM was traced to `ledongthuc/pdf.(*buffer).readArray` —
99% of 117 GB heap. PDF parsing is now gated behind `COSIFT_CRAWL_PDF=true`
(default off). Re-enable once we migrate to a safer PDF library.

## Crawl vs search latency tradeoff (iter 439)

`max_concurrent` directly trades crawl throughput for search latency
because HNSW.AddPassage and HNSW.Search share a single sync.RWMutex.
At 22 vec/sec of writes, the read lock waits often.

Observed on the live 817K-vector graph:

| max_concurrent | Crawl rate | Search /search?q=... |
|---|---|---|
| 256 (current) | ~1300 docs/min | 1.5-3 s |
| ~128 (estimate)  | ~800 docs/min  | ~300-500 ms |
| ~32 (default) | ~200 docs/min  | ~100-150 ms |

To switch: edit `cosift.json` field `crawler.max_concurrent`, restart
cosift. The Caddy active health probe on /healthz absorbs the brief
downtime as fast 502s (not 522).

The bench-pq internal latency (1.8 ms) reflects HNSW alone with no
contention; everything above that comes from the ollama embed call
(~50 ms when warm) and the contention envelope.

## Crawler tuning (iter 432-433)

Live `~/cosift.json` on the GH200, aggressive-frontier configuration:

```json
{
  "crawler": {
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "max_concurrent": 256,
    "per_host_delay_ms": 300,
    "respect_robots": false,
    "max_urls_per_host": 0
  }
}
```

`max_urls_per_host=0` disables the iter-195 spider-trap cap entirely
(reddit/wikipedia can have arbitrary depth). `respect_robots=false`
ignores robots.txt — `per_host_delay_ms=300` is still in effect so
hosts get ≤ ~3 req/s. UA mimics Chrome to bypass simple bot walls
(Cloudflare still fingerprints TLS so the heavy CF-protected sites
return 403/429 anyway; that would need a real headless browser).

## Recall (iter 428 + 429)

Live `bench-pq` after hnsw-rebuild on the production graph:

| Path | Recall@10 mean | p50 | p95 | Latency (avg) |
|---|---|---|---|---|
| Brute force (ground truth) | 1.000 | 1.0 | 1.0 | ~15 ms |
| HNSW raw vectors (PQ off) | 0.888 | 1.0 | 1.0 | 416 µs |
| HNSW + PQ M=96 K=256 | 0.598 | 0.7 | 1.0 | 456 µs |

Production now runs with `COSIFT_DISABLE_PQ=true` — the PQ path costs ~30%
recall relative to raw and the speedup over raw HNSW is negligible
(456µs vs 416µs). PQ is worth revisiting at much larger scale where the
graph stops fitting in RAM, but at 80K vectors it's a recall regression.

### Iter 438: efSearch tuning under graph erosion

As the post-iter-428 graph grew via AddPassage from 47 K → 817 K vectors,
self-recall@10 dropped 0.89 → 0.40 at default efSearch=50.
`COSIFT_HNSW_EF_SEARCH=200` raises this back to 0.55 with ~1.8 ms search
latency (still 100× faster than brute force). Recall plateaus past 200.

Full recovery would need a re-rebuild. Worth doing every ~Nx growth in
vector count, e.g. weekly.

**Iter 440 cost note**: a force-rebuild on the 924K-vec graph took
24m 46s (AddPassage cost is O(log N) per insert; the total scales with
N log N, plus dim work per neighbor evaluation). Crawler work during
the rebuild window is lost when the rebuilt checkpoint is swapped in —
operator gives up roughly `rebuild_minutes × crawl_rate` of docs. At
~1300/min that's ~30 K. The lost URLs are re-crawled later, so the
loss is temporary.

Workflow:
```
ssh ubuntu@<host>
ckpt=$(curl -fsS -X POST http://127.0.0.1:7777/admin/checkpoint | jq -r .path)
~/cosift hnsw-rebuild -dir "$ckpt" -force
sudo systemctl stop cosift-serve
mv ~/cosift-data/pebble ~/cosift-data/pebble.old
mv "$ckpt" ~/cosift-data/pebble
sudo systemctl start cosift-serve
# Optional: retrain PQ after rebuild
curl -fsS -X POST -H 'Content-Type: application/json' -d '{}' http://127.0.0.1:7777/admin/pq-train
```

| efSearch | Recall@10 mean | hnsw latency |
|---|---|---|
| 50  | 0.404 | 589 µs |
| 100 | 0.490 | 995 µs |
| 200 | 0.548 | 1.8 ms |
| 400 | 0.570 | 3.1 ms |

To take a fresh recall measurement against the live serve:

```bash
ssh ubuntu@192.222.56.72
ckpt=$(curl -fsS -X POST http://127.0.0.1:7777/admin/checkpoint | jq -r .path)
COSIFT_LOAD_HNSW=true ~/cosift -config ~/cosift.json bench-pq -dir "$ckpt" -n 100 -k 10 -seed 7 -no-pq
rm -rf "$ckpt"
```

## Next steps (deferred)

- Investigate why only ~50% of indexed docs have HNSW vectors (was 47K vectors / 110K docs at iter 428). Probably embed failures on certain content types.
- PQ quality investigation: smaller M (e.g. 48) or different distance metric (symmetric vs asymmetric). Don't re-enable until recall ≥ raw - 5%.
- Migrate off `ledongthuc/pdf` so PDFs can be re-enabled.
- Multi-box deployment story (read replicas served from the same GCS snapshot).
- Cloudflare orange→gray (or install CF Origin Cert) for cosift.pilotprotocol.network to use Let's Encrypt end-to-end.
- vLLM swap-in for Ollama if `/answer` concurrency becomes a bottleneck.
