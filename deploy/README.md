# cosift deploy — content pipeline (GH200 production)

Operational artifacts for the running cosift backend (`cosift.pilotprotocol.network`,
binary `/home/ubuntu/cosift`, service `cosift-serve`). Captured from the server so the
content pipeline is reproducible from git, not only on the box.

## Layout
- `harvesters/` — scripts in `/usr/local/bin/` that feed the crawl frontier via the
  cosift admin API (`/admin/crawl-enqueue`, `/admin/rss-import`, `/admin/wet-import-bulk`,
  `/admin/site-submit`). All free/OSS, stdlib-only.
- `systemd/` — `.service` (oneshot) + `.timer` units that schedule the harvesters.
- `lists/` — source lists the harvesters read (`cosift-feeds.txt` RSS feeds,
  `cosift-subreddits.txt`, `cosift-sites.txt`).

## Sources & cadence
| Harvester | Source | Schedule |
|---|---|---|
| rss-refresh | `cosift-feeds.txt` (RSS/Atom incl. macrumors, tomshardware, arXiv cs.*) | 30 min |
| arxiv-harvest | arXiv export API (recent papers per cs.* category) | daily 05:30 |
| hf-harvest | HF Hub API top-likes models/datasets/spaces + blog | daily 06:30 |
| hf-full-harvest | full HF Hub enumeration (cursor-paginated, resumable) | every 3h |
| hn-harvest | HN Firebase API + external-link domain promotion | 4×/day |
| reddit-harvest | per-subreddit Atom RSS + link promotion (rate-limited by reddit) | 4×/day |
| wet-refresh | CommonCrawl WET (rotating offset, lexical-only) | daily 03:00 |
| pypi/hf-task/github-topic | popular PyPI / HF-by-task / GitHub-by-topic | weekly |
| sitemap-refresh | (disabled — net-negative on single box) | — |
| cosift-compact | `POST /admin/hnsw-compact` when `/stats` zombies ≥ 15% (async job, polls `hnsw_compact`) | weekly Sun 04:30 |

## Notes
- Curated ingester: `disable_link_following=true` + `include_domains` allowlist in
  `cosift.json` (on the server). `/admin/allow-domain` grows it at runtime from HN/Reddit
  link-domain frequency (persisted to `cosift-dynamic-domains.txt`).
- Env (systemd drop-ins on server): `COSIFT_HOST_PARTITION[_READ]=1`, `COSIFT_QUERY_LOG`,
  `COSIFT_DYNAMIC_DOMAINS_FILE`, WET/feedback tunables.
- To re-deploy: copy `harvesters/*` → `/usr/local/bin/`, `systemd/*` → `/etc/systemd/system/`,
  `lists/*` → `/home/ubuntu/`, then `systemctl daemon-reload && systemctl enable --now <timer>`.
