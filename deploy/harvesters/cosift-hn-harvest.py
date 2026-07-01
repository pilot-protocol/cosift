#!/usr/bin/env python3
"""cosift-hn-harvest — index Hacker News via the official Firebase API.

Enqueues each story's HN discussion page (news.ycombinator.com, allowlisted)
AND its external link. External links live on arbitrary domains, so we grow the
crawl allowlist ORGANICALLY: a link-target domain that recurs on HN >= THRESHOLD
times is promoted via /admin/allow-domain, then its URLs are enqueued. One-off
junk domains never cross the threshold and stay filtered.

Stdlib only (urllib + json) — no pip deps. Frequency state persists across runs.
"""
import json, os, sys, time, urllib.request, urllib.parse
from collections import Counter

ADDR = "http://127.0.0.1:7777"
HN = "https://hacker-news.firebaseio.com/v0"
STATE = "/home/ubuntu/cosift-hn-domains.json"   # {domain: count, "_promoted":[...]}
THRESHOLD = int(os.environ.get("COSIFT_HN_PROMOTE_THRESHOLD", "3"))
PER_LIST = int(os.environ.get("COSIFT_HN_PER_LIST", "60"))
LISTS = ["topstories", "beststories", "newstories", "askstories", "showstories"]

def token():
    try:
        return json.load(open("/home/ubuntu/cosift.json")).get("cluster", {}).get("peer_auth_token", "")
    except Exception:
        return ""
TOKEN = token()

def get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "cosift-hn/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def post(path, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(ADDR + path, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    if TOKEN:
        req.add_header("Authorization", "Bearer " + TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status
    except Exception as e:
        return str(e)

def host_of(u):
    try:
        h = urllib.parse.urlparse(u).hostname or ""
        return h.lower().lstrip(".")
    except Exception:
        return ""

def etld1(h):
    # crude eTLD+1 (good enough for promotion granularity)
    parts = h.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else h

def main():
    state = {}
    if os.path.exists(STATE):
        try: state = json.load(open(STATE))
        except Exception: state = {}
    counts = Counter({k: v for k, v in state.items() if not k.startswith("_")})
    promoted = set(state.get("_promoted", []))

    # 1. Collect story ids across lists.
    ids = []
    seen = set()
    for lst in LISTS:
        try:
            arr = json.loads(get(f"{HN}/{lst}.json"))
        except Exception as e:
            print(f"hn-harvest: {lst} failed: {e}", file=sys.stderr); continue
        for i in (arr or [])[:PER_LIST]:
            if i not in seen:
                seen.add(i); ids.append(i)
    print(f"hn-harvest: {len(ids)} unique stories")

    hn_pages, ext_urls = [], []
    for i in ids:
        try:
            it = json.loads(get(f"{HN}/item/{i}.json"))
        except Exception:
            continue
        if not it:
            continue
        hn_pages.append(f"https://news.ycombinator.com/item?id={i}")
        u = it.get("url")
        if u and u.startswith("http"):
            ext_urls.append(u)

    # 2. Count external-link domains; promote those crossing THRESHOLD.
    newly_promoted = []
    for u in ext_urls:
        d = etld1(host_of(u))
        if d:
            counts[d] += 1
    for d, n in counts.items():
        if n >= THRESHOLD and d not in promoted:
            r = post("/admin/allow-domain", {"domain": d})
            if r in (200, "200") or str(r) == "200":
                promoted.add(d); newly_promoted.append(d)
    print(f"hn-harvest: promoted {len(newly_promoted)} new domains: {newly_promoted[:20]}")

    # 3. Enqueue HN pages (always) + external urls whose domain is promoted.
    nq = 0
    for u in hn_pages:
        post("/admin/crawl-enqueue", {"url": u}); nq += 1
    ne = 0
    for u in ext_urls:
        if etld1(host_of(u)) in promoted:
            post("/admin/crawl-enqueue", {"url": u}); ne += 1
    print(f"hn-harvest: enqueued {nq} HN pages + {ne} external links")

    # 4. Persist counts + promotions.
    out = dict(counts); out["_promoted"] = sorted(promoted)
    json.dump(out, open(STATE, "w"))
    print(f"hn-harvest: done at {time.strftime('%FT%TZ', time.gmtime())} "
          f"({len(promoted)} domains promoted total)")

if __name__ == "__main__":
    main()
