#!/usr/bin/env python3
"""cosift-reddit-harvest — index Reddit via per-subreddit RSS/Atom (free, OSS).

Reddit's .json API serves HTML to datacenter IPs, but the .rss (Atom) endpoint
works. Per subreddit we enqueue the comment page (reddit.com, allowlisted) and
the post's external link, with the SAME organic domain-promotion as HN: an
external domain recurring >= THRESHOLD times is promoted via /admin/allow-domain.

Stdlib only. Evaluation notes (logged): per-feed item count + HTTP status, so we
can see reddit's reliability/rate-limiting vs HN over time.
"""
import json, os, re, sys, time, urllib.request, urllib.parse
from collections import Counter

ADDR = "http://127.0.0.1:7777"
SUBS_FILE = "/home/ubuntu/cosift-subreddits.txt"
STATE = "/home/ubuntu/cosift-reddit-domains.json"
THRESHOLD = int(os.environ.get("COSIFT_REDDIT_PROMOTE_THRESHOLD", "3"))
SPACING = float(os.environ.get("COSIFT_REDDIT_SPACING", "2"))

def token():
    try:
        return json.load(open("/home/ubuntu/cosift.json")).get("cluster", {}).get("peer_auth_token", "")
    except Exception:
        return ""
TOKEN = token()

def get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "cosift-reddit/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace"), r.status
    except Exception as e:
        return "", str(e)

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
        return (urllib.parse.urlparse(u).hostname or "").lower().lstrip(".")
    except Exception:
        return ""

def etld1(h):
    p = h.split(".")
    return ".".join(p[-2:]) if len(p) >= 2 else h

def main():
    if not os.path.exists(SUBS_FILE):
        print(f"reddit-harvest: {SUBS_FILE} missing", file=sys.stderr); return
    subs = [l.strip() for l in open(SUBS_FILE) if l.strip() and not l.startswith("#")]

    state = {}
    if os.path.exists(STATE):
        try: state = json.load(open(STATE))
        except Exception: state = {}
    counts = Counter({k: v for k, v in state.items() if not k.startswith("_")})
    promoted = set(state.get("_promoted", []))

    comment_pages, ext_urls = [], []
    for sub in subs:
        url = f"https://www.reddit.com/r/{sub}/.rss"
        body, status = get(url)
        # Atom entries: comment-page link + external [link] href in content.
        cpages = re.findall(r'<link href="(https://www\.reddit\.com/r/[^"]+)"', body)
        exts = re.findall(r'href="([^"]+)">\[link\]</a>', body)
        comment_pages.extend(cpages)
        ext_urls.extend(e for e in exts if e.startswith("http"))
        print(f"reddit-harvest: r/{sub} -> {len(cpages)} threads, {len(exts)} ext (http={status})")
        time.sleep(SPACING)  # reddit politeness

    # Count + promote external domains.
    newly = []
    for u in ext_urls:
        d = etld1(host_of(u))
        if d:
            counts[d] += 1
    for d, n in counts.items():
        if n >= THRESHOLD and d not in promoted:
            if str(post("/admin/allow-domain", {"domain": d})) == "200":
                promoted.add(d); newly.append(d)
    print(f"reddit-harvest: promoted {len(newly)} new domains: {newly[:20]}")

    # Enqueue comment pages (allowlisted) + external urls from promoted domains.
    nq = ne = 0
    for u in set(comment_pages):
        post("/admin/crawl-enqueue", {"url": u}); nq += 1
    for u in set(ext_urls):
        if etld1(host_of(u)) in promoted:
            post("/admin/crawl-enqueue", {"url": u}); ne += 1
    print(f"reddit-harvest: enqueued {nq} threads + {ne} external links")

    out = dict(counts); out["_promoted"] = sorted(promoted)
    json.dump(out, open(STATE, "w"))
    print(f"reddit-harvest: done at {time.strftime('%FT%TZ', time.gmtime())}")

if __name__ == "__main__":
    main()
