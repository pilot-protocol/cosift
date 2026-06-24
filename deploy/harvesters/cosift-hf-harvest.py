#!/usr/bin/env python3
"""cosift-hf-harvest — index HuggingFace via the public Hub API (free, OSS).

Enqueues the most-liked models, datasets, and Spaces (popularity = quality, vs
sort=lastModified which is auto-uploaded checkpoint spam), plus recent blog
posts. huggingface.co is already in the allowlist, so no domain promotion is
needed — just enqueue. Stdlib only.

URL shapes: model <id> -> /<id>; dataset -> /datasets/<id>; space -> /spaces/<id>.
"""
import json, os, sys, time, urllib.request

ADDR = "http://127.0.0.1:7777"
HF = "https://huggingface.co"
LIMIT = int(os.environ.get("COSIFT_HF_LIMIT", "100"))
SORT = os.environ.get("COSIFT_HF_SORT", "likes")

def token():
    try:
        return json.load(open("/home/ubuntu/cosift.json")).get("cluster", {}).get("peer_auth_token", "")
    except Exception:
        return ""
TOKEN = token()

def get_json(url, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent": "cosift-hf/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

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

def harvest(kind, path_prefix):
    """kind: api endpoint (models/datasets/spaces); path_prefix: URL prefix."""
    try:
        items = get_json(f"{HF}/api/{kind}?sort={SORT}&direction=-1&limit={LIMIT}")
    except Exception as e:
        print(f"hf-harvest: {kind} failed: {e}", file=sys.stderr); return 0
    n = 0
    for it in items:
        i = it.get("id")
        if not i:
            continue
        post("/admin/crawl-enqueue", {"url": f"{HF}{path_prefix}{i}"}); n += 1
    print(f"hf-harvest: {kind} -> enqueued {n} (sort={SORT})")
    return n

def main():
    total = 0
    total += harvest("models", "/")
    total += harvest("datasets", "/datasets/")
    total += harvest("spaces", "/spaces/")
    # Blog index (posts are also covered by the blog RSS feed).
    post("/admin/crawl-enqueue", {"url": f"{HF}/blog"})
    total += 1
    print(f"hf-harvest: total enqueued={total} at {time.strftime('%FT%TZ', time.gmtime())}")

if __name__ == "__main__":
    main()
