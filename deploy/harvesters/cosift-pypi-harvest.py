#!/usr/bin/env python3
"""cosift-pypi-harvest — index the most-used PyPI packages' project pages.

Pre-indexing for baseline coverage: the top-N packages by download count (from
hugovk's well-known list) cover the common Python ecosystem so /search and
/research can answer "what does package X do / how to install it". The long tail
of niche packages is handled live by /find. pypi.org is allowlisted; we enqueue
project pages and the crawler indexes the description + README. Stdlib only.
"""
import json, os, urllib.request, time

ADDR = "http://127.0.0.1:7777"
TOP = int(os.environ.get("COSIFT_PYPI_TOP", "1000"))
LIST = "https://hugovk.github.io/top-pypi-packages/top-pypi-packages.min.json"

def token():
    try: return json.load(open("/home/ubuntu/cosift.json")).get("cluster",{}).get("peer_auth_token","")
    except Exception: return ""
TOKEN = token()

def post(path, payload):
    req = urllib.request.Request(ADDR+path, data=json.dumps(payload).encode(),
        method="POST", headers={"Content-Type":"application/json"})
    if TOKEN: req.add_header("Authorization","Bearer "+TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=30) as r: return r.status
    except Exception as e: return str(e)

def main():
    req = urllib.request.Request(LIST, headers={"User-Agent":"cosift-pypi/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        rows = json.loads(r.read()).get("rows", [])
    n = 0
    for row in rows[:TOP]:
        name = row.get("project")
        if not name: continue
        post("/admin/crawl-enqueue", {"url": f"https://pypi.org/project/{name}/"})
        n += 1
    print(f"pypi-harvest: enqueued {n} package pages (top {TOP}) at {time.strftime('%FT%TZ', time.gmtime())}")

if __name__ == "__main__":
    main()
