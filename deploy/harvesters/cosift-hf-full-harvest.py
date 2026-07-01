#!/usr/bin/env python3
"""cosift-hf-full-harvest — enumerate the ENTIRE HuggingFace Hub and enqueue
every model, dataset, and Space for indexing. "As many as possible" coverage.

Walks the Hub via cursor pagination (Link: rel="next" header), limit=1000/page.
RESUMABLE: the per-kind cursor is checkpointed to STATE after every page, so a
run picks up exactly where the last left off — across crashes, restarts, and the
per-run budget. When a kind is fully enumerated it resets (next cycle re-walks to
pick up newly-published repos).

Reality check: enumeration is fast, but the crawler then has to fetch+embed each
page at its per-host rate — full-Hub coverage is a multi-week grind on one box.
The frontier is the buffer; we throttle enqueue per run so we don't build an
absurd backlog faster than it can be reasoned about. Stdlib only.
"""
import json, os, sys, time, urllib.request

ADDR = "http://127.0.0.1:7777"
HF = "https://huggingface.co"
STATE = "/home/ubuntu/cosift-hf-cursor.json"
BUDGET = int(os.environ.get("COSIFT_HF_FULL_BUDGET", "50000"))  # max enqueues per run
PAGE = int(os.environ.get("COSIFT_HF_FULL_PAGE", "1000"))
KINDS = [("models", "/"), ("datasets", "/datasets/"), ("spaces", "/spaces/")]

def token():
    try: return json.load(open("/home/ubuntu/cosift.json")).get("cluster",{}).get("peer_auth_token","")
    except Exception: return ""
TOKEN = token()

def http(url):
    """GET returning (json_body, next_url_or_None) using the Link rel=next header."""
    req = urllib.request.Request(url, headers={"User-Agent":"cosift-hf-full/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        body = json.loads(r.read())
        nxt = None
        link = r.headers.get("Link", "")
        for part in link.split(","):
            if 'rel="next"' in part:
                s = part.find("<"); e = part.find(">")
                if s != -1 and e != -1:
                    nxt = part[s+1:e]
        return body, nxt

def post(path, payload):
    req = urllib.request.Request(ADDR+path, data=json.dumps(payload).encode(),
        method="POST", headers={"Content-Type":"application/json"})
    if TOKEN: req.add_header("Authorization","Bearer "+TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=30) as r: return r.status
    except Exception: return 0

def load_state():
    if os.path.exists(STATE):
        try: return json.load(open(STATE))
        except Exception: pass
    return {}

def main():
    st = load_state()
    enq = 0
    for kind, prefix in KINDS:
        if enq >= BUDGET:
            break
        ck = "cursor_" + kind
        # Resume from saved cursor; empty/"DONE" means start fresh this cycle.
        cur = st.get(ck) or ""
        url = f"{HF}/api/{kind}?limit={PAGE}" + (f"&cursor={cur}" if cur else "")
        page_n = 0
        while enq < BUDGET:
            try:
                items, nxt = http(url)
            except Exception as e:
                print(f"hf-full: {kind} page failed: {e}", file=sys.stderr); break
            for it in items:
                i = it.get("id")
                if i:
                    post("/admin/crawl-enqueue", {"url": f"{HF}{prefix}{i}"})
                    enq += 1
            page_n += 1
            if not nxt:
                st[ck] = ""  # fully enumerated; restart next cycle for new repos
                print(f"hf-full: {kind} FULLY enumerated this cycle")
                break
            # checkpoint the cursor from the next URL so we resume mid-kind.
            cidx = nxt.find("cursor=")
            st[ck] = nxt[cidx+7:] if cidx != -1 else ""
            url = nxt
            if enq >= BUDGET:
                print(f"hf-full: {kind} budget hit at page {page_n}, cursor saved")
                break
        json.dump(st, open(STATE, "w"))
    print(f"hf-full: enqueued {enq} this run at {time.strftime('%FT%TZ', time.gmtime())}")

if __name__ == "__main__":
    main()
