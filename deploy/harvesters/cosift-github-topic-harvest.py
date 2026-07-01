#!/usr/bin/env python3
"""cosift-github-topic-harvest — index top-starred GitHub repos per topic.

READMEs are the canonical install/usage docs for tools/libraries. Harvesting by
topic warms the corpus with high-quality, actively-maintained repos. github.com
is allowlisted. The GitHub search API is unauthenticated here (≈10 req/min), so
we space topic queries. Stdlib only.
"""
import json, os, urllib.request, urllib.parse, time

ADDR = "http://127.0.0.1:7777"
PER = int(os.environ.get("COSIFT_GH_TOPIC_LIMIT", "30"))
SPACING = float(os.environ.get("COSIFT_GH_SPACING", "7"))  # unauth search ~10/min
TOPICS = os.environ.get("COSIFT_GH_TOPICS",
    "machine-learning deep-learning computer-vision nlp llm ocr object-detection "
    "speech-recognition reinforcement-learning robotics jetson embedded "
    "kubernetes devops observability rust golang database "
    "web-framework security cryptography").split()

def token():
    try: return json.load(open("/home/ubuntu/cosift.json")).get("cluster",{}).get("peer_auth_token","")
    except Exception: return ""
TOKEN = token()

def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent":"cosift-gh-topic/1.0",
        "Accept":"application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=30) as r: return json.loads(r.read())

def post(path, payload):
    req = urllib.request.Request(ADDR+path, data=json.dumps(payload).encode(),
        method="POST", headers={"Content-Type":"application/json"})
    if TOKEN: req.add_header("Authorization","Bearer "+TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=30) as r: return r.status
    except Exception as e: return str(e)

def main():
    total = 0
    for topic in TOPICS:
        url = ("https://api.github.com/search/repositories?q=topic:"
               + urllib.parse.quote(topic) + f"&sort=stars&order=desc&per_page={PER}")
        try:
            res = get_json(url)
        except Exception as e:
            print(f"gh-topic-harvest: {topic} failed: {e}"); time.sleep(SPACING); continue
        items = res.get("items", [])
        n = 0
        for it in items:
            u = it.get("html_url")
            if u: post("/admin/crawl-enqueue", {"url": u}); n += 1
        print(f"gh-topic-harvest: {topic} -> enqueued {n}")
        total += n
        time.sleep(SPACING)
    print(f"gh-topic-harvest: total {total} at {time.strftime('%FT%TZ', time.gmtime())}")

if __name__ == "__main__":
    main()
