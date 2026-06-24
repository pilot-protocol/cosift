#!/usr/bin/env python3
"""cosift-hf-task-harvest — index top HF models per ML task/pipeline tag.

Complements cosift-hf-harvest.py (which does global top-by-likes and misses
niche-but-relevant models). Harvesting top-N PER TASK surfaces the best model
for each capability (object-detection, OCR/image-to-text, ASR, etc.) — e.g.
license-plate / DETR detectors that never crack the global likes leaderboard.
huggingface.co is allowlisted. Stdlib only.
"""
import json, os, urllib.request, urllib.parse, time

ADDR = "http://127.0.0.1:7777"
HF = "https://huggingface.co"
PER = int(os.environ.get("COSIFT_HF_TASK_LIMIT", "50"))
TASKS = os.environ.get("COSIFT_HF_TASKS",
    "object-detection image-segmentation image-classification image-to-text "
    "automatic-speech-recognition text-to-speech text-generation text-classification "
    "token-classification question-answering summarization translation "
    "sentence-similarity feature-extraction zero-shot-classification "
    "text-to-image image-to-image depth-estimation").split()

def token():
    try: return json.load(open("/home/ubuntu/cosift.json")).get("cluster",{}).get("peer_auth_token","")
    except Exception: return ""
TOKEN = token()

def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent":"cosift-hf-task/1.0"})
    with urllib.request.urlopen(req, timeout=40) as r: return json.loads(r.read())

def post(path, payload):
    req = urllib.request.Request(ADDR+path, data=json.dumps(payload).encode(),
        method="POST", headers={"Content-Type":"application/json"})
    if TOKEN: req.add_header("Authorization","Bearer "+TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=30) as r: return r.status
    except Exception as e: return str(e)

def main():
    total = 0
    for task in TASKS:
        url = f"{HF}/api/models?filter={urllib.parse.quote(task)}&sort=likes&direction=-1&limit={PER}"
        try: items = get_json(url)
        except Exception as e:
            print(f"hf-task-harvest: {task} failed: {e}"); continue
        n = 0
        for it in items:
            i = it.get("id")
            if i: post("/admin/crawl-enqueue", {"url": f"{HF}/{i}"}); n += 1
        print(f"hf-task-harvest: {task} -> enqueued {n}")
        total += n
        time.sleep(1)
    print(f"hf-task-harvest: total {total} at {time.strftime('%FT%TZ', time.gmtime())}")

if __name__ == "__main__":
    main()
