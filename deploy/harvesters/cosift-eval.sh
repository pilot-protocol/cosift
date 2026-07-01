#!/bin/bash
# Iter 500: daily eval smoke run. Hits /admin/eval-quick, appends one line
# per run to /var/log/cosift-eval.log with timestamp + JSON summary. Lets
# operators grep historical answer rates and spot quality regressions
# tied to corpus growth or code changes.
set -e
TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
RESULT=$(curl -s --max-time 300 http://127.0.0.1:7777/admin/eval-quick)
# Compact one-line JSON summary (drop the verbose per-query results for the log).
SUMMARY=$(echo "$RESULT" | python3 -c '
import json,sys
try:
    d=json.load(sys.stdin)
    out={k:d[k] for k in ["queries","answered","no_info","empty","errors","answer_rate_pct","avg_latency_ms","total_elapsed","chat_model"] if k in d}
    print(json.dumps(out))
except Exception as e:
    print("{\"error\":\"%s\"}" % e)
')
echo "${TS} ${SUMMARY}" | sudo tee -a /var/log/cosift-eval.log >/dev/null
echo "${TS} ${SUMMARY}"
