#!/usr/bin/env bash
# Run the three services locally without Docker:
#   verifier on :8000, sample-a on :9101 (bucket layout A),
#   sample-b on :9102 (different bucket layout B)
set -euo pipefail
cd "$(dirname "$0")"

cleanup() {
  kill "${A_PID:-}" "${B_PID:-}" "${V_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT

INSTANCE_ID=sample-a SAMPLE_BUCKETS="0.05,0.1,0.25,0.5,1,2.5,5" \
  python3 -m uvicorn app.sample_target:app --host 127.0.0.1 --port 9101 &
A_PID=$!

INSTANCE_ID=sample-b SAMPLE_BUCKETS="0.1,0.3,0.7,1.5,3,6" \
  python3 -m uvicorn app.sample_target:app --host 127.0.0.1 --port 9102 &
B_PID=$!

SCRAPE_TARGETS="sample-a=http://127.0.0.1:9101/metrics,sample-b=http://127.0.0.1:9102/metrics" \
  python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000 &
V_PID=$!

echo "verifier:  http://127.0.0.1:8000/docs"
echo "sample-a:  http://127.0.0.1:9101/metrics"
echo "sample-b:  http://127.0.0.1:9102/metrics"
wait
