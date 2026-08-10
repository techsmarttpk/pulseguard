#!/usr/bin/env bash
# Controlled benchmark sweep: runs the simulator at each of a set of
# target rates in turn, warms up, samples the live Prometheus metrics,
# and captures docker stats CPU/memory — everything requested by
# "run controlled benchmarks at 100/500/1000/5000 msg/sec and report
# actual producer rate, consumer rate, lag, P50/P95/P99, CPU, memory".
#
# Requires: the full PulseGuard stack already up (`docker compose up -d`),
# the docker CLI, and Python 3 available on THIS machine (not inside a
# container) since it edits .env directly. Run from the repo root.
#
# Usage:
#   ./scripts/rate_sweep.sh                     # sweeps 100 500 1000 5000
#   ./scripts/rate_sweep.sh 100 1000 20000       # custom rate list
set -euo pipefail

if [ "$#" -gt 0 ]; then
  RATES=("$@")
else
  RATES=(100 500 1000 5000)
fi

OUT="benchmark_report_$(date +%Y%m%d_%H%M%S).txt"
echo "PulseGuard controlled rate-sweep benchmark — $(date)" | tee "$OUT"
echo "Rates: ${RATES[*]}" | tee -a "$OUT"
echo "======================================================================" | tee -a "$OUT"

for rate in "${RATES[@]}"; do
  echo "" | tee -a "$OUT"
  echo "### Target rate: $rate msg/sec ###" | tee -a "$OUT"
  python3 scripts/benchmark.py \
    --set-rate "$rate" \
    --warmup 20 \
    --duration 40 \
    --interval 5 \
    --docker-stats \
    2>&1 | tee -a "$OUT"
done

echo "" | tee -a "$OUT"
echo "Done. Full report saved to $OUT" | tee -a "$OUT"
echo "Restore your normal throughput afterward, e.g.:" | tee -a "$OUT"
echo "  python3 scripts/benchmark.py --set-rate 1000 --duration 5 --warmup 0" | tee -a "$OUT"
