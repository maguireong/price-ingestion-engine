#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for engine in baseline snapshot; do
  for distribution in hot-cold zipf; do
    echo
    echo "Expiry soak: engine=$engine distribution=$distribution"
    "$project_dir/scripts/load-test.sh" \
      --engine="$engine" \
      --ticker-distribution="$distribution" \
      --zipf-exponent=1.0 \
      --warmup-seconds=5 \
      --duration-seconds=310 \
      --target-quotes-per-second=5000 \
      --target-reads-per-second=5000 \
      --ticker-count=1000 \
      --producer-threads=2 \
      --reader-threads=2 \
      --sample-every=100 \
      "$@"
  done
done
