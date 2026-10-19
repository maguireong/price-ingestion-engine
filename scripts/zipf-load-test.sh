#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

exec "$script_dir/load-test.sh" \
  --ticker-distribution=zipf \
  --zipf-exponent=1.0 \
  "$@"
