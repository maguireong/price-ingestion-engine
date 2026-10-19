#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="$project_dir/.build/load-run"

rm -rf "$build_dir"
mkdir -p "$build_dir/main"

find "$project_dir/src/main/java" -name '*.java' -print0 \
  | xargs -0 javac --release 17 -d "$build_dir/main"

if [[ -n "${PRICE_HARNESS_JFR_FILE:-}" ]]; then
  jfr_settings="${PRICE_HARNESS_JFR_SETTINGS:-profile}"
  mkdir -p "$(dirname "$PRICE_HARNESS_JFR_FILE")"
  java \
    "-XX:StartFlightRecording=filename=$PRICE_HARNESS_JFR_FILE,settings=$jfr_settings,dumponexit=true" \
    -cp "$build_dir/main" \
    prices.load.PriceIngestionLoadHarness "$@"
else
  java -cp "$build_dir/main" \
    prices.load.PriceIngestionLoadHarness "$@"
fi
