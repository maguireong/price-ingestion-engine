#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
profile_dir="$project_dir/.build/profiles"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
recording_file="${PRICE_HARNESS_JFR_FILE:-$profile_dir/price-load-$run_id.jfr}"
settings_file="$profile_dir/price-load-$run_id.jfc"

if ! command -v jfr >/dev/null 2>&1; then
  echo "The JDK 'jfr' command is required but was not found on PATH." >&2
  exit 1
fi

mkdir -p "$profile_dir" "$(dirname "$recording_file")"

jfr configure \
  --input profile.jfc \
  --output "$settings_file" \
  gc=detailed \
  allocation-profiling=high \
  method-profiling=high \
  locking-threshold=1us

echo "Java Flight Recorder profile: $recording_file"
PRICE_HARNESS_JFR_FILE="$recording_file" \
PRICE_HARNESS_JFR_SETTINGS="$settings_file" \
  "$project_dir/scripts/load-test.sh" "$@"

echo
java -cp "$project_dir/.build/load-run/main" \
  prices.load.JfrProfileSummary "$recording_file"

echo
echo "Recording: $recording_file"
echo "Raw event inventory: jfr summary \"$recording_file\""
