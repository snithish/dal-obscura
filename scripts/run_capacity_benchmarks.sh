#!/usr/bin/env sh
set -eu

# Reproducible X21 benchmark harness. It records metadata beside each JSON
# result and refuses to overwrite an existing evidence directory. This script
# measures the process-local workloads; the 60-minute multi-consumer run still
# requires the separately documented deployment procedure.

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [OUTPUT_DIRECTORY]" >&2
  exit 2
fi

output=${1:-.benchmarks/capacity-$(date -u +%Y%m%dT%H%M%SZ)}
runs=${DAL_OBSCURA_CAPACITY_RUNS:-5}
case "$runs" in
  ''|*[!0-9]*) echo "DAL_OBSCURA_CAPACITY_RUNS must be a positive integer" >&2; exit 2 ;;
esac
if [ "$runs" -lt 1 ]; then
  echo "DAL_OBSCURA_CAPACITY_RUNS must be at least 1" >&2
  exit 2
fi
if [ -e "$output" ]; then
  echo "refusing to overwrite existing capacity evidence: $output" >&2
  exit 2
fi

mkdir -p "$output"
umask 077
metadata="$output/metadata.txt"
{
  printf 'recorded_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'git_commit=%s\n' "$(git rev-parse HEAD)"
  printf 'git_dirty=%s\n' "$(test -z "$(git status --porcelain)" && echo false || echo true)"
  printf 'platform=%s\n' "$(uname -srvmp)"
  printf 'python=%s\n' "$(uv run python --version 2>&1)"
  printf 'uv_lock_sha256=%s\n' "$(sha256sum uv.lock | awk '{print $1}')"
  printf 'runs=%s\n' "$runs"
} > "$metadata"

run_suite() {
  suite=$1
  suite_name=$(basename "$suite" .py)
  for run in $(seq 1 "$runs"); do
    result="$output/${suite_name}-${run}.json"
    echo "running $suite (run $run/$runs)"
    uv run pytest "$suite" --benchmark-only --benchmark-json="$result"
  done
}

run_suite tests/benchmarks/test_masking_row_filter_benchmarks.py
run_suite tests/benchmarks/test_iceberg_multifile_benchmark.py
run_suite tests/benchmarks/test_ticket_to_response_benchmark.py

uv run python scripts/summarize_capacity_benchmarks.py "$output"

echo "capacity evidence written: $output"
