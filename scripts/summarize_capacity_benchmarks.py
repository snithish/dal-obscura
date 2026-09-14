#!/usr/bin/env python3
"""Summarize pytest-benchmark evidence without running another benchmark.

The capacity harness intentionally keeps every raw per-run JSON file. This
small reader produces a stable, reviewable aggregate and fails closed when a
run is malformed or a suite is missing a benchmark result.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any

EXPECTED_SUITES = (
    "test_masking_row_filter_benchmarks",
    "test_iceberg_multifile_benchmark",
    "test_ticket_to_response_benchmark",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output = args.output
    metadata = _read_metadata(output / "metadata.txt")
    try:
        runs = int(metadata["runs"])
    except (KeyError, ValueError) as exc:
        raise SystemExit("capacity metadata is missing a valid runs value") from exc
    if runs < 1:
        raise SystemExit("capacity metadata runs must be positive")

    suites: dict[str, Any] = {}
    for suite in EXPECTED_SUITES:
        records = []
        for run in range(1, runs + 1):
            path = output / f"{suite}-{run}.json"
            if not path.is_file():
                raise SystemExit(f"missing benchmark evidence: {path}")
            records.append(_load_json(path))
        benchmark_names = [
            str(item.get("name"))
            for item in records[0].get("benchmarks", [])
            if isinstance(item, dict) and item.get("name")
        ]
        if not benchmark_names:
            raise SystemExit(f"suite has no benchmark results: {suite}")
        benchmarks: dict[str, Any] = {}
        for name in benchmark_names:
            means: list[float] = []
            medians: list[float] = []
            for record in records:
                result = next(
                    (
                        item
                        for item in record.get("benchmarks", [])
                        if isinstance(item, dict) and item.get("name") == name
                    ),
                    None,
                )
                if not isinstance(result, dict) or not isinstance(result.get("stats"), dict):
                    raise SystemExit(f"benchmark {name!r} is missing from {suite}")
                stats = result["stats"]
                mean = stats.get("mean")
                median = stats.get("median", mean)
                if not isinstance(mean, (int, float)) or not isinstance(median, (int, float)):
                    raise SystemExit(f"benchmark {name!r} has invalid timing data")
                means.append(float(mean))
                medians.append(float(median))
            benchmarks[name] = {
                "runs": len(means),
                "mean_ms": _round_ms(statistics.mean(means)),
                "median_ms": _round_ms(statistics.median(medians)),
                "min_ms": _round_ms(min(means)),
                "max_ms": _round_ms(max(means)),
            }
        suites[suite] = {"runs": len(records), "benchmarks": benchmarks}

    summary = {
        "git_commit": metadata.get("git_commit"),
        "git_dirty": metadata.get("git_dirty"),
        "runs": runs,
        "suites": suites,
    }
    destination = output / "summary.json"
    destination.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(f"capacity summary written: {destination}")
    return 0


def _read_metadata(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise SystemExit(f"missing capacity metadata: {path}")
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        if separator and key and value:
            values[key] = value
    return values


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"invalid benchmark evidence: {path}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"benchmark evidence must be an object: {path}")
    return value


def _round_ms(seconds: float) -> float:
    return round(seconds * 1_000, 3)


if __name__ == "__main__":
    raise SystemExit(main())
