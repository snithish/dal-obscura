from __future__ import annotations

from pathlib import Path


def test_capacity_runbook_and_runner_encode_a21_measurement_contract() -> None:
    runbook = Path("evaluation/capacity/README.md").read_text()
    runner = Path("scripts/run_capacity_benchmarks.sh")
    script = runner.read_text()

    for requirement in (
        "4-vCPU/8-GiB",
        "10,000 schema nodes",
        "10,000-entry discovery traversal",
        "10-million-row",
        "15% regression",
        "60-minute mixed workload",
        "16 consumers",
        "RSS growth must be at most 10%",
        "implemented-unverified",
    ):
        assert requirement in runbook

    assert runner.stat().st_mode & 0o111
    for suite in (
        "tests/benchmarks/test_masking_row_filter_benchmarks.py",
        "tests/benchmarks/test_iceberg_multifile_benchmark.py",
        "tests/benchmarks/test_ticket_to_response_benchmark.py",
    ):
        assert suite in script
    assert "--benchmark-json" in script
    assert "refusing to overwrite existing capacity evidence" in script
    assert "uv_lock_sha256" in script

