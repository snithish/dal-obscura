from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_WRAPPER = Path("scripts/require_no_skips.py")


def _run_wrapper(tmp_path: Path, report: str) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        "-c",
        "import pathlib, sys; pathlib.Path(sys.argv[-1].split('=', 1)[1]).write_text(sys.argv[1])",
        report,
    ]
    return subprocess.run(
        [sys.executable, str(_WRAPPER), "--junitxml", str(tmp_path / "junit.xml"), "--", *command],
        check=False,
        capture_output=True,
        text=True,
    )


def test_mandatory_wrapper_rejects_skipped_junit_cases(tmp_path: Path) -> None:
    report = '<testsuite><testcase name="skipped"><skipped /></testcase></testsuite>'
    result = _run_wrapper(tmp_path, report)

    assert result.returncode == 1
    assert "skipped 1 test" in result.stderr


def test_mandatory_wrapper_accepts_complete_junit_cases(tmp_path: Path) -> None:
    report = '<testsuite><testcase name="passed" /></testsuite>'
    result = _run_wrapper(tmp_path, report)

    assert result.returncode == 0
