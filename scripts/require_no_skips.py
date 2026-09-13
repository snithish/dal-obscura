#!/usr/bin/env python3
"""Run a pytest command and fail when the resulting JUnit report has skips."""

from __future__ import annotations

import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def main(argv: list[str]) -> int:
    try:
        report_index = argv.index("--junitxml")
        separator = argv.index("--", report_index + 1)
        report_path = Path(argv[report_index + 1])
    except (ValueError, IndexError):
        print("usage: require_no_skips.py --junitxml REPORT -- COMMAND [ARGS...]", file=sys.stderr)
        return 2

    command = argv[separator + 1 :]
    if not command:
        print("the wrapped command must not be empty", file=sys.stderr)
        return 2
    if report_path.exists():
        report_path.unlink()

    completed = subprocess.run([*command, f"--junitxml={report_path}"])
    if completed.returncode != 0:
        return completed.returncode
    if not report_path.is_file():
        print(f"pytest did not write the JUnit report: {report_path}", file=sys.stderr)
        return 1

    try:
        root = ET.parse(report_path).getroot()
    except ET.ParseError as error:
        print(f"could not parse pytest JUnit report {report_path}: {error}", file=sys.stderr)
        return 1
    skipped = root.findall(".//skipped")
    if skipped:
        print(f"mandatory pytest lane skipped {len(skipped)} test(s)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
