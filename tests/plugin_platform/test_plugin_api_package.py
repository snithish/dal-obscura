from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PACKAGE_DIR = Path(__file__).parents[2] / "packages" / "plugin-api"


def test_plugin_api_package_is_self_contained_and_compilable() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "src"],
        cwd=PACKAGE_DIR,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    pyproject = (PACKAGE_DIR / "pyproject.toml").read_text()
    assert 'name = "dal-obscura-plugin-api"' in pyproject
    assert 'package-dir = {"" = "src"}' in pyproject
    assert (PACKAGE_DIR / "src/dal_obscura_plugin_api/contracts.py").exists()
