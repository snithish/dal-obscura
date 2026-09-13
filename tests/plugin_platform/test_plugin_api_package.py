from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

from dal_obscura.common.plugin_api import (
    PLUGIN_API_VERSION,
    SUPPORTED_PLUGIN_API_VERSIONS,
    SUPPORTED_PLUGIN_CONFIG_VERSIONS,
)

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
    compatibility = PACKAGE_DIR.parents[1] / "docs/plugin-platform/PLUGIN_API_COMPATIBILITY.md"
    assert "PLUGIN_API_VERSION" in compatibility.read_text()


def test_plugin_api_import_boundary_excludes_service_and_provider_modules() -> None:
    modules: set[str] = set()
    for path in (PACKAGE_DIR / "src/dal_obscura_plugin_api").glob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                modules.add(node.module)

    forbidden_prefixes = (
        "dal_obscura",
        "fastapi",
        "sqlalchemy",
        "pyiceberg",
    )
    assert not {
        module
        for module in modules
        if not module.startswith("dal_obscura_plugin_api")
        and module.startswith(forbidden_prefixes)
    }


def test_core_and_public_version_policy_is_explicit_and_aligned() -> None:
    from dal_obscura_plugin_api import (
        PLUGIN_API_VERSION as public_api_version,
    )
    from dal_obscura_plugin_api import (
        SUPPORTED_PLUGIN_API_VERSIONS as public_api_versions,
    )
    from dal_obscura_plugin_api import (
        SUPPORTED_PLUGIN_CONFIG_VERSIONS as public_config_versions,
    )

    assert PLUGIN_API_VERSION == public_api_version == "1"
    assert SUPPORTED_PLUGIN_API_VERSIONS == public_api_versions == frozenset({"1"})
    assert SUPPORTED_PLUGIN_CONFIG_VERSIONS == public_config_versions == frozenset({1})
