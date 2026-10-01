from __future__ import annotations

import ast
from pathlib import Path

PACKAGE_DIR = Path(__file__).parents[2] / "packages" / "plugin-api"


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
        if not module.startswith("dal_obscura_plugin_api") and module.startswith(forbidden_prefixes)
    }
