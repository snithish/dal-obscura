"""Keep wheel admission metadata and independent package ownership qualified."""

import ast
import json
from pathlib import Path

import dal_obscura_delta
from dal_obscura_delta.catalog import CATALOG_DESCRIPTOR
from dal_obscura_delta.format import FORMAT_DESCRIPTOR


def test_static_descriptors_match_runtime_contract_without_importing_service():
    package = Path(dal_obscura_delta.__file__).parent
    static = json.loads((package / "dal_obscura-plugin.json").read_text())["descriptors"]
    expected = [
        {k: v for k, v in descriptor.to_json().items() if k not in {"distribution", "version"}}
        for descriptor in (CATALOG_DESCRIPTOR, FORMAT_DESCRIPTOR)
    ]
    assert static == expected
    imports = set()
    for path in package.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)
    assert not any(name == "dal_obscura" or name.startswith("dal_obscura.") for name in imports)
