from __future__ import annotations

import re
from pathlib import Path

_ROW = re.compile(
    r"^\| `(?P<module>src/[^`]+)` \| .* \| .* \| "
    r"\*\*(?P<disposition>[^*]+)\*\* \| .+ \|$"
)
_ALLOWED = {
    "KEEP",
    "KEEP-COMPATIBILITY",
    "KEEP-PICKLE",
    "KEEP-PUBLIC-API",
    "KEEP-GENERATED",
}


def test_dead_code_inventory_is_source_backed_and_pickle_boundary_is_preserved() -> None:
    inventory = Path("docs/plugin-platform/DEAD_CODE_INVENTORY.md")
    text = inventory.read_text()
    rows = [_ROW.match(line) for line in text.splitlines()]

    parsed = [match.groupdict() for match in rows if match]
    assert parsed, "the inventory must contain at least one reviewed source module"
    assert all(item["disposition"] in _ALLOWED for item in parsed)
    assert all(Path(item["module"]).is_file() for item in parsed)

    iceberg = next(item for item in parsed if item["module"].endswith("table_formats/iceberg.py"))
    assert iceberg["disposition"] == "KEEP-PICKLE"
    assert "pickle" in text.lower()
    assert "No module currently has sufficient evidence for deletion." in text
