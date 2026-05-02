from __future__ import annotations


def allow_rule(
    columns: list[str],
    *,
    principals: list[str] | None = None,
    masks: dict[str, object] | None = None,
    row_filter: str | None = None,
) -> dict[str, object]:
    rule: dict[str, object] = {
        "principals": principals or ["user1"],
        "columns": columns,
        "effect": "allow",
    }
    if masks:
        rule["masks"] = masks
    if row_filter:
        rule["row_filter"] = row_filter
    return rule
