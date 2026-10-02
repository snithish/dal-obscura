"""Validate finite per-query budgets without opening a database connection."""

import re
from decimal import Decimal


def validate_memory_limit(value: str) -> str:
    """Require an explicit positive byte size rather than a host-relative limit."""
    match = (
        re.fullmatch(r"(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB|KIB|MIB|GIB|TIB)", value.strip(), re.I)
        if isinstance(value, str)
        else None
    )
    if match is None:
        raise ValueError("duckdb_memory_limit must be a positive finite size, such as 512MB")
    unit = match[2].upper()
    powers = {"B": 0, "KB": 1, "MB": 2, "GB": 3, "TB": 4, "KIB": 1, "MIB": 2, "GIB": 3, "TIB": 4}
    size = Decimal(match[1]) * (1024 if "I" in unit else 1000) ** powers[unit]
    if size < 1 or size > 2**63 - 1:
        raise ValueError("duckdb_memory_limit must be between 1 byte and 2^63-1 bytes")
    return value.strip()
