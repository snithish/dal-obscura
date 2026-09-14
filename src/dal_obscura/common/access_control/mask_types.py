"""Canonical mask vocabulary shared by policy validation and clients."""

from __future__ import annotations

SUPPORTED_MASK_TYPES = (
    "null",
    "redact",
    "hash",
    "email",
    "keep_last",
    "default",
)
