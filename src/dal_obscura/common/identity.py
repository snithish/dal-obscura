"""Canonical identity-key encoding shared by storage migration and auth."""

from __future__ import annotations


def encode_federated_identity(issuer: str, value: str) -> str:
    """Preserves an exact issuer and escapes identity delimiters."""

    return f"{_escape_component(issuer)}|{_escape_component(value)}"


def encode_federated_group(issuer: str, group: str) -> str:
    """Encodes a federated group principal without changing its marker."""

    return encode_federated_identity(issuer, f"group:{group}")


def _escape_component(value: str) -> str:
    return value.replace("%", "%25").replace("|", "%7C")
