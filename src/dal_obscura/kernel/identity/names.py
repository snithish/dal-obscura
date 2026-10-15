"""Canonical identity-key encoding shared by storage migration and auth."""

from __future__ import annotations


def encode_federated_identity(issuer: str, value: str) -> str:
    """Preserves an exact issuer and gives subject identities a type tag."""

    return f"{_escape_component(issuer)}|u|{_escape_component(value)}"


def encode_federated_group(issuer: str, group: str) -> str:
    """Encodes a federated group principal with a distinct type tag."""

    return f"{_escape_component(issuer)}|g|{_escape_component(group)}"


def _escape_component(value: str) -> str:
    return value.replace("%", "%25").replace("|", "%7C")
