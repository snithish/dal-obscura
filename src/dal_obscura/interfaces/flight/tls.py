"""Bounded TLS material loading for the Flight transport."""

from __future__ import annotations

from pathlib import Path

import pyarrow.flight as flight


def _tls_certificates(cert: str | None, key: str | None) -> list[flight.CertKeyPair] | None:
    if cert is None and key is None:
        return None
    if cert is None or key is None:
        raise ValueError("DAL_OBSCURA_TLS_CERT and DAL_OBSCURA_TLS_KEY must be set together")
    return [flight.CertKeyPair(_tls_material(cert, "certificate"), _tls_material(key, "key"))]


def _tls_root_certificates(client_ca: str | None) -> bytes | None:
    if client_ca is None:
        return None
    return _tls_material(client_ca, "client CA")


def _tls_material(value: str, label: str) -> bytes:
    """Loads a PEM file path or inline PEM material with a bounded size."""

    path = Path(value)
    material = path.read_bytes() if path.is_file() else value.encode("utf-8")
    if not material or len(material) > 1024 * 1024:
        raise ValueError(f"TLS {label} material must be between 1 byte and 1 MiB")
    return material
