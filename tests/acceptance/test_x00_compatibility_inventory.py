"""Executable X00 inventory checks for immutable compatibility constraints."""

from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
from typing import Any

from dal_obscura.common.ticket_delivery.models import (
    TicketPayload,
    canonical_ticket_payload_bytes,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load_manifest() -> dict[str, Any]:
    return json.loads((FIXTURE_DIR / "ticket_compat_manifest.json").read_text())


def _resolve_symbol(spec: str) -> object:
    module_name, symbol_path = spec.split(":", 1)
    value: object = importlib.import_module(module_name)
    for component in symbol_path.split("."):
        value = getattr(value, component)
    return value


def test_x00_manifest_binds_preserved_pickle_symbols_and_synthetic_fixtures() -> None:
    manifest = _load_manifest()

    assert manifest["manifest_version"] == 1
    assert manifest["baseline_commit"] == "5208eee9af35e38b5ab8294524a0485611c2b0a7"
    assert manifest["synthetic_only"] is True
    boundary = manifest["pickle_boundary"]
    assert boundary["status"] == "preserved"
    assert boundary["compatibility_rule"].startswith("Do not change serializer")

    for symbol in [*boundary["serializer_symbols"], *boundary["referenced_types"]]:
        assert _resolve_symbol(symbol) is not None

    fixtures = {fixture["id"]: fixture for fixture in manifest["fixtures"]}
    assert fixtures["ticket-json-v1"]["mutable"] is False
    assert fixtures["ticket-pickle-boundary-v1"]["mutable"] is False
    assert (FIXTURE_DIR / "ticket_payload_v1.json").exists()


def test_x00_canonical_ticket_fixture_has_stable_bytes_and_digest() -> None:
    raw = json.loads((FIXTURE_DIR / "ticket_payload_v1.json").read_text())
    payload = TicketPayload.from_dict(raw)

    canonical = canonical_ticket_payload_bytes(payload)
    round_trip = TicketPayload.from_dict(json.loads(json.dumps(raw)))
    assert canonical == canonical_ticket_payload_bytes(round_trip)
    assert hashlib.sha256(canonical).hexdigest() == (
        "c2511c569e0e35fe0fa6db06af7e786ba0049e5cd8407b67116b33032dbab605"
    )
