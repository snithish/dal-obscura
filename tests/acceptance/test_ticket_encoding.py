"""Ticket encoding is deterministic for the current contract."""

from __future__ import annotations

import json
from pathlib import Path

from dal_obscura.read.tickets import (
    TicketPayload,
    canonical_ticket_payload_bytes,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def test_ticket_encoding_is_canonical_and_round_trips() -> None:
    raw = json.loads((FIXTURE_DIR / "ticket_payload_v2.json").read_text())
    payload = TicketPayload.from_dict(raw)

    canonical = canonical_ticket_payload_bytes(payload)
    round_trip = TicketPayload.from_dict(json.loads(json.dumps(raw)))
    assert canonical == canonical_ticket_payload_bytes(round_trip)
    assert json.loads(canonical) == payload.to_dict()
    reordered = dict(reversed(list(raw.items())))
    assert canonical == canonical_ticket_payload_bytes(TicketPayload.from_dict(reordered))
