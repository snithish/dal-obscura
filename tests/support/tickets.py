"""Fresh ticket values with valid defaults; callers override behavior-relevant fields."""

from dataclasses import replace
from typing import Any

from dal_obscura.common.ticket_delivery.models import TicketPayload
from tests.support.use_cases import scan_payload


def ticket_payload(**changes: Any) -> TicketPayload:
    base = TicketPayload(
        asset_id="00000000-0000-4000-8000-000000000001",
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan=scan_payload(),
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
    )
    return replace(base, **changes)
