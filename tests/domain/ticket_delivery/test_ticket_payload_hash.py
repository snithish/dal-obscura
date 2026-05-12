from __future__ import annotations

from dal_obscura.common.ticket_delivery.models import (
    ScanPayload,
    TicketPayload,
    ticket_payload_hash,
)


def _payload(
    *,
    ticket_id: str = "00000000-0000-0000-0000-000000000001",
    catalog: str | None = "analytics",
    target: str = "default.users",
    tenant_id: str = "tenant-a",
    columns: list[str] | None = None,
    scan: ScanPayload | None = None,
    policy_version: int = 100,
    principal_id: str = "user1",
    expires_at: int = 9999999999,
    nonce: str = "nonce-a",
) -> TicketPayload:
    default_scan: ScanPayload = {
        "read_payload": "payload-a",
        "full_row_filter": None,
        "masks": {},
    }
    return TicketPayload(
        ticket_id=ticket_id,
        catalog=catalog,
        target=target,
        tenant_id=tenant_id,
        columns=columns or ["id"],
        scan=scan if scan is not None else default_scan,
        policy_version=policy_version,
        principal_id=principal_id,
        expires_at=expires_at,
        nonce=nonce,
    )


def test_ticket_payload_hash_is_stable_for_same_payload():
    assert ticket_payload_hash(_payload()) == ticket_payload_hash(_payload())


def test_ticket_payload_hash_changes_when_executable_scan_payload_changes():
    first = _payload()
    second = _payload(scan={"read_payload": "payload-b", "full_row_filter": None, "masks": {}})

    assert ticket_payload_hash(first) != ticket_payload_hash(second)


def test_ticket_payload_hash_changes_when_authorization_context_changes():
    assert ticket_payload_hash(_payload(principal_id="user1")) != ticket_payload_hash(
        _payload(principal_id="user2")
    )
    assert ticket_payload_hash(_payload(tenant_id="tenant-a")) != ticket_payload_hash(
        _payload(tenant_id="tenant-b")
    )
    assert ticket_payload_hash(_payload(policy_version=100)) != ticket_payload_hash(
        _payload(policy_version=101)
    )
