from __future__ import annotations

from dataclasses import replace

from dal_obscura.application.reads.tickets.payload import ticket_payload_hash
from tests.support.read.tickets import ticket_payload


def test_ticket_payload_hash_changes_when_executable_scan_payload_changes():
    first = ticket_payload()
    second = ticket_payload(
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": "payload-b",
            "full_row_filter": None,
            "masks": {},
        }
    )

    assert ticket_payload_hash(first) != ticket_payload_hash(second)


def test_ticket_payload_hash_changes_when_authorization_context_changes():
    assert ticket_payload_hash(ticket_payload(principal_id="user1")) != ticket_payload_hash(
        ticket_payload(principal_id="user2")
    )
    assert ticket_payload_hash(ticket_payload()) != ticket_payload_hash(
        replace(ticket_payload(), identity_context="changed-groups-or-attributes")
    )
    assert ticket_payload_hash(ticket_payload(policy_version=100)) != ticket_payload_hash(
        ticket_payload(policy_version=101)
    )
