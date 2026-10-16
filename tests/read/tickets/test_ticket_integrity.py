"""Signed references, passive payload validation and authorization-bound integrity."""

from __future__ import annotations

import base64
import hmac
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest

from dal_obscura.adapters.tickets.signing import HmacTicketCodecAdapter
from dal_obscura.application.reads.tickets.payload import TicketPayload, ticket_payload_hash
from tests.support.read.fakes import ticket_payload


def _scan_payload():
    return {
        "authorization_columns": ["id", "region"],
        "read_payload": "payload",
        "full_row_filter": None,
        "masks": {},
    }


def _ticket_payload() -> TicketPayload:
    return ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        target="catalog.db.table",
        columns=["id"],
        scan=_scan_payload(),
        policy_version=1,
        expires_at=2**31,
        nonce="abc123",
    )


def test_signed_ticket_matches_architecture_baseline(monkeypatch):
    codec = HmacTicketCodecAdapter("secret")
    payload = _ticket_payload()
    baseline = (Path(__file__).parent / "fixtures" / "signed_reference.txt").read_text().strip()
    assert codec.sign_payload(payload) == baseline
    monkeypatch.setattr("dal_obscura.adapters.tickets.signing.time.time", lambda: 100)
    restored = codec.verify(baseline)
    assert (restored.ticket_id, restored.expires_at, restored.nonce) == (
        payload.ticket_id,
        payload.expires_at,
        payload.nonce,
    )


def test_ticket_verify_rejects_noncanonical_base64_even_with_valid_signature():
    codec = HmacTicketCodecAdapter("secret")
    payload = _ticket_payload()
    ticket = codec.sign_payload(payload)
    encoded, signature = ticket.split(".", 1)

    with pytest.raises(PermissionError, match="Invalid ticket payload"):
        codec.verify(f"{encoded[:4]}!{encoded[4:]}.{signature}")


def test_ticket_key_rotation_accepts_previous_keys_but_signs_with_current_key():
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        target="catalog.db.table",
        columns=["id"],
        scan=_scan_payload(),
        policy_version=1,
        expires_at=2**31,
        nonce="rotation",
    )
    old = HmacTicketCodecAdapter("old-secret")
    rotated = HmacTicketCodecAdapter("new-secret", previous_secrets=("old-secret",))

    old_ticket = old.sign_payload(payload)
    new_ticket = rotated.sign_payload(payload)

    assert rotated.verify(old_ticket).ticket_id == payload.ticket_id
    assert rotated.verify(new_ticket).ticket_id == payload.ticket_id
    with pytest.raises(PermissionError):
        old.verify(new_ticket)


def test_ticket_key_rotation_rejects_duplicate_keys():
    with pytest.raises(ValueError, match="unique"):
        HmacTicketCodecAdapter("same", previous_secrets=("same",))


def test_signed_ticket_is_opaque_and_does_not_embed_scan_payload():
    codec = HmacTicketCodecAdapter("secret")
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        catalog="analytics",
        target="default.users",
        columns=["id", "email"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": "sensitive-pickled-scan-task",
            "full_row_filter": "region = 'us'",
            "masks": {"email": {"type": "email", "value": None}},
        },
        expires_at=2**31,
        nonce="nonce",
    )

    ticket = codec.sign_payload(payload)
    encoded_reference = ticket.split(".", 1)[0]
    decoded_reference = base64.urlsafe_b64decode(encoded_reference.encode("utf-8"))

    assert b"sensitive-pickled-scan-task" not in decoded_reference
    assert b"default.users" not in decoded_reference
    assert b"email" not in decoded_reference
    assert b"region = 'us'" not in decoded_reference

    verified = codec.verify(ticket)
    assert verified.ticket_id == payload.ticket_id
    assert verified.expires_at == payload.expires_at
    assert verified.nonce == payload.nonce
    assert not hasattr(verified, "scan")
    assert not hasattr(verified, "target")
    assert not hasattr(verified, "columns")


def test_ticket_rejects_expiry_at_current_second(monkeypatch):
    codec = HmacTicketCodecAdapter("secret")
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        target="t",
        columns=[],
        scan=_scan_payload(),
        policy_version=1,
        expires_at=100,
        nonce="expired",
    )
    monkeypatch.setattr("dal_obscura.adapters.tickets.signing.time.time", lambda: 100)

    with pytest.raises(PermissionError, match="expired"):
        codec.verify(codec.sign_payload(payload))


def test_ticket_rejects_boolean_expiry_even_with_valid_signature():
    secret = "secret"
    codec = HmacTicketCodecAdapter(secret)
    raw = b'{"expires_at":true,"nonce":"nonce","ticket_id":"ticket"}'
    encoded_payload = base64.urlsafe_b64encode(raw).decode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), raw, sha256).hexdigest()

    with pytest.raises(PermissionError, match="Invalid ticket payload"):
        codec.verify(f"{encoded_payload}.{signature}")


def test_ticket_rejects_tampered_signature():
    codec = HmacTicketCodecAdapter("secret")
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        target="t",
        columns=["id"],
        scan=_scan_payload(),
        policy_version=1,
        expires_at=2**31,
        nonce="nonce",
    )
    ticket = codec.sign_payload(payload)
    tampered = ticket[:-1] + ("0" if ticket[-1] != "0" else "1")

    with pytest.raises(PermissionError):
        codec.verify(tampered)


def test_ticket_rejects_invalid_ticket_format():
    codec = HmacTicketCodecAdapter("secret")

    with pytest.raises(PermissionError):
        codec.verify("missing-separator")


def test_ticket_rejects_non_json_payload_with_valid_signature():
    secret = "secret"
    codec = HmacTicketCodecAdapter(secret)
    raw = b"not-json"
    encoded_payload = base64.urlsafe_b64encode(raw).decode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), raw, sha256).hexdigest()

    with pytest.raises(PermissionError):
        codec.verify(f"{encoded_payload}.{signature}")


def test_signed_reference_rejects_unknown_fields_even_with_valid_signature():
    import json

    codec = HmacTicketCodecAdapter("secret")
    raw = json.dumps(
        {"ticket_id": "id", "nonce": "nonce", "expires_at": 2**31, "future_field": "ignored-before"}
    ).encode()
    signature = hmac.new(b"secret", raw, sha256).hexdigest()
    token = f"{base64.urlsafe_b64encode(raw).decode()}.{signature}"
    with pytest.raises(PermissionError, match="Invalid ticket payload"):
        codec.verify(token)


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


def test_ticket_payload_preserves_the_full_row_filter():
    scan = ticket_payload().scan.copy()
    scan["full_row_filter"] = "LOWER(region) = 'us'"
    raw = ticket_payload(scan=scan).to_dict()

    restored = TicketPayload.from_dict(raw)

    assert restored.scan["full_row_filter"] == "LOWER(region) = 'us'"


@pytest.mark.parametrize(
    "changes,message",
    [
        pytest.param({"asset_id": None}, "asset_id", id="missing-governed-asset"),
        pytest.param({"expires_at": True}, "expires_at", id="boolean-expiry"),
        pytest.param({"policy_version": False}, "policy_version", id="boolean-policy-version"),
        pytest.param({"columns": ["id", 1]}, "columns", id="non-string-column"),
        pytest.param({"unexpected": "value"}, "fields", id="unknown-field"),
        pytest.param(
            {
                "scan": {
                    "authorization_columns": ["id", "region"],
                    "read_payload": "payload",
                    "masks": {},
                    "full_row_filter": {"type": "comparison"},
                }
            },
            "full_row_filter",
            id="non-sql-filter",
        ),
    ],
)
def test_ticket_payload_rejects_malformed_fields(changes, message):
    raw = ticket_payload().to_dict()
    raw.update(changes)

    with pytest.raises(ValueError, match=message):
        TicketPayload.from_dict(raw)
