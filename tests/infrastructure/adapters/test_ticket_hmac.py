import base64
import hmac
from hashlib import sha256

import pytest

from dal_obscura.common.ticket_delivery.models import TicketPayload
from dal_obscura.data_plane.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter


def _scan_payload():
    return {"read_payload": "payload", "full_row_filter": None, "masks": {}}


def test_ticket_sign_and_verify():
    codec = HmacTicketCodecAdapter("secret")
    payload = TicketPayload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        catalog="catalog1",
        target="catalog.db.table",
        columns=["id"],
        scan=_scan_payload(),
        policy_version=1,
        principal_id="user1",
        expires_at=2**31,
        nonce="abc123",
    )
    ticket = codec.sign_payload(payload)
    verified = codec.verify(ticket)
    assert verified.ticket_id == payload.ticket_id
    assert verified.expires_at == payload.expires_at
    assert verified.nonce == payload.nonce
    assert verified.target == ""
    assert verified.columns == []


def test_signed_ticket_is_opaque_and_does_not_embed_scan_payload():
    codec = HmacTicketCodecAdapter("secret")
    payload = TicketPayload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        catalog="analytics",
        target="default.users",
        columns=["id", "email"],
        scan={
            "read_payload": "sensitive-pickled-scan-task",
            "full_row_filter": "region = 'us'",
            "masks": {"email": {"type": "email", "value": None}},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=2**31,
        nonce="nonce",
        tenant_id="tenant-a",
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
    assert verified.scan["read_payload"] == ""


def test_ticket_expiry():
    codec = HmacTicketCodecAdapter("secret")
    payload = TicketPayload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        target="t",
        columns=[],
        scan=_scan_payload(),
        policy_version=1,
        principal_id="user1",
        expires_at=0,
        nonce="expired",
    )
    ticket = codec.sign_payload(payload)
    with pytest.raises(PermissionError):
        codec.verify(ticket)


def test_ticket_rejects_tampered_signature():
    codec = HmacTicketCodecAdapter("secret")
    payload = TicketPayload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        target="t",
        columns=["id"],
        scan=_scan_payload(),
        policy_version=1,
        principal_id="user1",
        expires_at=2**31,
        nonce="nonce",
    )
    ticket = codec.sign_payload(payload)
    tampered = ticket[:-1] + ("0" if ticket[-1] != "0" else "1")

    with pytest.raises(PermissionError):
        codec.verify(tampered)


def test_ticket_rejects_malformed_payload():
    secret = "secret"
    codec = HmacTicketCodecAdapter(secret)
    raw = b'{"not":"valid ticket payload"'
    encoded_payload = base64.urlsafe_b64encode(raw).decode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), raw, sha256).hexdigest()

    with pytest.raises(PermissionError):
        codec.verify(f"{encoded_payload}.{signature}")


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
