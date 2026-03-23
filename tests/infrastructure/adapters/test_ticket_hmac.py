import base64
import hmac
from hashlib import sha256

import pytest

from dal_obscura.domain.ticket_delivery.models import TicketPayload
from dal_obscura.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter


def _scan_payload():
    return {"read_payload": "payload", "row_filter": None, "masks": {}}


def test_ticket_sign_and_verify():
    codec = HmacTicketCodecAdapter("secret")
    payload = TicketPayload(
        catalog="catalog1",
        target="catalog.db.table",
        columns=["id"],
        scan=_scan_payload(),
        policy_version=1,
        principal_id="user1",
        expires_at=2**31,
        nonce="abc123",
        format="iceberg",
    )
    ticket = codec.sign_payload(payload)
    verified = codec.verify(ticket)
    assert verified.catalog == payload.catalog
    assert verified.target == payload.target
    assert verified.format == payload.format
    assert verified.columns == payload.columns


def test_ticket_expiry():
    codec = HmacTicketCodecAdapter("secret")
    payload = TicketPayload(
        target="t",
        columns=[],
        scan=_scan_payload(),
        policy_version=1,
        principal_id="user1",
        expires_at=0,
        nonce="expired",
        format="iceberg",
    )
    ticket = codec.sign_payload(payload)
    with pytest.raises(PermissionError):
        codec.verify(ticket)


def test_ticket_rejects_tampered_signature():
    codec = HmacTicketCodecAdapter("secret")
    payload = TicketPayload(
        target="t",
        columns=["id"],
        scan=_scan_payload(),
        policy_version=1,
        principal_id="user1",
        expires_at=2**31,
        nonce="nonce",
        format="iceberg",
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
