from dal_obscura.domain.ticket_delivery import TicketPayload
from dal_obscura.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter


def test_ticket_sign_and_verify():
    codec = HmacTicketCodecAdapter("secret")
    payload = TicketPayload(
        table="catalog.db.table",
        columns=["id"],
        scan={"task": {"file": "a"}},
        policy_version=1,
        principal_id="user1",
        expires_at=2**31,
        nonce="abc123",
    )
    ticket = codec.sign_payload(payload)
    verified = codec.verify(ticket)
    assert verified.table == payload.table
    assert verified.columns == payload.columns


def test_ticket_expiry():
    codec = HmacTicketCodecAdapter("secret")
    payload = TicketPayload(
        table="t",
        columns=[],
        scan={},
        policy_version=1,
        principal_id="user1",
        expires_at=0,
        nonce="expired",
    )
    ticket = codec.sign_payload(payload)
    try:
        codec.verify(ticket)
        assert False, "expected expiry"
    except PermissionError:
        assert True
