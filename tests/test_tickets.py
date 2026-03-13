from dal_obscura.tickets import TicketSigner, new_ticket_payload


def test_ticket_sign_and_verify():
    signer = TicketSigner("secret")
    payload = new_ticket_payload(
        table="catalog.db.table",
        snapshot="1",
        columns=["id"],
        scan={"task": {"file": "a"}},
        policy_version=1,
        principal_id="user1",
        ttl_seconds=60,
    )
    ticket = signer.sign_payload(payload).encode()
    verified = signer.verify(ticket)
    assert verified.table == payload.table
    assert verified.columns == payload.columns


def test_ticket_expiry():
    signer = TicketSigner("secret")
    payload = new_ticket_payload(
        table="t",
        snapshot="1",
        columns=[],
        scan={},
        policy_version=1,
        principal_id="user1",
        ttl_seconds=-1,
    )
    ticket = signer.sign_payload(payload).encode()
    try:
        signer.verify(ticket)
        assert False, "expected expiry"
    except PermissionError:
        assert True
