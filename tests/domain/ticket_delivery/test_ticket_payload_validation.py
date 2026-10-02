"""Value-object parsing owns malformed payload cases; use cases test ticket admission."""

import pytest

from dal_obscura.read.tickets import TicketPayload
from tests.support.tickets import ticket_payload


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
