from __future__ import annotations

from tests.interfaces.control_plane.test_actor_auth import _bearer, _client
from tests.interfaces.control_plane.workspace_helpers import ADMIN_HEADERS, _provision_draft


def test_audit_events_are_transactional_redacted_and_scoped() -> None:
    client = _client()
    asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["asset-owner"]},
        headers=ADMIN_HEADERS,
    )
    saved = client.put(
        f"/v1/assets/{asset['id']}/draft",
        json={
            "expected_revision": 0,
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["asset-owner"],
                    "columns": ["id"],
                    "masks": {},
                    "row_filter": None,
                }
            ],
        },
        headers=ADMIN_HEADERS,
    ).json()
    client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        json={"expected_draft_revision": saved["revision"]},
        headers=ADMIN_HEADERS,
    )

    events = client.get("/v1/audit/events", headers=ADMIN_HEADERS)
    assert events.status_code == 200
    payload = events.json()
    assert [event["action"] for event in payload[:2]] == [
        "policy.publication.activate",
        "policy.draft.save",
    ]
    assert all(event["resource_id"] == asset["id"] for event in payload[:2])
    assert all("rules" not in event["details"] for event in payload[:2])

    owner_events = client.get("/v1/audit/events", headers=_bearer("owner-token"))
    outsider_events = client.get("/v1/audit/events", headers=_bearer("outsider-token"))
    assert owner_events.status_code == 200
    assert len(owner_events.json()) == 3
    assert owner_events.json()[-1]["action"] == "asset.owners.replace"
    assert outsider_events.status_code == 200
    assert outsider_events.json() == []


def test_audit_limit_is_bounded() -> None:
    client = _client()
    response = client.get("/v1/audit/events?limit=201", headers=ADMIN_HEADERS)
    assert response.status_code == 422
