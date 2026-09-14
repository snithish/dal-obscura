from __future__ import annotations

from tests.interfaces.control_plane.test_actor_auth import _bearer, _client
from tests.interfaces.control_plane.workspace_helpers import ADMIN_HEADERS, _provision_draft


def test_audit_events_are_transactional_redacted_and_scoped() -> None:
    client = _client()
    asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["asset-owner"], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    current_draft = client.get(f"/v1/assets/{asset['id']}/draft", headers=ADMIN_HEADERS).json()
    saved_response = client.put(
        f"/v1/assets/{asset['id']}/draft",
        json={
            "expected_revision": current_draft["revision"],
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
    )
    saved = saved_response.json()
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
    assert payload[1]["correlation_id"] == saved_response.headers["x-request-id"]

    owner_events = client.get("/v1/audit/events", headers=_bearer("owner-token"))
    outsider_events = client.get("/v1/audit/events", headers=_bearer("outsider-token"))
    assert owner_events.status_code == 200
    owner_actions = [event["action"] for event in owner_events.json()]
    assert len(owner_actions) == 4
    assert "asset.owners.replace" in owner_actions
    assert outsider_events.status_code == 200
    assert outsider_events.json() == []


def test_audit_limit_is_bounded() -> None:
    client = _client()
    response = client.get("/v1/audit/events?limit=201", headers=ADMIN_HEADERS)
    assert response.status_code == 422
    payload = response.json()
    assert payload["error"]["code"] == "validation_error"
    assert payload["error"]["request_id"] == response.headers["x-request-id"]
    assert payload["error"]["field_errors"]


def test_audit_page_uses_keyset_cursor_and_preserves_scope() -> None:
    client = _client()
    asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["asset-owner"], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["asset-owner", "backup-owner"], "expected_revision": 1},
        headers=ADMIN_HEADERS,
    )
    first = client.get("/v1/audit/events/page?limit=1", headers=_bearer("owner-token"))
    assert first.status_code == 200
    first_payload = first.json()
    assert len(first_payload["items"]) == 1
    assert first_payload["next_cursor"]

    second = client.get(
        "/v1/audit/events/page?limit=1&cursor=" + first_payload["next_cursor"],
        headers=_bearer("owner-token"),
    )
    assert second.status_code == 200
    second_payload = second.json()
    assert len(second_payload["items"]) == 1
    assert second_payload["items"][0]["id"] != first_payload["items"][0]["id"]
    assert all(
        item["resource_id"] == asset["id"]
        for item in first_payload["items"] + second_payload["items"]
    )

    invalid = client.get(
        "/v1/audit/events/page?cursor=invalid",
        headers=ADMIN_HEADERS,
    )
    assert invalid.status_code == 400


def test_audit_page_filters_before_pagination() -> None:
    client = _client()
    _provision_draft(client)
    response = client.get(
        "/v1/audit/events/page?action=workspace.runtime.update&resource_type=workspace&limit=1",
        headers=ADMIN_HEADERS,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"]
    assert all(
        item["action"] == "workspace.runtime.update" and item["resource_type"] == "workspace"
        for item in payload["items"]
    )
    assert payload["next_cursor"] is None

    by_actor = client.get(
        "/v1/audit/events/page?actor=platform%3Aadmin&outcome=success",
        headers=ADMIN_HEADERS,
    )
    assert by_actor.status_code == 200
    assert by_actor.json()["items"]
    assert all(item["actor"] == "platform:admin" for item in by_actor.json()["items"])

    too_long = client.get(
        "/v1/audit/events/page?actor=" + ("x" * 201),
        headers=ADMIN_HEADERS,
    )
    assert too_long.status_code == 422
