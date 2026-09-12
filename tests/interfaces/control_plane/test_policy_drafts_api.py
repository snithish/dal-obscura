from __future__ import annotations

from tests.interfaces.control_plane.test_actor_auth import (
    _allow_rule,
    _bearer,
    _client,
    _provision_owned_asset,
)


def test_policy_draft_is_revisioned_and_conflicts_are_explicit() -> None:
    client = _client()
    asset = _provision_owned_asset(client)
    owner = _bearer("owner-token")

    initial = client.get(f"/v1/assets/{asset}/draft", headers=owner)
    saved = client.put(
        f"/v1/assets/{asset}/draft",
        json={"expected_revision": 0, "rules": [_allow_rule(row_filter="region = 'us'")]},
        headers=owner,
    )
    stale = client.put(
        f"/v1/assets/{asset}/draft",
        json={"expected_revision": 0, "rules": [_allow_rule(row_filter="region = 'eu'")]},
        headers=owner,
    )
    current = client.get(f"/v1/assets/{asset}/draft", headers=owner)

    assert initial.status_code == 200
    assert initial.json()["revision"] == 0
    assert saved.status_code == 200
    assert saved.json()["revision"] == 1
    assert saved.json()["content_hash"]
    assert stale.status_code == 409
    assert current.status_code == 200
    assert current.json()["rules"][0]["row_filter"] == "region = 'us'"


def test_policy_draft_body_is_hidden_from_non_owner() -> None:
    client = _client()
    asset = _provision_owned_asset(client)

    response = client.get(f"/v1/assets/{asset}/draft", headers=_bearer("outsider-token"))

    assert response.status_code == 403
