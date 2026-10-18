from __future__ import annotations

from tests.interfaces.http.authorization_cases import _allow_rule, _provision_owned_asset
from tests.support.control.actors import _bearer, _client
from tests.support.control.workspace import ADMIN_HEADERS


def test_asset_owner_can_delegate_read_without_edit_or_grant(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)
    grant = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [{"principal": "group:asset-owners", "capability": "grant"}],
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )
    grants = client.put(
        f"/v1/assets/{asset}/grants",
        json={"grants": [{"principal": "outsider", "capability": "read"}], "expected_revision": 2},
        headers=_bearer("owner-token"),
    )

    inventory = client.get("/v1/assets", headers=_bearer("outsider-token"))
    detail = client.get(f"/v1/assets/{asset}", headers=_bearer("outsider-token"))
    replace = client.put(
        f"/v1/assets/{asset}/policy",
        json={
            "expected_revision": detail.json()["policy_revision"],
            "rules": [_allow_rule(row_filter=None)],
        },
        headers=_bearer("outsider-token"),
    )

    assert grant.status_code == 200
    assert grants.status_code == 200
    assert grants.json()["grants"] == [{"principal": "outsider", "capability": "read"}]
    assert inventory.status_code == 200
    assert inventory.json()[0]["id"] == str(asset)
    assert detail.status_code == 200
    assert detail.json()["id"] == str(asset)
    assert replace.status_code == 403


def test_asset_owner_cannot_delegate_grant_management(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)
    grant = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [{"principal": "group:asset-owners", "capability": "grant"}],
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )

    response = client.put(
        f"/v1/assets/{asset}/grants",
        json={"grants": [{"principal": "outsider", "capability": "grant"}]},
        headers=_bearer("owner-token"),
    )

    assert grant.status_code == 200
    assert response.status_code == 403
    assert "Only platform admins" in response.json()["error"]["message"]


def test_grant_manager_cannot_self_escalate_but_can_delegate_held_authority(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)

    delegated = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [{"principal": "grant-manager", "capability": "grant"}],
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )
    self_escalation = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [
                {"principal": "grant-manager", "capability": "grant"},
                {"principal": "grant-manager", "capability": "edit"},
            ],
            "expected_revision": 2,
        },
        headers=_bearer("grant-manager-token"),
    )
    delegation = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [
                {"principal": "grant-manager", "capability": "grant"},
                {"principal": "outsider", "capability": "read"},
                {"principal": "outsider", "capability": "edit"},
            ],
            "expected_revision": 2,
        },
        headers=_bearer("grant-manager-token"),
    )

    assert delegated.status_code == 200
    assert self_escalation.status_code == 403
    assert "already hold" in self_escalation.json()["error"]["message"]
    assert delegation.status_code == 200
    editor_access = client.get(f"/v1/assets/{asset}/access", headers=_bearer("outsider-token"))
    assert editor_access.status_code == 200
    assert editor_access.json()["can_revoke_tokens"] is False
    assert (
        next(item for item in editor_access.json()["capabilities"] if item["capability"] == "edit")[
            "allowed"
        ]
        is True
    )
