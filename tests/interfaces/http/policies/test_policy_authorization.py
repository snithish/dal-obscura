from __future__ import annotations

from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from dal_obscura.adapters.sql.database.orm import AssetRecord, DataPlaneTicketRecord
from tests.interfaces.http.authorization_cases import (
    _allow_rule,
    _provision_asset_without_owner,
    _provision_owned_asset,
    _replace_live_policy,
)
from tests.support.control.actors import _bearer, _client


def _create_ticket_for_asset(client: TestClient, asset_id: UUID) -> UUID:
    ticket_id = uuid4()
    session_factory_for_test = _test_session_factory(client)
    with session_factory_for_test() as session:
        asset = session.get(AssetRecord, asset_id)
        assert asset is not None
        session.add(
            DataPlaneTicketRecord(
                ticket_id=ticket_id,
                asset_id=asset_id,
                catalog="analytics",
                target=asset.target,
                principal_id="user1",
                policy_version=asset.policy_revision,
                expires_at=4_000_000_000,
                max_exchanges=1,
                exchange_count=0,
                payload_hash="a" * 64,
                payload_json={},
            )
        )
        session.commit()
    return ticket_id


def _test_session_factory(client: TestClient) -> Any:
    return cast(Any, client.app).state.test_session_factory


def test_mask_exemptions_survive_policy_save_and_live_read(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)
    owner = _bearer("owner-token")
    rule = _allow_rule(row_filter="region = 'us'")
    rule["masks"] = {
        "email": {"type": "hash", "exempt_principals": ["user:alice", "group:privacy"]}
    }
    response = _replace_live_policy(client, asset, [rule], owner)
    assert response.status_code == 200, response.text
    detail = client.get(f"/v1/assets/{asset}", headers=owner)
    assert detail.status_code == 200, detail.text
    assert detail.json()["policy_rules"][0]["masks"]["email"] == {
        "type": "hash",
        "exempt_principals": ["group:privacy", "user:alice"],
    }
    invalid = _allow_rule(row_filter=None)
    invalid["masks"] = {"email": {"type": "hash", "exempt_principals": ["group:"]}}
    rejected = _replace_live_policy(client, asset, [invalid], owner)
    assert rejected.status_code == 422
    unsupported = _allow_rule(row_filter=None)
    unsupported["masks"] = {"email": {"type": "hash", "silent_drop": True}}
    assert _replace_live_policy(client, asset, [unsupported], owner).status_code == 422


def test_asset_owner_can_replace_live_policy_with_revision_precondition(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)
    owner = _bearer("owner-token")
    access = client.get(f"/v1/assets/{asset}/access", headers=owner)
    assert access.status_code == 200
    assert access.json()["can_revoke_tokens"] is True
    assert {item["capability"]: item["allowed"] for item in access.json()["capabilities"]} == {
        "read": True,
        "edit": True,
        "grant": False,
    }

    response = client.put(
        f"/v1/assets/{asset}/policy",
        json={"expected_revision": 1, "rules": [_allow_rule(row_filter="region = 'us'")]},
        headers=owner,
    )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "asset_id": str(asset),
        "policy_revision": 2,
        "revoked_token_count": 0,
    }
    detail = client.get(f"/v1/assets/{asset}", headers=owner)
    assert detail.status_code == 200
    assert detail.json()["policy_revision"] == 2
    assert detail.json()["policy_rules"][0]["row_filter"] == "region = 'us'"

    stale = client.put(
        f"/v1/assets/{asset}/policy", json={"expected_revision": 0, "rules": []}, headers=owner
    )
    assert stale.status_code == 409


def test_only_asset_owner_or_platform_admin_can_revoke_asset_tokens(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)
    other_asset = _provision_owned_asset(client, target="default.audit")
    ticket_ids = [_create_ticket_for_asset(client, asset) for _ in range(2)]
    other_ticket_id = _create_ticket_for_asset(client, other_asset)

    denied = client.post(f"/v1/assets/{asset}/tickets/revoke", headers=_bearer("outsider-token"))
    owner = client.post(f"/v1/assets/{asset}/tickets/revoke", headers=_bearer("owner-token"))

    assert denied.status_code == 403
    assert owner.status_code == 200, owner.text
    assert owner.json() == {"asset_id": str(asset), "revoked_token_count": 2}
    with _test_session_factory(client)() as session:
        revoked_tickets = [
            session.get(DataPlaneTicketRecord, ticket_id) for ticket_id in ticket_ids
        ]
        other_ticket = session.get(DataPlaneTicketRecord, other_ticket_id)
        assert all(
            ticket is not None and ticket.revoked_at is not None for ticket in revoked_tickets
        )
        assert other_ticket is not None and other_ticket.revoked_at is None


def test_asset_policy_change_preserves_tickets_unless_owner_requests_revocation(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)
    ticket_id = _create_ticket_for_asset(client, asset)
    owner = _bearer("owner-token")

    changed = client.put(
        f"/v1/assets/{asset}/policy",
        json={"expected_revision": 1, "rules": [_allow_rule(row_filter="region = 'us'")]},
        headers=owner,
    )
    with _test_session_factory(client)() as session:
        ticket = session.get(DataPlaneTicketRecord, ticket_id)
        assert ticket is not None and ticket.revoked_at is None

    changed_and_revoked = client.put(
        f"/v1/assets/{asset}/policy",
        json={
            "expected_revision": 2,
            "rules": [_allow_rule(row_filter="region = 'eu'")],
            "revoke_existing_tokens": True,
        },
        headers=owner,
    )

    assert changed.status_code == 200, changed.text
    assert changed.json()["revoked_token_count"] == 0
    assert changed_and_revoked.status_code == 200, changed_and_revoked.text
    assert changed_and_revoked.json()["revoked_token_count"] == 1
    with _test_session_factory(client)() as session:
        ticket = session.get(DataPlaneTicketRecord, ticket_id)
        assert ticket is not None and ticket.revoked_at is not None


def test_non_owner_cannot_change_live_policy(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)

    replace = client.put(
        f"/v1/assets/{asset}/policy",
        json={"expected_revision": 0, "rules": [_allow_rule(row_filter="region = 'us'")]},
        headers=_bearer("outsider-token"),
    )

    assert replace.status_code == 403


def test_policy_edit_authorizes_before_validating_submitted_rules(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)

    response = client.put(
        f"/v1/assets/{asset}/policy",
        json={"expected_revision": 1, "rules": [_allow_rule(row_filter="region =")]},
        headers=_bearer("outsider-token"),
    )

    assert response.status_code == 403


def test_non_owner_cannot_read_asset_policy_or_preview(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)

    inventory = client.get("/v1/assets", headers=_bearer("outsider-token"))
    detail = client.get(f"/v1/assets/{asset}", headers=_bearer("outsider-token"))
    preview = client.post(
        f"/v1/assets/{asset}/policy-evaluate",
        json={"principal": "analyst", "groups": [], "claims": {}},
        headers=_bearer("outsider-token"),
    )

    assert inventory.status_code == 200
    assert inventory.json() == []
    assert detail.status_code == 403
    assert preview.status_code == 403
    attributes = client.get(
        f"/v1/assets/{asset}/identity-attributes", headers=_bearer("outsider-token")
    )
    assert attributes.status_code == 403
    owner_attributes = client.get(
        f"/v1/assets/{asset}/identity-attributes", headers=_bearer("owner-token")
    )
    assert owner_attributes.status_code == 200


def test_policy_save_rejects_invalid_row_filter_before_commit(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)

    response = _replace_live_policy(
        client, asset, [_allow_rule(row_filter="region =")], _bearer("owner-token")
    )

    assert response.status_code == 422
    error = response.json()["error"]
    assert error["field_errors"][0]["field"] == "rules.0"
    assert "Invalid row_filter SQL" in error["field_errors"][0]["message"]


def test_policy_save_rejects_deny_rule_with_mask_before_commit(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)
    rule = _allow_rule(row_filter=None)
    rule["effect"] = "deny"
    rule["masks"] = {"email": {"type": "email"}}

    response = _replace_live_policy(client, asset, [rule], _bearer("owner-token"))

    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_error"
    assert error["field_errors"][0]["field"] == "rules.0.effect"


def test_platform_admin_can_assign_owner_and_bootstrap_policy(client_factory):
    client = _client(client_factory)
    asset = _provision_asset_without_owner(client)

    owners = client.put(
        f"/v1/assets/{asset}/owners",
        json={"owners": ["group:asset-owners"], "expected_revision": 0},
        headers=_bearer("admin-oidc-token"),
    )
    policy = _replace_live_policy(
        client, asset, [_allow_rule(row_filter=None)], _bearer("admin-oidc-token")
    )

    assert owners.status_code == 200
    assert owners.json()["owners"] == ["group:asset-owners"]
    assert policy.status_code == 200


def test_rule_metadata_and_global_allow_all_round_trip(client_factory):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)
    rule = {
        "ordinal": 0,
        "effect": "allow_all",
        "principals": ["*"],
        "columns": ["*"],
        "masks": {},
        "row_filter": None,
        "when": {},
        "name": "Open access",
        "description": "All authenticated readers",
    }
    response = _replace_live_policy(client, asset, [rule], _bearer("owner-token"))
    assert response.status_code == 200, response.text
    loaded = client.get(f"/v1/assets/{asset}", headers=_bearer("owner-token")).json()[
        "policy_rules"
    ][0]
    assert {key: loaded[key] for key in rule} == rule


@pytest.mark.parametrize(
    "change",
    [
        {"principals": ["group:analysts"]},
        {"columns": ["email"]},
        {"row_filter": "region = 'US'"},
        {"when": {"region": "US"}},
        {"masks": {"email": {"type": "hash"}}},
    ],
)
def test_global_allow_all_rejects_restrictions(client_factory, change):
    client = _client(client_factory)
    asset = _provision_owned_asset(client)
    rule = {"effect": "allow_all", "principals": ["*"], "columns": ["*"], **change}
    response = _replace_live_policy(client, asset, [rule], _bearer("owner-token"))
    assert response.status_code == 422
    error = response.json()["error"]["field_errors"][0]
    assert error["field"] == "rules.0"
    assert "Allow all must target all users" in error["message"]
