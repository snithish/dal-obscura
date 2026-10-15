from __future__ import annotations

from dal_obscura.kernel.identity.names import encode_federated_identity
from tests.interfaces.http.authorization_cases import (
    _allow_rule,
    _provision_asset_without_owner,
    _provision_owned_asset,
)
from tests.support.control.actors import _bearer, _client
from tests.support.control.workspace import ADMIN_HEADERS


def test_federated_asset_owner_is_scoped_to_exact_issuer_through_api(client_factory):
    client = _client(client_factory)
    asset = _provision_asset_without_owner(client)
    issuer_a = "https://issuer-a.example/"

    owners = client.put(
        f"/v1/assets/{asset}/owners",
        json={"owners": [encode_federated_identity(issuer_a, "alice")], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    assert owners.status_code == 200, owners.text

    current = client.get(f"/v1/assets/{asset}", headers=ADMIN_HEADERS).json()
    owner_update = client.put(
        f"/v1/assets/{asset}/policy",
        json={
            "expected_revision": current["policy_revision"],
            "rules": [_allow_rule(row_filter="region = 'us'")],
        },
        headers=_bearer("issuer-a-owner-token"),
    )
    other_issuer_update = client.put(
        f"/v1/assets/{asset}/policy",
        json={
            "expected_revision": current["policy_revision"] + 1,
            "rules": [_allow_rule(row_filter="region = 'eu'")],
        },
        headers=_bearer("issuer-b-owner-token"),
    )

    assert owner_update.status_code == 200, owner_update.text
    assert other_issuer_update.status_code == 403


def test_workspace_summary_is_scoped_to_visible_assets(client_factory):
    client = _client(client_factory)
    _provision_owned_asset(client)

    outsider = client.get("/v1/workspace/summary", headers=_bearer("outsider-token"))
    owner = client.get("/v1/workspace/summary", headers=_bearer("owner-token"))

    assert outsider.status_code == 200
    assert outsider.json()["asset_count"] == 0
    assert outsider.json()["catalog_count"] == 0
    assert owner.status_code == 200
    assert owner.json()["asset_count"] == 1
    assert owner.json()["enabled_auth_provider_count"] == 0


def test_non_admin_cannot_read_catalog_or_auth_settings(client_factory):
    client = _client(client_factory)

    catalogs = client.get("/v1/catalogs", headers=_bearer("outsider-token"))
    runtime = client.get("/v1/settings/runtime", headers=_bearer("outsider-token"))
    providers = client.get("/v1/settings/auth-providers", headers=_bearer("outsider-token"))

    assert catalogs.status_code == 403
    assert runtime.status_code == 403
    assert providers.status_code == 403
