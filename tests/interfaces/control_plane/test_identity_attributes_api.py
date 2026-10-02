from __future__ import annotations

from dal_obscura.control_plane.application import schema_service
from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    DEFAULT_AUTH_MODULE,
    _provision_asset,
)
from tests.support.schema_catalog import EvaluationCatalog


def _args():
    return {
        "issuer": "https://issuer.example",
        "group_claims": ["realm.roles"],
        "attribute_claims": {"department": "employee.department"},
        "attribute_definitions": {
            "department": {
                "label": "Department",
                "description": "Business unit",
                "allowed_values": ["default", "Finance"],
            }
        },
    }


def test_discovery_exposes_only_enabled_attribute_metadata(client_factory):
    client = client_factory()
    asset = _provision_asset(client)
    saved = client.put(
        "/v1/settings/auth-providers",
        headers=ADMIN_HEADERS,
        json={
            "expected_revision": client.get(
                "/v1/settings/auth-providers/revision", headers=ADMIN_HEADERS
            ).json()["revision"],
            "providers": [
                {"ordinal": 1, "module": DEFAULT_AUTH_MODULE, "args": _args()},
                {"ordinal": 2, "module": DEFAULT_AUTH_MODULE, "args": _args(), "enabled": False},
            ],
        },
    )
    assert saved.status_code == 200, saved.text
    url = f"/v1/assets/{asset['id']}/identity-attributes"
    assert client.get(url).status_code == 401
    providers = client.get(url, headers=ADMIN_HEADERS).json()
    assert len(providers) == 1
    assert providers[0]["attributes"] == [
        {
            "key": "department",
            "claim_path": "employee.department",
            "label": "Department",
            "description": "Business unit",
            "allowed_values": ["default", "Finance"],
        }
    ]
    assert set(providers[0]) == {"ordinal", "issuer", "revision", "attributes"}


def test_unsaved_mapping_preview_is_source_free_and_does_not_persist(client_factory):
    client = client_factory()
    url = "/v1/settings/auth-providers/1/attribute-preview"
    payload = {"claims": {"employee": {"department": "Finance"}}, "provider_args": _args()}
    assert client.post(url, json=payload).status_code == 401
    response = client.post(url, json=payload, headers=ADMIN_HEADERS)
    assert response.status_code == 200, response.text
    assert response.json() == {"attributes": {"department": "Finance"}}
    assert client.get("/v1/settings/auth-providers", headers=ADMIN_HEADERS).json() == []
    payload["claims"] = {"employee": {"department": "Unknown"}}
    rejected = client.post(url, json=payload, headers=ADMIN_HEADERS)
    assert rejected.status_code == 400
    assert "outside its allowed values" in rejected.text


def test_policy_test_maps_subject_groups_attributes_and_reports_missing_conditions(
    client_factory, monkeypatch
):
    client = client_factory()
    asset = _provision_asset(client)
    client.put(
        "/v1/settings/auth-providers",
        headers=ADMIN_HEADERS,
        json={
            "expected_revision": client.get(
                "/v1/settings/auth-providers/revision", headers=ADMIN_HEADERS
            ).json()["revision"],
            "providers": [{"ordinal": 1, "module": DEFAULT_AUTH_MODULE, "args": _args()}],
        },
    )
    monkeypatch.setattr(schema_service, "load_catalog", lambda *args, **kwargs: EvaluationCatalog())
    url = f"/v1/assets/{asset['id']}/policy-evaluate"
    payload = {
        "principal": "ignored-synthetic-id",
        "groups": ["ignored"],
        "provider_ordinal": 1,
        "claims": {
            "sub": "user1",
            "realm": {"roles": ["analysts"]},
            "employee": {"department": "default"},
        },
    }
    response = client.post(url, json=payload, headers=ADMIN_HEADERS)
    assert response.status_code == 200, response.text
    evidence = response.json()["evidence"]
    assert evidence["identity"] == {
        "principal": "user1",
        "groups": ["analysts"],
        "attributes": {"department": "default"},
    }
    assert evidence["conditions"][0]["matched"] is True
    del payload["claims"]["employee"]
    response = client.post(url, json=payload, headers=ADMIN_HEADERS)
    assert response.status_code == 200, response.text
    assert response.json()["evidence"]["conditions"][0]["missing"] is True
    payload["provider_ordinal"] = 999
    assert client.post(url, json=payload, headers=ADMIN_HEADERS).status_code == 400


def test_internal_attribute_preview_rejects_nested_values(client_factory, monkeypatch):
    client = client_factory()
    asset = _provision_asset(client)
    monkeypatch.setattr(schema_service, "load_catalog", lambda *args, **kwargs: EvaluationCatalog())
    response = client.post(
        f"/v1/assets/{asset['id']}/policy-evaluate",
        headers=ADMIN_HEADERS,
        json={"principal": "user1", "claims": {"department": ["default"]}},
    )
    assert response.status_code == 400
    assert "Internal attributes must be scalar" in response.text
