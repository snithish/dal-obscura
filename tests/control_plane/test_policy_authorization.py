from __future__ import annotations

from uuid import UUID

import pytest

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import AuthorizationFailure
from dal_obscura.control_plane.application.provisioning import ProvisioningService

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)


def test_asset_owner_can_replace_policy_rules(db_session):
    service, asset_id = _workspace_asset(db_session)
    service.replace_asset_owners(asset_id, ["user:alice@example.com"])

    service.replace_policy_rules(
        asset_id,
        [_allow_rule()],
        actor=ControlPlaneActor(
            principal="user:alice@example.com",
            groups=(),
            platform_admin=False,
        ),
    )

    assert service.list_policy_rules(asset_id)[0]["principals"] == ["group:data-stewards"]


def test_group_owner_can_replace_policy_rules(db_session):
    service, asset_id = _workspace_asset(db_session)
    service.replace_asset_owners(asset_id, ["group:data-owners"])

    service.replace_policy_rules(
        asset_id,
        [_allow_rule()],
        actor=ControlPlaneActor(
            principal="user:bob@example.com",
            groups=("data-owners",),
            platform_admin=False,
        ),
    )

    assert service.list_policy_rules(asset_id)[0]["columns"] == ["id", "email"]


def test_outsider_cannot_replace_policy_rules(db_session):
    service, asset_id = _workspace_asset(db_session)
    service.replace_asset_owners(asset_id, ["user:alice@example.com"])

    with pytest.raises(AuthorizationFailure, match="Only platform admins or asset owners"):
        service.replace_policy_rules(
            asset_id,
            [_allow_rule()],
            actor=ControlPlaneActor(
                principal="user:eve@example.com",
                groups=("analytics",),
                platform_admin=False,
            ),
        )


def test_platform_admin_can_replace_policy_rules_without_asset_owner(db_session):
    service, asset_id = _workspace_asset(db_session)

    service.replace_policy_rules(
        asset_id,
        [_allow_rule()],
        actor=ControlPlaneActor.for_platform_admin("platform:admin"),
    )

    assert service.list_policy_rules(asset_id)[0]["effect"] == "allow"


def _workspace_asset(db_session) -> tuple[ProvisioningService, UUID]:
    service = ProvisioningService(db_session)
    service.upsert_workspace_catalog(
        name="analytics",
        module=ICEBERG_CATALOG_MODULE,
        options={"type": "sql", "uri": "sqlite:///catalog.db"},
    )
    asset = service.upsert_workspace_asset(
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        table_identifier="prod.users",
        options={},
    )
    return service, UUID(asset["id"])


def _allow_rule() -> dict[str, object]:
    return {
        "ordinal": 1,
        "effect": "allow",
        "principals": ["group:data-stewards"],
        "when": {},
        "columns": ["id", "email"],
        "masks": {},
        "row_filter": None,
    }
