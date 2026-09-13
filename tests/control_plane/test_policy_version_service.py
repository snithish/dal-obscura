from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

from dal_obscura.common.config_store.orm import (
    ActivePublicationRecord,
    AuditEventRecord,
    ConfigPublicationRecord,
    PublicationOperationRecord,
)
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.application.policy_version_service import (
    _catalogs_with_selected,
    _validate_idempotency_key,
)
from dal_obscura.control_plane.application.provisioning import ProvisioningService
from dal_obscura.control_plane.domain.models import CatalogDraft, CompiledCatalog
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def test_republishing_a_catalog_replaces_its_compiled_configuration() -> None:
    tenant_id = uuid4()
    current = CompiledCatalog(
        tenant_id=tenant_id,
        catalog="analytics",
        config={"module": "iceberg", "options": {"uri": "https://old.example"}},
    )
    selected = CatalogDraft(
        id=uuid4(),
        cell_id=uuid4(),
        tenant_id=tenant_id,
        name="analytics",
        module="iceberg.rest",
        options={"uri": "https://new.example"},
        revision=3,
    )

    result = _catalogs_with_selected([current], selected)

    assert len(result) == 1
    assert result[0].config == {
        "module": "iceberg.rest",
        "options": {"uri": "https://new.example"},
        "revision": 3,
    }


def test_idempotency_keys_are_bounded_printable_text() -> None:
    assert _validate_idempotency_key("  publish-1  ") == "publish-1"

    with pytest.raises(ValidationFailure, match="between 1 and 128"):
        _validate_idempotency_key("   ")
    with pytest.raises(ValidationFailure, match="printable"):
        _validate_idempotency_key("publish\n1")
    with pytest.raises(ValidationFailure, match="128"):
        _validate_idempotency_key("x" * 129)


def test_publication_failure_rolls_back_generation_audit_and_operation(
    db_session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ProvisioningService(db_session)
    admin = ControlPlaneActor.for_platform_admin("platform:admin")
    service.upsert_workspace_runtime_settings(900, 8, 1)
    service.upsert_workspace_catalog(
        name="analytics",
        module=(
            "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
        ),
        options={"type": "sql", "uri": "sqlite:///catalog.db"},
    )
    asset = service.upsert_workspace_asset(
        catalog="analytics",
        target="default.events",
        backend="iceberg",
        table_identifier="prod.events",
        options={},
    )
    asset_id = UUID(asset["id"])
    service.replace_policy_rules(
        asset_id,
        [
            {
                "ordinal": 1,
                "effect": "allow",
                "principals": ["user1"],
                "when": {},
                "columns": ["id"],
                "masks": {},
                "row_filter": None,
            }
        ],
        actor=admin,
    )
    service.replace_asset_owners(asset_id, ["user:owner@example.com"])
    service.replace_workspace_auth_providers(
        [
            {
                "ordinal": 1,
                "module": (
                    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks."
                    "OidcJwksIdentityProvider"
                ),
                "args": {"issuer": "https://issuer.example"},
                "enabled": True,
            }
        ]
    )

    def fail_audit(*_args, **_kwargs):
        raise RuntimeError("injected audit failure")

    monkeypatch.setattr(PublicationStore, "record_asset_audit_event", fail_audit)
    with pytest.raises(RuntimeError, match="injected audit failure"):
        service.create_asset_policy_version(
            asset_id,
            actor=admin,
            idempotency_key="rollback-1",
        )
    db_session.rollback()

    assert db_session.scalar(select(ActivePublicationRecord)) is None
    assert db_session.scalar(select(ConfigPublicationRecord)) is None
    assert db_session.scalar(select(PublicationOperationRecord)) is None
    assert db_session.scalar(select(AuditEventRecord)) is None
