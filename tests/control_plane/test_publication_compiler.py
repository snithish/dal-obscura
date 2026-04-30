from __future__ import annotations

from uuid import uuid4

import pytest

from dal_obscura.control_plane.application.compiler import PublicationCompiler
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.domain.models import (
    AssetDraft,
    AuthProviderDraft,
    CatalogDraft,
    CellRuntimeDraft,
    PolicyRuleDraft,
    PublishDraft,
)


def _draft(row_filter: str = "region = 'us'") -> PublishDraft:
    cell_id = uuid4()
    tenant_id = uuid4()
    catalog_id = uuid4()
    asset_id = uuid4()
    return PublishDraft(
        cell_id=cell_id,
        tenants=[tenant_id],
        runtime=CellRuntimeDraft(
            ticket_ttl_seconds=900,
            max_tickets=64,
            path_rules=[],
        ),
        auth_providers=[
            AuthProviderDraft(
                ordinal=1,
                module="dal_obscura.data_plane.infrastructure.adapters.identity_default.DefaultIdentityAdapter",
                args={"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}},
                enabled=True,
            )
        ],
        catalogs=[
            CatalogDraft(
                id=catalog_id,
                cell_id=cell_id,
                tenant_id=tenant_id,
                name="analytics",
                module="dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog",
                options={"type": "sql", "uri": "sqlite:///warehouse.db"},
            )
        ],
        assets=[
            AssetDraft(
                id=asset_id,
                cell_id=cell_id,
                tenant_id=tenant_id,
                catalog_id=catalog_id,
                catalog_name="analytics",
                target="default.users",
                backend="iceberg",
                table_identifier="prod.users",
                options={},
                rules=[
                    PolicyRuleDraft(
                        ordinal=10,
                        effect="allow",
                        principals=["group:analyst"],
                        when={"tenant": "acme"},
                        columns=["id", "email", "region"],
                        masks={"email": {"type": "email"}},
                        row_filter=row_filter,
                    )
                ],
            )
        ],
    )


def test_compiler_publishes_asset_policy_version_and_runtime():
    compiled = PublicationCompiler().compile(_draft())

    assert compiled.runtime.ticket["ttl_seconds"] == 900
    assert compiled.runtime.auth_chain["providers"][0]["ordinal"] == 1
    assert len(compiled.assets) == 1
    asset = compiled.assets[0]
    assert asset.catalog == "analytics"
    assert asset.target == "default.users"
    assert asset.compiled_config["policy"]["rules"][0]["row_filter"] == "region = 'us'"
    assert isinstance(asset.policy_version, int)


def test_compiler_changes_policy_version_when_row_filter_changes():
    first = PublicationCompiler().compile(_draft(row_filter="region = 'us'")).assets[0]
    second = PublicationCompiler().compile(_draft(row_filter="region = 'eu'")).assets[0]

    assert first.policy_version != second.policy_version


def test_compiler_rejects_invalid_row_filter_sql():
    with pytest.raises(ValidationFailure, match="Invalid row_filter"):
        PublicationCompiler().compile(_draft(row_filter="region ="))


def test_compiler_rejects_masks_on_deny_rules():
    draft = _draft()
    deny_rule = PolicyRuleDraft(
        ordinal=20,
        effect="deny",
        principals=["group:analyst"],
        when={},
        columns=["email"],
        masks={"email": {"type": "redact"}},
        row_filter=None,
    )
    draft.assets[0].rules.append(deny_rule)

    with pytest.raises(ValidationFailure, match="deny rules may not define masks"):
        PublicationCompiler().compile(draft)
