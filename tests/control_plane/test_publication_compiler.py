from __future__ import annotations

from dataclasses import replace
from typing import Any, cast
from uuid import uuid4

import pytest

from dal_obscura.common.access_control.compiled_policy import (
    CompiledMaskRule,
    CompiledPolicy,
    CompiledPolicyRule,
)
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
from tests.support.row_filters import (
    PARSER_MULTIPLE_STATEMENT_ROW_FILTERS,
    PARSER_NON_FILTER_STATEMENT_ROW_FILTERS,
    PARSER_UNSAFE_EXPRESSION_ROW_FILTERS,
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
            max_ticket_exchanges=2,
        ),
        auth_providers=[
            AuthProviderDraft(
                ordinal=1,
                module=(
                    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks."
                    "OidcJwksIdentityProvider"
                ),
                args={"issuer": "https://issuer.example"},
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
    assert compiled.runtime.ticket["max_tickets"] == 64
    assert compiled.runtime.ticket["max_exchanges"] == 2
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


def test_compiler_rejects_non_iceberg_backend():
    draft = _draft()
    draft.assets[0].backend = "delta"
    draft.assets[0].table_identifier = "/warehouse/users"

    with pytest.raises(ValidationFailure, match="Unsupported backend 'delta'"):
        PublicationCompiler().compile(draft)


def test_compiler_rejects_unknown_backend():
    draft = _draft()
    draft.assets[0].backend = "unknown"

    with pytest.raises(ValidationFailure, match="Unsupported backend 'unknown'"):
        PublicationCompiler().compile(draft)


def test_compiler_requires_physical_iceberg_identifier():
    draft = _draft()
    draft.assets[0].table_identifier = None

    with pytest.raises(ValidationFailure, match="physical Iceberg identifier"):
        PublicationCompiler().compile(draft)


def test_compiler_rejects_dynamic_runtime_modules():
    catalog_draft = _draft()
    catalog_draft.catalogs[0] = replace(
        catalog_draft.catalogs[0], module="untrusted.catalog.Provider"
    )
    with pytest.raises(ValidationFailure, match="Unsupported catalog module"):
        PublicationCompiler().compile(catalog_draft)

    identity_draft = _draft()
    identity_draft.auth_providers[0] = AuthProviderDraft(
        ordinal=1, module="untrusted.identity.Provider", args={}, enabled=True
    )
    with pytest.raises(ValidationFailure, match="Unsupported identity provider"):
        PublicationCompiler().compile(identity_draft)


def test_compiler_rejects_static_jwks_material_in_identity_provider():
    draft = _draft()
    draft.auth_providers[0].args["jwks"] = {"keys": [{"kty": "RSA"}]}

    with pytest.raises(ValidationFailure, match="static JWKS"):
        PublicationCompiler().compile(draft)


def test_compiler_rejects_custom_backend_with_provider_module():
    draft = _draft()
    draft.catalogs[0].options["provider_modules"] = ["example.PostgresProviderFactory"]
    draft.assets[0].backend = "postgres"
    draft.assets[0].table_identifier = "public.users"
    draft.assets[0].options = {
        "dsn": {"secret": "POSTGRES_DSN"},
    }

    with pytest.raises(ValidationFailure, match="Unsupported backend 'postgres'"):
        PublicationCompiler().compile(draft)


def test_compiler_rejects_custom_backend_without_provider_module():
    draft = _draft()
    draft.assets[0].backend = "postgres"

    with pytest.raises(ValidationFailure, match="Unsupported backend 'postgres'"):
        PublicationCompiler().compile(draft)


def test_compiler_rejects_invalid_row_filter_sql():
    with pytest.raises(ValidationFailure, match="Invalid row_filter"):
        PublicationCompiler().compile(_draft(row_filter="region ="))


@pytest.mark.parametrize(
    "row_filter",
    [
        *PARSER_MULTIPLE_STATEMENT_ROW_FILTERS,
        *PARSER_NON_FILTER_STATEMENT_ROW_FILTERS,
        *PARSER_UNSAFE_EXPRESSION_ROW_FILTERS,
        "regexp_matches(region, 'us')",
    ],
)
def test_compiler_rejects_unsafe_row_filter_shapes(row_filter):
    with pytest.raises(ValidationFailure, match="Invalid row_filter"):
        PublicationCompiler().compile(_draft(row_filter=row_filter))


def test_compiler_rejects_deny_rules():
    draft = _draft()
    deny_rule = PolicyRuleDraft(
        ordinal=20,
        effect=cast(Any, "deny"),
        principals=["group:analyst"],
        when={},
        columns=["email"],
        masks={},
        row_filter=None,
    )
    draft.assets[0].rules.append(deny_rule)

    with pytest.raises(ValidationFailure, match="Policy rules are explicit grants"):
        PublicationCompiler().compile(draft)


@pytest.mark.parametrize(
    "mask",
    [
        {},
        {"type": "unknown"},
        {"type": "keep_last", "value": -1},
        {"type": "redact"},
        {"type": "redact", "value": 1},
    ],
)
def test_compiler_rejects_invalid_mask_instead_of_dropping_it(mask: dict[str, object]):
    draft = _draft()
    draft.assets[0].rules[0] = PolicyRuleDraft(
        ordinal=10,
        effect="allow",
        principals=["group:analyst"],
        when={"tenant": "acme"},
        columns=["id", "email", "region"],
        masks={"email": mask},
        row_filter="region = 'us'",
    )

    with pytest.raises(ValidationFailure, match="Invalid mask"):
        PublicationCompiler().compile(draft)


def test_compiled_policy_round_trips_to_evaluator_policy():
    compiled = CompiledPolicy(
        version=7,
        catalog="analytics",
        target="users",
        rules=[
            CompiledPolicyRule(
                ordinal=1,
                effect="allow",
                principals=["group:data-stewards"],
                columns=["id", "email"],
                masks={"email": CompiledMaskRule(type="email", value=None)},
                row_filter="region = 'us'",
                when={"department": "analytics"},
            )
        ],
    )

    policy = compiled.to_policy()

    assert policy.version == 7
    assert policy.datasets[0].catalog == "analytics"
    assert policy.datasets[0].target == "users"
    assert policy.datasets[0].rules[0].columns == ["id", "email"]
