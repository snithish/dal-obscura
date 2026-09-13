from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from dal_obscura_iceberg_rest.catalog import RestCatalog
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext

from dal_obscura.control_plane.application.catalog_service import validate_catalog_options
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        correlation_id="io-boundary",
    )


def _rest_config(**options: object) -> CatalogConfig:
    return CatalogConfig(
        plugin_id="iceberg.rest",
        instance_id="io-boundary",
        revision=1,
        options={"uri": "https://catalog.example/v1", **options},
    )


def test_storage_paths_cannot_escape_a_published_uri_root() -> None:
    enforcer = PathRuleEnforcer([{"root": "s3://warehouse/curated"}])

    enforcer.check("s3://warehouse/curated/orders/part-0.parquet")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://warehouse/curated/../secrets/credentials.json")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://other-bucket/curated/orders/part-0.parquet")


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"uri": "https://user:pass@catalog.example/v1"}, "credentials"),
        ({"uri": "https://catalog.example/v1?access_token=leak"}, "query"),
        ({"uri": "https://other.example/v1"}, "outside"),
    ],
)
def test_catalog_options_fail_closed_at_the_egress_boundary(
    options: dict[str, str], message: str
) -> None:
    with pytest.raises(ValidationFailure, match=message):
        validate_catalog_options(options, egress_allowlist=("catalog.example",))


def test_rest_plugin_rejects_insecure_authenticated_endpoint_before_provider_setup() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        RestCatalog(_rest_config(uri="http://catalog.example/v1", token="resolved"), _context())


def test_rest_plugin_rejects_credential_bearing_auxiliary_uri() -> None:
    with pytest.raises(ValueError, match="warehouse"):
        RestCatalog(
            _rest_config(warehouse="s3://user:password@warehouse.example/root"),
            _context(),
        )
