"""Secret rotation cannot bypass endpoint policy through a cached read provider."""

import pytest
from dal_obscura_plugin_api import PluginDescriptor

from dal_obscura.adapters.providers.published import LiveConfigCatalogRegistry
from dal_obscura.kernel.plans.snapshot import (
    PublishedAssetSnapshot,
    PublishedCatalog,
    PublishedSource,
)
from dal_obscura.kernel.policy.models import AssetPolicy
from dal_obscura.plugins import PluginRegistry
from dal_obscura.plugins.bootstrap import create_plugin_registry
from dal_obscura.runtime.secrets import SecretProvider
from tests.support.sources.schema_catalog import EvaluationCatalog, schema_registry


@pytest.fixture
def catalog_access():
    from contextlib import ExitStack

    with ExitStack() as resources:

        def build(*, allowlist=()):
            received = []
            values = {
                "ENDPOINT": "https://user:database-password@permitted.example/v1?token=database-token",
                "PASSWORD": "http://169.254.169.254/opaque?secret=credential-value",
            }

            def load_catalog(name, **options):
                received.append(options)
                return EvaluationCatalog()

            providers = schema_registry(load_catalog)
            installed = create_plugin_registry()
            plugins = PluginRegistry(
                allowlist={
                    ("catalog", "iceberg.catalog"): installed.identity(
                        "catalog", "iceberg.catalog"
                    ),
                    ("table_format", "iceberg"): installed.identity("table_format", "iceberg"),
                },
                factory_loader=lambda entry: providers.load(
                    "catalog" if entry.name == "iceberg.catalog" else "table_format", entry.name
                ),
            )
            plugins.reload()

            class Backend:
                descriptor = PluginDescriptor("secret_backend", "fixture", "3", 1, "fixture", "1")

                def get_secret(self, key):
                    return values.get(key)

                def close(self):
                    pass

            secrets = SecretProvider(Backend(), {"catalog:analytics": list(values)})
            resources.callback(secrets.close)
            asset = PublishedAssetSnapshot(
                PublishedCatalog(
                    "analytics",
                    "iceberg.catalog",
                    1,
                    {
                        "uri": {"secret": "ENDPOINT", "scope": "catalog:analytics"},
                        "password": {"secret": "PASSWORD", "scope": "catalog:analytics"},
                    },
                ),
                "default.users",
                PublishedSource("iceberg", "prod.users"),
                AssetPolicy(1, "analytics", "default.users", []),
            )

            class SnapshotReader:
                def get_asset_and_catalog(self, catalog, target):
                    return asset

            catalogs = LiveConfigCatalogRegistry(
                SnapshotReader(),
                plugin_registry=plugins,
                secret_provider=secrets,
                egress_allowlist=allowlist,
            )
            resources.callback(catalogs.close)
            return catalogs, received, values

        yield build


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://169.254.169.254/metadata?token=do-not-disclose",
        "//169.254.169.254/metadata?token=do-not-disclose",
    ],
)
def test_read_rejects_resolved_special_endpoint_before_provider_io(
    catalog_access, caplog, endpoint
):
    catalogs, received, values = catalog_access()
    values["ENDPOINT"] = endpoint
    with pytest.raises(ValueError) as error, catalogs.open("analytics", "default.users"):
        pytest.fail("Special endpoint reached a read provider")
    assert received == []
    assert "169.254.169.254" not in str(error.value)
    assert "do-not-disclose" not in str(error.value)
    assert "do-not-disclose" not in caplog.text


def test_read_rechecks_rotated_endpoints_before_provider_cache_reuse(catalog_access):
    catalogs, received, values = catalog_access(allowlist=("permitted.example",))
    endpoint, credential = values["ENDPOINT"], values["PASSWORD"]
    with catalogs.open("analytics", "default.users") as access:
        assert access.table_format.get_schema().names == ["id", "email", "region"]
    assert received == [{"uri": endpoint, "password": credential}]
    values["ENDPOINT"] = "https://user:rotated-secret@blocked.example/v1"
    with pytest.raises(ValueError) as error, catalogs.open("analytics", "default.users"):
        pytest.fail("Rotated endpoint bypassed admission")
    assert "blocked.example" not in str(error.value)
    assert "rotated-secret" not in str(error.value)
    assert received == [{"uri": endpoint, "password": credential}]
