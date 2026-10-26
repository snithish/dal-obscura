"""Resolved catalog endpoints are checked before either governance IO boundary."""

from uuid import uuid4

import pytest
from dal_obscura_plugin_api import CatalogConfig, DiscoveryPage, PluginDescriptor
from dal_obscura_plugin_api.options import PluginOptions

from dal_obscura.adapters.providers.governance import (
    ProviderAssetSchemaLoader,
    ProviderCatalogBrowser,
)
from dal_obscura.application.governance.errors import ValidationFailure
from dal_obscura.application.governance.values import AssetSchemaDiscovery, CatalogDiscovery
from dal_obscura.runtime.secrets import SecretProvider
from tests.support.sources.schema_catalog import EvaluationCatalog, schema_registry


def _request(boundary, *, options, global_options=None, secrets=None, allowlist=(), lookups=None):
    received = []

    def load_catalog(name, **resolved):
        received.append(resolved)
        return EvaluationCatalog()

    registry = schema_registry(load_catalog)
    registry.options = PluginOptions({"iceberg.catalog": global_options or {}})
    values = secrets or {}

    class Backend:
        descriptor = PluginDescriptor("secret_backend", "fixture", "3", 1, "fixture", "1")

        def get_secret(self, key):
            if lookups is not None:
                lookups.append(key)
            return values.get(key)

        def close(self):
            pass

    provider = SecretProvider(Backend(), {"catalog:analytics": list(values)})
    captured = CatalogDiscovery(uuid4(), CatalogConfig("iceberg.catalog", "analytics", 1, options))
    if boundary == "schema":
        loader = ProviderAssetSchemaLoader(
            plugin_registry=registry, secret_provider=provider, egress_allowlist=allowlist
        )

        def call():
            return loader.load_asset_schema(
                AssetSchemaDiscovery(uuid4(), 1, "events", "iceberg", "prod.users", captured)
            )
    else:
        browser = ProviderCatalogBrowser(
            plugin_registry=registry, secret_provider=provider, egress_allowlist=allowlist
        )

        def call():
            return browser.discover(captured, session_key="endpoint-test")

    return call, received


@pytest.mark.parametrize("boundary", ["schema", "discovery"])
@pytest.mark.parametrize("source", ["reference", "global"])
@pytest.mark.parametrize("scheme", ["http:", ""])
def test_resolved_special_endpoint_is_rejected_before_provider_io(boundary, source, scheme):
    endpoint = f"{scheme}//169.254.169.254/metadata?secret=do-not-disclose"
    options = {"uri": {"secret": "ENDPOINT", "scope": "catalog:analytics"}}
    call, received = _request(
        boundary,
        options=options if source == "reference" else {},
        global_options={"uri": endpoint} if source == "global" else {},
        secrets={"ENDPOINT": endpoint},
    )
    with pytest.raises(ValidationFailure) as error:
        call()
    assert "uri" in str(error.value)
    assert "169.254.169.254" not in str(error.value)
    assert "do-not-disclose" not in str(error.value)
    assert received == []


@pytest.mark.parametrize("boundary", ["schema", "discovery"])
def test_resolved_endpoint_uses_configured_egress_allowlist(boundary):
    call, received = _request(
        boundary,
        options={"uri": {"secret": "ENDPOINT", "scope": "catalog:analytics"}},
        secrets={"ENDPOINT": "https://private-catalog.example/v1"},
        allowlist=("permitted.example",),
    )
    with pytest.raises(ValidationFailure) as error:
        call()
    assert "private-catalog.example" not in str(error.value)
    assert received == []


@pytest.mark.parametrize("boundary", ["schema", "discovery"])
def test_endpoint_policy_preserves_authorized_credentials_and_instance_precedence(boundary):
    endpoint = "https://user:database-password@permitted.example/v1?token=database-token"
    credential = "http://169.254.169.254/opaque?secret=credential-value"
    call, received = _request(
        boundary,
        options={
            "uri": {"secret": "ENDPOINT", "scope": "catalog:analytics"},
            "password": {"secret": "PASSWORD", "scope": "catalog:analytics"},
        },
        global_options={"uri": "https://blocked.example"},
        secrets={"ENDPOINT": endpoint, "PASSWORD": credential},
        allowlist=("permitted.example",),
    )
    result = call()
    if boundary == "schema":
        assert result.names == ["id", "email", "region"]
    else:
        assert result == DiscoveryPage(())
    assert received == [{"uri": endpoint, "password": credential}]


@pytest.mark.parametrize("boundary", ["schema", "discovery"])
def test_url_shaped_credential_reference_name_is_opaque(boundary):
    name = "https://secret-identifiers.example/credential"
    credential = "http://169.254.169.254/opaque?secret=credential-value"
    call, received = _request(
        boundary,
        options={
            "uri": "https://permitted.example/v1",
            "password": {"secret": name, "scope": "catalog:analytics"},
        },
        secrets={name: credential},
        allowlist=("permitted.example",),
    )
    call()
    assert received == [{"uri": "https://permitted.example/v1", "password": credential}]


@pytest.mark.parametrize("boundary", ["schema", "discovery"])
def test_url_shaped_endpoint_reference_name_does_not_bypass_resolved_policy(boundary):
    name = "https://secret-identifiers.example/endpoint"
    endpoint = "https://denied-endpoint.example/v1?token=endpoint-secret"
    lookups = []
    call, received = _request(
        boundary,
        options={"uri": {"secret": name, "scope": "catalog:analytics"}},
        secrets={name: endpoint},
        allowlist=("permitted.example",),
        lookups=lookups,
    )
    with pytest.raises(ValidationFailure) as error:
        call()
    assert lookups == [name]
    assert received == []
    assert "uri" in str(error.value)
    assert "secret-identifiers.example" not in str(error.value)
    assert "denied-endpoint.example" not in str(error.value)
    assert "endpoint-secret" not in str(error.value)
