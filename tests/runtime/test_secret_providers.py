from __future__ import annotations

from typing import Any, cast

import pytest

from dal_obscura.runtime.secrets import (
    SecretProvider,
    SecretProviderConfig,
    load_secret_provider,
    load_secret_provider_from_environment,
    resolve_secret_refs,
)


def test_env_secret_provider_reads_configured_prefix(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_SECRET_JWT", "jwt-secret")

    provider = load_secret_provider(SecretProviderConfig(config={"prefix": "DAL_OBSCURA_SECRET_"}))

    assert provider.get_secret("JWT") == "jwt-secret"


def test_load_secret_provider_uses_fixed_environment_provider(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOCAL_catalog-password", "value")

    provider = load_secret_provider(
        SecretProviderConfig(config={"prefix": "LOCAL_"}),
    )

    assert provider.get_secret("catalog-password") == "value"


def test_load_secret_provider_from_environment_uses_startup_prefix(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("LOCAL_catalog-password", "value")
    provider = load_secret_provider_from_environment(
        {
            "DAL_OBSCURA_SECRET_PROVIDER_KIND": "environment",
            "DAL_OBSCURA_SECRET_PROVIDER_CONFIG": '{"prefix":"LOCAL_"}',
        }
    )

    assert provider.get_secret("catalog-password") == "value"


def test_startup_backend_uses_global_options_from_supplied_environment(monkeypatch):
    import json

    monkeypatch.setenv("GLOBAL_TOKEN", "startup-value")
    provider = load_secret_provider_from_environment(
        {
            "DAL_OBSCURA_PLUGIN_OPTIONS": json.dumps({"dal.plugins.environment.prefix": "GLOBAL_"}),
            "DAL_OBSCURA_SECRET_PROVIDER_CONFIG": json.dumps(
                {
                    "scope_grants": {"catalog:analytics": ["TOKEN"]},
                }
            ),
        }
    )
    try:
        assert (
            resolve_secret_refs(
                {"secret": "TOKEN", "scope": "catalog:analytics"},
                provider=provider,
                expected_scope="catalog:analytics",
            )
            == "startup-value"
        )
    finally:
        provider.close()


def test_load_secret_provider_from_environment_rejects_dynamic_module():
    with pytest.raises(ValueError, match="SECRET_PROVIDER_MODULE is unsupported"):
        load_secret_provider_from_environment(
            {"DAL_OBSCURA_SECRET_PROVIDER_MODULE": "untrusted.module.Provider"}
        )


def test_load_secret_provider_from_environment_rejects_retired_secret_payload():
    with pytest.raises(ValueError, match="SECRET_PROVIDER_SECRETS is unsupported"):
        load_secret_provider_from_environment({"DAL_OBSCURA_SECRET_PROVIDER_SECRETS": "{}"})


def test_load_secret_provider_from_environment_requires_production_grants():
    with pytest.raises(ValueError, match=r"non-empty.*scope_grants"):
        load_secret_provider_from_environment(
            {
                "DAL_OBSCURA_SECRET_PROVIDER_KIND": "environment",
                "DAL_OBSCURA_SECRET_PROVIDER_CONFIG": "{}",
            },
            require_scope_grants=True,
        )


def test_load_secret_provider_rejects_duplicate_json_settings() -> None:
    with pytest.raises(ValueError, match="duplicate JSON key"):
        load_secret_provider_from_environment(
            {
                "DAL_OBSCURA_SECRET_PROVIDER_CONFIG": (
                    '{"prefix":"LOCAL_","scope_grants":{},"scope_grants":{}}'
                )
            }
        )


def test_load_secret_provider_rejects_unknown_kind():
    with pytest.raises(ValueError, match="not admitted"):
        load_secret_provider(
            SecretProviderConfig(kind="untrusted.module.provider"),
        )


def test_resolve_secret_refs_uses_explicit_secret_shape_only(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOCAL_jwt-signing", "jwt")
    monkeypatch.setenv("LOCAL_api-key", "api")
    provider = load_secret_provider(
        SecretProviderConfig(
            config={"prefix": "LOCAL_", "scope_grants": {"identity": ["jwt-signing", "api-key"]}}
        )
    )

    resolved = resolve_secret_refs(
        {
            "jwt_secret": {"secret": "jwt-signing", "scope": "identity"},
            "plain_env_ref": {"key": "DAL_OBSCURA_JWT_SECRET"},
            "keys": [{"id": "svc", "secret": {"secret": "api-key", "scope": "identity"}}],
        },
        provider=provider,
        expected_scope="identity",
    )

    assert resolved == {
        "jwt_secret": "jwt",
        "plain_env_ref": {"key": "DAL_OBSCURA_JWT_SECRET"},
        "keys": [{"id": "svc", "secret": "api"}],
    }


def test_resolve_secret_refs_rejects_malformed_secret_mappings():
    provider = load_secret_provider(SecretProviderConfig())
    with pytest.raises(ValueError, match="scope"):
        resolve_secret_refs(
            {"password": {"secret": 42, "scope": "identity"}},
            provider=provider,
            expected_scope="identity",
        )
    with pytest.raises(ValueError, match="name"):
        resolve_secret_refs(
            {"password": {"secret": "", "scope": "identity"}},
            provider=provider,
            expected_scope="identity",
        )


def test_resolve_secret_refs_rejects_missing_secret():
    provider = load_secret_provider(
        SecretProviderConfig(config={"scope_grants": {"identity": ["missing"]}})
    )

    with pytest.raises(ValueError, match="Secret 'missing' could not be resolved"):
        resolve_secret_refs(
            {"jwt_secret": {"secret": "missing", "scope": "identity"}},
            provider=provider,
            expected_scope="identity",
        )


def test_resolve_secret_refs_requires_matching_scope(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOCAL_catalog-password", "value")
    provider = load_secret_provider(SecretProviderConfig(config={"prefix": "LOCAL_"}))

    with pytest.raises(ValueError, match="not granted"):
        resolve_secret_refs(
            {"password": {"secret": "catalog-password", "scope": "catalog:analytics"}},
            provider=provider,
            expected_scope="catalog:analytics",
        )
    with pytest.raises(ValueError, match="scope"):
        resolve_secret_refs(
            {"password": {"secret": "catalog-password", "scope": "catalog:other"}},
            provider=provider,
            expected_scope="catalog:analytics",
        )
    with pytest.raises(ValueError, match="scope"):
        resolve_secret_refs(
            {"password": {"secret": "catalog-password"}},
            provider=provider,
            expected_scope="catalog:analytics",
        )
    with pytest.raises(ValueError, match="scope"):
        resolve_secret_refs(
            {"password": {"secret": "catalog-password", "scope": "catalog:analytics"}},
            provider=provider,
        )


def test_env_provider_enforces_operator_scope_grants(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOCAL_catalog-password", "value")
    provider = load_secret_provider(
        SecretProviderConfig(
            config={"prefix": "LOCAL_", "scope_grants": {"catalog:analytics": ["catalog-password"]}}
        )
    )

    assert resolve_secret_refs(
        {"password": {"secret": "catalog-password", "scope": "catalog:analytics"}},
        provider=provider,
        expected_scope="catalog:analytics",
    ) == {"password": "value"}
    with pytest.raises(ValueError, match="not granted"):
        resolve_secret_refs(
            {"password": {"secret": "other-secret", "scope": "catalog:analytics"}},
            provider=provider,
            expected_scope="catalog:analytics",
        )


def test_env_provider_rejects_malformed_scope_grants():
    with pytest.raises(ValueError, match="scope grant"):
        load_secret_provider(
            SecretProviderConfig(config={"scope_grants": {"catalog:analytics": "catalog-password"}})
        )


def test_resolver_rejects_provider_without_scope_authorization():
    class UnscopedProvider:
        def get_secret(self, key: str) -> str | None:
            return "secret-value"

    with pytest.raises(ValueError, match="scope authorization"):
        resolve_secret_refs(
            {"password": {"secret": "password", "scope": "catalog:analytics"}},
            provider=cast(SecretProvider, UnscopedProvider()),
            expected_scope="catalog:analytics",
        )


@pytest.mark.parametrize("scope", ["catalog:analytics", "catalog:other"])
def test_frozen_catalog_arrays_resolve_secret_references_without_bypassing_scope(
    monkeypatch, scope
):
    from dal_obscura_plugin_api import CatalogConfig

    monkeypatch.setenv("CATALOG_TOKEN", "private-token")
    provider = load_secret_provider(
        SecretProviderConfig(config={"scope_grants": {"catalog:analytics": ["CATALOG_TOKEN"]}})
    )
    config = CatalogConfig(
        "fixture.catalog",
        "analytics",
        1,
        {"channels": [{"headers": {"secret": "CATALOG_TOKEN", "scope": scope}}]},
    )
    if scope == "catalog:other":
        with pytest.raises(ValueError, match="scope does not match"):
            resolve_secret_refs(
                config.options, provider=provider, expected_scope="catalog:analytics"
            )
    else:
        assert resolve_secret_refs(
            config.options, provider=provider, expected_scope="catalog:analytics"
        ) == {"channels": [{"headers": "private-token"}]}


def test_backend_cannot_override_host_scope_grants():
    reads = []

    class Backend:
        def is_allowed(self, key, scope):
            return True

        def get_secret(self, key):
            reads.append(key)
            return "private-value"

        def close(self):
            pass

    provider = SecretProvider(cast(Any, Backend()), {"catalog:analytics": ["approved"]})
    with pytest.raises(ValueError, match="not granted"):
        resolve_secret_refs(
            {"secret": "unapproved", "scope": "catalog:analytics"},
            provider=provider,
            expected_scope="catalog:analytics",
        )
    assert reads == []


def test_startup_loads_an_artifact_locked_external_secret_backend(tmp_path, monkeypatch):
    import json
    from importlib import metadata

    from dal_obscura.plugins import PluginRegistry, build_plugin_lock, load_static_plugin_descriptor
    from tests.support.plugins.installed_plugin import installed_plugin

    installed_plugin(tmp_path)
    info = tmp_path / "acme_delta_plugin-1.0.dist-info"
    (info / "entry_points.txt").write_text(
        "[dal_obscura.secret_backends.v3]\nacme.secrets = acme_secret_fixture:factory\n"
    )
    manifest = info / "dal_obscura-plugin.json"
    document = json.loads(manifest.read_text())
    document["descriptors"][0].update(kind="secret_backend", plugin_id="acme.secrets")
    manifest.write_text(json.dumps(document))
    (tmp_path / "acme_secret_fixture.py").write_text("""
from pathlib import Path
from dal_obscura_plugin_api import PluginDescriptor

class Backend:
    descriptor = PluginDescriptor("secret_backend", "acme.secrets", "3", 1,
                                  "acme-delta-plugin", "1.0")
    def __init__(self, options):
        if set(options) != {"prefix", "marker"}:
            raise ValueError("Backend received unexpected options")
        self.options = options
    def get_secret(self, key):
        return self.options["prefix"] + key
    def close(self):
        Path(self.options["marker"]).write_text("closed")

def factory(options, context):
    context.check_active()
    return Backend(options)
""")
    with (info / "RECORD").open("a") as record:
        record.write("\nacme_secret_fixture.py,,\n")
    entries = metadata.EntryPoints(metadata.Distribution.at(info).entry_points)
    entry = next(iter(entries))
    registry = PluginRegistry(
        allowlist={
            ("secret_backend", "acme.secrets"): build_plugin_lock(
                "secret_backend", entry, load_static_plugin_descriptor(entry)
            )
        },
        entry_points_fn=lambda: entries,
        options={"dal.plugins.acme.secrets.prefix": "startup:"},
    )
    registry.reload()
    monkeypatch.syspath_prepend(str(tmp_path))
    marker = tmp_path / "closed.marker"
    provider = load_secret_provider(
        SecretProviderConfig(
            kind="acme.secrets",
            config={"marker": str(marker), "scope_grants": {"catalog:analytics": ["TOKEN"]}},
        ),
        registry=registry,
    )
    try:
        assert (
            resolve_secret_refs(
                {"secret": "TOKEN", "scope": "catalog:analytics"},
                provider=provider,
                expected_scope="catalog:analytics",
            )
            == "startup:TOKEN"
        )
    finally:
        provider.close()
    assert marker.read_text() == "closed"
