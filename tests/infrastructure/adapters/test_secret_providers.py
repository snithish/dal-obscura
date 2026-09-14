from __future__ import annotations

from uuid import UUID

import pytest

from dal_obscura.data_plane.infrastructure.adapters.secret_providers import (
    EnvSecretProvider,
    SecretProviderConfig,
    SecretProviderContext,
    load_secret_provider,
    load_secret_provider_from_environment,
    resolve_secret_refs,
)


def test_env_secret_provider_reads_configured_prefix(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_SECRET_JWT", "jwt-secret")

    provider = EnvSecretProvider(config={"prefix": "DAL_OBSCURA_SECRET_"}, secrets={})

    assert provider.get_secret("JWT") == "jwt-secret"


def test_load_secret_provider_uses_fixed_environment_provider(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOCAL_catalog-password", "value")

    provider = load_secret_provider(
        SecretProviderConfig(config={"prefix": "LOCAL_"}),
        context=SecretProviderContext(
            database_url="sqlite+pysqlite:///:memory:",
            cell_id=UUID("00000000-0000-0000-0000-000000000001"),
        ),
    )

    assert provider.get_secret("catalog-password") == "value"


def test_load_secret_provider_from_environment_uses_startup_prefix(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("LOCAL_catalog-password", "value")
    module_name = (
        "dal_obscura.data_plane.infrastructure.adapters.secret_providers.EnvSecretProvider"
    )

    provider = load_secret_provider_from_environment(
        {
            "DAL_OBSCURA_SECRET_PROVIDER_MODULE": module_name,
            "DAL_OBSCURA_SECRET_PROVIDER_CONFIG": '{"prefix":"LOCAL_"}',
        }
    )

    assert provider.get_secret("catalog-password") == "value"


def test_load_secret_provider_from_environment_rejects_dynamic_module():
    with pytest.raises(ValueError, match="only environment secrets"):
        load_secret_provider_from_environment(
            {"DAL_OBSCURA_SECRET_PROVIDER_MODULE": "untrusted.module.Provider"}
        )


def test_load_secret_provider_rejects_dynamic_module_path():
    with pytest.raises(ValueError, match="only environment secrets"):
        load_secret_provider(
            SecretProviderConfig(module="untrusted.module.Provider"),
            context=SecretProviderContext(
                database_url="sqlite+pysqlite:///:memory:",
                cell_id=UUID("00000000-0000-0000-0000-000000000001"),
            ),
        )


def test_resolve_secret_refs_uses_explicit_secret_shape_only(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOCAL_jwt-signing", "jwt")
    monkeypatch.setenv("LOCAL_api-key", "api")
    provider = EnvSecretProvider(config={"prefix": "LOCAL_"})

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


def test_resolve_secret_refs_rejects_missing_secret():
    provider = EnvSecretProvider()

    with pytest.raises(ValueError, match="Secret 'missing' could not be resolved"):
        resolve_secret_refs(
            {"jwt_secret": {"secret": "missing", "scope": "identity"}},
            provider=provider,
            expected_scope="identity",
        )


def test_resolve_secret_refs_requires_matching_scope(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOCAL_catalog-password", "value")
    provider = EnvSecretProvider(config={"prefix": "LOCAL_"})

    assert resolve_secret_refs(
        {"password": {"secret": "catalog-password", "scope": "catalog:analytics"}},
        provider=provider,
        expected_scope="catalog:analytics",
    ) == {"password": "value"}
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
