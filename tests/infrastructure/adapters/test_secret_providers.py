from __future__ import annotations

from uuid import UUID

import pytest

from dal_obscura.data_plane.infrastructure.adapters.secret_providers import (
    EnvSecretProvider,
    SecretProviderConfig,
    SecretProviderContext,
    load_secret_provider,
    resolve_secret_refs,
)


def test_env_secret_provider_reads_configured_prefix(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_SECRET_JWT", "jwt-secret")

    provider = EnvSecretProvider(config={"prefix": "DAL_OBSCURA_SECRET_"}, secrets={})

    assert provider.get_secret("JWT") == "jwt-secret"


def test_load_secret_provider_instantiates_module_with_bootstrap_config():
    provider = load_secret_provider(
        SecretProviderConfig(
            module="tests.support.secret_provider_fakes.FakeSecretProvider",
            config={"prefix": "local"},
            secrets={"token": "bootstrap-token"},
        ),
        context=SecretProviderContext(
            database_url="sqlite+pysqlite:///:memory:",
            cell_id=UUID("00000000-0000-0000-0000-000000000001"),
        ),
    )

    assert provider.get_secret("catalog-password") == "local:bootstrap-token:catalog-password"


def test_load_secret_provider_rejects_missing_bootstrap_secret():
    with pytest.raises(ValueError, match="bootstrap secret 'token'"):
        load_secret_provider(
            SecretProviderConfig(
                module="tests.support.secret_provider_fakes.FakeSecretProvider",
                config={"prefix": "local"},
                secrets={"token": {"env": "DAL_OBSCURA_MISSING_TOKEN"}},
            ),
            context=SecretProviderContext(
                database_url="sqlite+pysqlite:///:memory:",
                cell_id=UUID("00000000-0000-0000-0000-000000000001"),
            ),
        )


def test_load_secret_provider_reads_bootstrap_secret_from_file(tmp_path):
    token_file = tmp_path / "provider-token"
    token_file.write_text("file-token\n", encoding="utf-8")

    provider = load_secret_provider(
        SecretProviderConfig(
            module="tests.support.secret_provider_fakes.FakeSecretProvider",
            config={"prefix": "local"},
            secrets={"token": {"file": str(token_file)}},
        ),
        context=SecretProviderContext(
            database_url="sqlite+pysqlite:///:memory:",
            cell_id=UUID("00000000-0000-0000-0000-000000000001"),
        ),
    )

    assert provider.get_secret("catalog-password") == "local:file-token:catalog-password"


def test_resolve_secret_refs_uses_explicit_secret_shape_only():
    provider = load_secret_provider(
        SecretProviderConfig(
            module="tests.support.secret_provider_fakes.FakeSecretProvider",
            config={"prefix": "local"},
            secrets={"token": "bootstrap-token"},
        ),
        context=SecretProviderContext(
            database_url="sqlite+pysqlite:///:memory:",
            cell_id=UUID("00000000-0000-0000-0000-000000000001"),
        ),
    )

    resolved = resolve_secret_refs(
        {
            "jwt_secret": {"secret": "jwt-signing"},
            "legacy": {"key": "DAL_OBSCURA_JWT_SECRET"},
            "keys": [{"id": "svc", "secret": {"secret": "api-key"}}],
        },
        provider=provider,
    )

    assert resolved == {
        "jwt_secret": "local:bootstrap-token:jwt-signing",
        "legacy": {"key": "DAL_OBSCURA_JWT_SECRET"},
        "keys": [{"id": "svc", "secret": "local:bootstrap-token:api-key"}],
    }


def test_resolve_secret_refs_rejects_missing_secret():
    provider = load_secret_provider(
        SecretProviderConfig(
            module="tests.support.secret_provider_fakes.FakeSecretProvider",
            config={"prefix": "local"},
            secrets={"token": "bootstrap-token"},
        ),
        context=SecretProviderContext(
            database_url="sqlite+pysqlite:///:memory:",
            cell_id=UUID("00000000-0000-0000-0000-000000000001"),
        ),
    )

    with pytest.raises(ValueError, match="Secret 'missing' could not be resolved"):
        resolve_secret_refs({"jwt_secret": {"secret": "missing"}}, provider=provider)
