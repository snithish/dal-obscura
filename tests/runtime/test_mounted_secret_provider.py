"""Mounted secret resolution through startup configuration and scoped references."""

import json
import os

import pytest

from dal_obscura.runtime.secrets import load_secret_provider_from_environment, resolve_secret_refs


@pytest.fixture
def mounted_provider(tmp_path):
    directory = tmp_path / "secrets"
    directory.mkdir()

    def create(**extra):
        return load_secret_provider_from_environment(
            {
                "DAL_OBSCURA_SECRET_PROVIDER_KIND": "mounted_file",
                "DAL_OBSCURA_SECRET_PROVIDER_CONFIG": json.dumps(
                    {
                        "directory": str(directory),
                        "scope_grants": {"catalog:analytics": ["TOKEN"]},
                        **extra,
                    }
                ),
            },
            require_scope_grants=True,
        )

    return directory, create


def test_mounted_secret_rotates_atomically_without_copying_value_into_reference(mounted_provider):
    directory, create = mounted_provider
    (directory / "TOKEN").write_text("first-value\n")
    provider = create()
    reference = {"secret": "TOKEN", "scope": "catalog:analytics"}
    assert (
        resolve_secret_refs(reference, provider=provider, expected_scope="catalog:analytics")
        == "first-value\n"
    )
    replacement = directory / "replacement"
    replacement.write_text("second-value")
    replacement.replace(directory / "TOKEN")
    assert (
        resolve_secret_refs(reference, provider=provider, expected_scope="catalog:analytics")
        == "second-value"
    )
    assert reference == {"secret": "TOKEN", "scope": "catalog:analytics"}


def test_mounted_secret_wrong_scope_never_reads_provider(mounted_provider, monkeypatch):
    _, create = mounted_provider
    provider = create()
    monkeypatch.setattr(provider, "get_secret", lambda key: pytest.fail("Unauthorized file read"))
    with pytest.raises(ValueError, match="does not match"):
        resolve_secret_refs(
            {"secret": "TOKEN", "scope": "catalog:analytics"},
            provider=provider,
            expected_scope="catalog:other",
        )


@pytest.mark.parametrize("name", ["../outside", "/absolute", ".", "..", "nested/token", "x\\token"])
def test_mounted_secret_never_resolves_path_names(mounted_provider, name):
    _, create = mounted_provider
    provider = create()
    assert provider.get_secret(name) is None
    granted = create(scope_grants={"catalog:analytics": [name]})
    with pytest.raises(ValueError, match="could not be resolved"):
        resolve_secret_refs(
            {"secret": name, "scope": "catalog:analytics"},
            provider=granted,
            expected_scope="catalog:analytics",
        )


@pytest.mark.parametrize("payload", [b"", b"\xff", b"x" * (64 * 1024 + 1)])
def test_mounted_secret_rejects_empty_invalid_or_oversized_values(mounted_provider, payload):
    directory, create = mounted_provider
    (directory / "TOKEN").write_bytes(payload)
    provider = create()
    with pytest.raises(ValueError, match="could not be resolved") as error:
        resolve_secret_refs(
            {"secret": "TOKEN", "scope": "catalog:analytics"},
            provider=provider,
            expected_scope="catalog:analytics",
        )
    assert "xxxxxxxx" not in str(error.value)


def test_secret_provider_rejects_unknown_kind():
    with pytest.raises(ValueError, match="SECRET_PROVIDER_KIND"):
        load_secret_provider_from_environment(
            {"DAL_OBSCURA_SECRET_PROVIDER_KIND": "untrusted.module.Provider"}
        )


def test_mounted_secret_io_failure_closes_owned_descriptors(mounted_provider, monkeypatch):
    directory, create = mounted_provider
    (directory / "TOKEN").write_text("sentinel-secret")
    provider = create()
    opened = []
    original = os.open

    def track(*args, **kwargs):
        descriptor = original(*args, **kwargs)
        opened.append(descriptor)
        return descriptor

    def failed_read(*args):
        raise OSError("sentinel-secret")

    monkeypatch.setattr(os, "open", track)
    monkeypatch.setattr(os, "read", failed_read)
    with pytest.raises(ValueError, match="could not be resolved") as error:
        resolve_secret_refs(
            {"secret": "TOKEN", "scope": "catalog:analytics"},
            provider=provider,
            expected_scope="catalog:analytics",
        )
    assert "sentinel-secret" not in str(error.value)
    assert len(opened) == 2
    for descriptor in opened:
        with pytest.raises(OSError):
            os.fstat(descriptor)
