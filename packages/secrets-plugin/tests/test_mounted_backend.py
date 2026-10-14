"""Mounted-file safety and resource ownership through the backend factory."""

import os
from datetime import UTC, datetime, timedelta

import pytest
from dal_obscura_plugin_api import ExecutionContext
from dal_obscura_secrets import mounted_file_factory


@pytest.fixture
def mounted_provider(tmp_path):
    directory = tmp_path / "secrets"
    directory.mkdir()

    def create(**extra):
        context = ExecutionContext(datetime.now(UTC) + timedelta(seconds=30), "mounted-backend")
        return mounted_file_factory({"directory": str(directory), **extra}, context)

    return directory, create


@pytest.mark.parametrize("kind", ["symbolic-link", "hard-link", "directory", "fifo"])
def test_mounted_secret_rejects_links_and_nonregular_files(mounted_provider, tmp_path, kind):
    directory, create = mounted_provider
    outside = tmp_path / "outside"
    outside.write_text("outside-secret")
    secret = directory / "TOKEN"
    if kind == "symbolic-link":
        secret.symlink_to(outside)
    elif kind == "hard-link":
        os.link(outside, secret)
    elif kind == "directory":
        secret.mkdir()
    else:
        os.mkfifo(secret)
    assert create().get_secret("TOKEN") is None


def test_mounted_secret_accepts_exact_utf8_byte_budget(mounted_provider):
    directory, create = mounted_provider
    value = "é" * (32 * 1024)
    (directory / "TOKEN").write_text(value)
    assert create().get_secret("TOKEN") == value


def test_mounted_secret_rejects_replaced_root_directory(mounted_provider):
    directory, create = mounted_provider
    (directory / "TOKEN").write_text("approved-secret")
    provider = create()
    directory.rename(directory.with_name("original"))
    directory.mkdir()
    (directory / "TOKEN").write_text("unapproved-secret")
    assert provider.get_secret("TOKEN") is None


@pytest.mark.parametrize(
    "payload",
    [b"value", b"\xff", b"", b"x" * (64 * 1024 + 1)],
    ids=["success", "invalid-utf8", "empty", "oversized"],
)
def test_mounted_secret_closes_file_and_directory_descriptors(
    mounted_provider, monkeypatch, payload
):
    directory, create = mounted_provider
    (directory / "TOKEN").write_bytes(payload)
    provider = create()
    opened = []
    original = os.open

    def track(*args, **kwargs):
        descriptor = original(*args, **kwargs)
        opened.append(descriptor)
        return descriptor

    monkeypatch.setattr(os, "open", track)
    assert provider.get_secret("TOKEN") == ("value" if payload == b"value" else None)
    assert len(opened) == 2
    for descriptor in opened:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_mounted_secret_rejects_symlink_directory(mounted_provider, tmp_path):
    directory, create = mounted_provider
    linked = tmp_path / "linked"
    linked.symlink_to(directory, target_is_directory=True)
    with pytest.raises(ValueError, match="directory is unavailable"):
        create(directory=str(linked))


@pytest.mark.parametrize(
    "config",
    [
        {"directory": "relative"},
        {"directory": None},
        {"directory": "/missing/dal-secrets"},
        {"secrets": {"TOKEN": "inline-secret"}},
    ],
)
def test_mounted_secret_rejects_invalid_startup_configuration(mounted_provider, config):
    _, create = mounted_provider
    with pytest.raises(ValueError):
        create(**config)


def test_mounted_secret_reads_complete_utf8_across_short_reads(mounted_provider, monkeypatch):
    directory, create = mounted_provider
    value = "é-secret-value\n"
    (directory / "TOKEN").write_text(value)
    provider = create()
    original = os.read
    monkeypatch.setattr(os, "read", lambda descriptor, size: original(descriptor, min(size, 7)))
    assert provider.get_secret("TOKEN") == value
