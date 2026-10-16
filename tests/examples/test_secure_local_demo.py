from __future__ import annotations

from pathlib import Path

import pytest
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID


def _module(path: str):
    import importlib.util

    spec = importlib.util.spec_from_file_location("secure_demo", Path(path))
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _cli():
    return _module("deployment/local-secure/scripts/demo.py")


def test_secure_configuration_is_private_stable_and_distinct(tmp_path):
    cli = _cli()
    first = cli.initialize(tmp_path, {})
    assert cli.initialize(tmp_path, {}) == first
    assert first["PROJECT_NAME"].startswith("dal-obscura-secure-local-")
    assert cli.public_urls(first) == {
        "ui": "https://localhost:28443",
        "issuer": "https://localhost:24443/realms/dal-obscura-demo",
        "flight": "grpc+tls://localhost:28815",
    }
    assert (tmp_path / ".env").stat().st_mode & 0o777 == 0o600
    assert len({first[key] for key in cli.SECRET_KEYS}) == len(cli.SECRET_KEYS)


def test_tls_generation_is_atomic_stable_private_and_correctly_scoped(tmp_path):
    cli = _cli()
    cli.ensure_tls(tmp_path)
    directory = tmp_path / ".tls"
    before = {path.name: path.read_bytes() for path in directory.iterdir()}
    cli.ensure_tls(tmp_path)
    assert {path.name: path.read_bytes() for path in directory.iterdir()} == before
    assert directory.stat().st_mode & 0o777 == 0o700
    keys = [path for path in directory.iterdir() if path.suffix == ".key"]
    assert len({path.read_bytes() for path in keys}) == len(keys)
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in keys)
    keycloak = x509.load_pem_x509_certificate((directory / "keycloak.crt").read_bytes())
    names = keycloak.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert set(names.get_values_for_type(x509.DNSName)) == {"localhost", "keycloak"}
    client = x509.load_pem_x509_certificate((directory / "client.crt").read_bytes())
    eku = client.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    assert ExtendedKeyUsageOID.CLIENT_AUTH in eku
    assert ExtendedKeyUsageOID.SERVER_AUTH not in eku


def test_incomplete_tls_never_rotates_existing_credentials(tmp_path):
    cli = _cli()
    directory = tmp_path / ".tls"
    directory.mkdir()
    (directory / "ca.key").write_text("preserve")
    with pytest.raises(ValueError, match="incomplete"):
        cli.ensure_tls(tmp_path)
    assert (directory / "ca.key").read_text() == "preserve"


def test_failed_certificate_generation_leaves_no_partial_identity(tmp_path, monkeypatch):
    cli = _cli()

    def fail(*args, **kwargs):
        raise RuntimeError("injected OpenSSL failure")

    monkeypatch.setattr("subprocess.run", fail)
    with pytest.raises(RuntimeError, match="injected"):
        cli.ensure_tls(tmp_path)
    assert not (tmp_path / ".tls").exists()


def test_mismatched_server_key_fails_before_startup(tmp_path):
    cli = _cli()
    cli.ensure_tls(tmp_path)
    directory = tmp_path / ".tls"
    (directory / "flight.key").write_bytes((directory / "client.key").read_bytes())
    with pytest.raises(ValueError, match=r"key.*certificate"):
        cli.validate_tls(tmp_path)


def test_failed_reset_preserves_ca_and_credentials(demo_process):
    demo = demo_process(secure=True, failure=True)
    _cli().initialize(demo.root, {})
    directory = demo.root / ".tls"
    directory.mkdir()
    (directory / "ca.key").write_text("preserve")
    before = (demo.root / ".env").read_bytes()
    assert demo.run("reset").returncode == 1
    assert (demo.root / ".env").read_bytes() == before
    assert (directory / "ca.key").read_text() == "preserve"


def test_reset_can_recover_incomplete_configuration_without_other_projects(demo_process):
    demo = demo_process(secure=True)
    (demo.root / ".env").write_text("ADMIN_TOKEN=interrupted\n")
    (demo.root / ".tls").mkdir()
    result = demo.run("reset")
    assert result.returncode == 0, result.stderr
    command = demo.commands()[0]
    assert command[-3:] == ["down", "--volumes", "--remove-orphans"]
    assert command[command.index("--project-name") + 1].startswith("dal-obscura-secure-local-")
    assert not (demo.root / ".env").exists()
    assert not (demo.root / ".tls").exists()


def test_failed_tls_cleanup_keeps_reset_credentials_for_retry(demo_process, monkeypatch):
    demo = demo_process(secure=True)
    cli = _module(str(demo.root / "scripts/demo.py"))
    cli.initialize(demo.root, {})
    (demo.root / ".tls").mkdir()
    before = (demo.root / ".env").read_bytes()
    monkeypatch.setenv("PATH", demo.environment["PATH"])
    monkeypatch.setenv("TEST_DOCKER_LOG", demo.environment["TEST_DOCKER_LOG"])

    def fail(*args, **kwargs):
        raise PermissionError("TLS cleanup failed")

    monkeypatch.setattr(cli.shutil, "rmtree", fail)
    assert cli.main(["reset"]) == 1
    assert (demo.root / ".env").read_bytes() == before


def test_reset_unlinks_tls_symlink_without_deleting_target(demo_process, tmp_path):
    demo = demo_process(secure=True)
    _cli().initialize(demo.root, {})
    outside = tmp_path / "other-identity"
    outside.mkdir()
    (outside / "ca.key").write_text("preserve")
    (demo.root / ".tls").symlink_to(outside, target_is_directory=True)
    result = demo.run("reset")
    assert result.returncode == 0, result.stderr
    assert not (demo.root / ".tls").exists()
    assert (outside / "ca.key").read_text() == "preserve"


def test_mtls_check_does_not_treat_unreachable_service_as_certificate_rejection(
    tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(str(Path("examples/demo/keycloak/scripts").resolve()))
    checker = _module("deployment/local-secure/scripts/check_secure.py")
    ca = tmp_path / "ca.crt"
    ca.write_bytes(b"fixture")
    monkeypatch.setenv("FLIGHT_URI", "grpc+tls://127.0.0.1:1")
    monkeypatch.setattr(checker.check_demo, "token", lambda _: "owner-token")

    def unavailable(*args, **kwargs):
        raise ConnectionRefusedError("service unreachable")

    monkeypatch.setattr(checker.flight, "FlightClient", unavailable)
    with pytest.raises(ConnectionRefusedError, match="unreachable"):
        checker.verify_mtls_rejection("missing client certificate", ca=str(ca))
