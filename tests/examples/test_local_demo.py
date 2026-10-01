from __future__ import annotations

import importlib
import json
import socket
from pathlib import Path

import pytest


def _cli():
    return importlib.import_module("examples.demo.keycloak.scripts.demo")


def test_configuration_is_stable_private_and_has_one_browser_identity(tmp_path):
    cli = _cli()
    first = cli.initialize_config(tmp_path, {})
    second = cli.initialize_config(tmp_path, {})
    assert first == second
    assert (tmp_path / ".env").stat().st_mode & 0o777 == 0o600
    assert cli.public_urls(first) == {
        "ui": "http://localhost:28821",
        "issuer": "http://localhost:20080/realms/dal-obscura-demo",
        "flight": "grpc+tcp://localhost:28115",
    }
    assert len(first["ADMIN_TOKEN"]) >= 32
    assert first["APP_DB_PASSWORD"] != first["ICEBERG_DB_PASSWORD"]


@pytest.mark.parametrize("port", ["0", "65536", "bad", "28821\nOTHER=bad"])
def test_invalid_port_never_writes_configuration(tmp_path, port):
    with pytest.raises(ValueError, match="port"):
        _cli().initialize_config(tmp_path, {"UI_PORT": port})
    assert not (tmp_path / ".env").exists()


def test_duplicate_ports_rejected(tmp_path):
    with pytest.raises(ValueError, match="distinct"):
        _cli().initialize_config(tmp_path, {"UI_PORT": "20080"})


def test_incomplete_existing_configuration_never_rotates_secrets(tmp_path):
    path = tmp_path / ".env"
    path.write_text("ADMIN_TOKEN=keep-this-value\n")
    with pytest.raises(ValueError, match="incomplete"):
        _cli().initialize_config(tmp_path, {})
    assert path.read_text() == "ADMIN_TOKEN=keep-this-value\n"


def test_changed_port_requires_explicit_reconfiguration(tmp_path):
    cli = _cli()
    initial = cli.initialize_config(tmp_path, {})
    with pytest.raises(ValueError, match="already configured"):
        cli.initialize_config(tmp_path, {"UI_PORT": "28822"})
    assert cli.read_config(tmp_path) == initial


def test_missing_configuration_gives_initialization_command(tmp_path):
    with pytest.raises(ValueError, match="demo init"):
        _cli().read_config(tmp_path)


@pytest.mark.socket
def test_reachable_but_unrelated_service_is_a_port_conflict():
    cli = _cli()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        with pytest.raises(ValueError, match=str(port)):
            cli.check_ports({"UI_PORT": str(port)}, set())
        cli.check_ports({"UI_PORT": str(port)}, {"UI_PORT"})


@pytest.mark.socket
def test_closed_connection_does_not_make_a_free_port_look_occupied():
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen()
        port = server.getsockname()[1]
        with socket.create_connection(("127.0.0.1", port)) as client:
            accepted, _ = server.accept()
            accepted.close()
            assert client.recv(1) == b""
    _cli().check_ports({"UI_PORT": str(port)}, set())


def test_concurrent_start_fails_without_touching_state(tmp_path):
    cli = _cli()
    with (
        cli.operation_lock(tmp_path),
        pytest.raises(ValueError, match="another demo command"),
        cli.operation_lock(tmp_path),
    ):
        pytest.fail("second operation must not acquire lock")


def test_start_waits_for_runtime_to_release_a_recently_stopped_port(monkeypatch):
    cli = _cli()
    attempts = []

    class Probe:
        def setsockopt(self, *_):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def bind(self, address):
            attempts.append(address)
            if len(attempts) == 1:
                raise OSError("runtime proxy is shutting down")

    monkeypatch.setattr(cli.socket, "socket", Probe)
    monkeypatch.setattr(cli, "sleep", lambda _: None, raising=False)
    cli.check_ports({"UI_PORT": "28821"}, set(), timeout=5)
    assert attempts == [("127.0.0.1", 28821)] * 2


def test_secret_redaction_includes_generated_credentials(tmp_path):
    cli = _cli()
    values = cli.initialize_config(tmp_path, {})
    redacted = cli.redact(
        f"url?password={values['APP_DB_PASSWORD']} {values['ADMIN_TOKEN']}", values
    )
    assert values["APP_DB_PASSWORD"] not in redacted
    assert values["ADMIN_TOKEN"] not in redacted


def test_iceberg_seed_is_atomic_multifile_and_preserves_edits(tmp_path, monkeypatch):
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog

    seed = importlib.import_module("examples.demo.keycloak.scripts.seed_table")
    options = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": str(tmp_path / "warehouse"),
    }
    monkeypatch.setattr(seed, "_catalog_options", lambda: options)
    fixture = json.loads(Path("examples/demo/keycloak/fixtures/demo_fixture.json").read_text())[
        "tables"
    ][0]
    seed._create_iceberg_table(fixture)
    catalog = load_catalog("retail_demo", **options)
    table = catalog.load_table(fixture["target"])
    assert len(list(table.scan().plan_files())) == 2
    _, schema = seed._schemas(fixture["schema"])
    table.append(pa.Table.from_pylist([fixture["rows"][0] | {"customer_id": 3001}], schema=schema))
    seed._create_iceberg_table(fixture)
    assert catalog.load_table(fixture["target"]).scan().to_arrow().num_rows == 5


def test_failed_iceberg_seed_does_not_publish_partial_table(tmp_path, monkeypatch):
    from pyiceberg.catalog import load_catalog
    from pyiceberg.table import Transaction

    seed = importlib.import_module("examples.demo.keycloak.scripts.seed_table")
    options = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": str(tmp_path / "warehouse"),
    }
    monkeypatch.setattr(seed, "_catalog_options", lambda: options)
    fixture = json.loads(Path("examples/demo/keycloak/fixtures/demo_fixture.json").read_text())[
        "tables"
    ][0]
    original = Transaction.append
    calls = 0

    def interrupted(self, batch, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected interruption")
        return original(self, batch, *args, **kwargs)

    monkeypatch.setattr(Transaction, "append", interrupted)
    with pytest.raises(RuntimeError, match="injected interruption"):
        seed._create_iceberg_table(fixture)
    assert not load_catalog("retail_demo", **options).table_exists(fixture["target"])
    monkeypatch.setattr(Transaction, "append", original)
    seed._create_iceberg_table(fixture)
    assert (
        load_catalog("retail_demo", **options)
        .load_table(fixture["target"])
        .scan()
        .to_arrow()
        .num_rows
        == 4
    )


def test_failed_reset_preserves_credentials(tmp_path, monkeypatch):
    cli = _cli()
    cli.initialize_config(tmp_path, {})
    original = (tmp_path / ".env").read_bytes()
    monkeypatch.setattr(cli, "ROOT", tmp_path)

    def unavailable(*args, **kwargs):
        raise RuntimeError("engine unavailable")

    monkeypatch.setattr(cli, "_compose", unavailable)
    assert cli.main(["reset"]) == 1
    assert (tmp_path / ".env").read_bytes() == original


def test_reset_can_recover_incomplete_configuration(tmp_path, monkeypatch):
    cli = _cli()
    (tmp_path / ".env").write_text("ADMIN_TOKEN=interrupted\n")
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    monkeypatch.setattr(cli, "_compose", lambda *args, **kwargs: "")
    assert cli.main(["reset"]) == 0
    assert not (tmp_path / ".env").exists()
