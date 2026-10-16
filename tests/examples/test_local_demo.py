from __future__ import annotations

import importlib
import json
import socket
from contextlib import closing
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


@pytest.mark.socket
def test_start_waits_for_runtime_to_release_a_recently_stopped_port():
    import time
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    cli = _cli()
    release = Event()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]

        def stop_proxy():
            assert release.wait(5)
            time.sleep(0.2)
            listener.close()

        with ThreadPoolExecutor(max_workers=1) as workers:
            stopped = workers.submit(stop_proxy)
            release.set()
            cli.check_ports({"UI_PORT": str(port)}, set(), timeout=5)
            stopped.result(timeout=5)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))


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
    monkeypatch.setenv("ICEBERG_CATALOG_URI", options["uri"])
    monkeypatch.setenv("ICEBERG_WAREHOUSE", options["warehouse"])
    fixture = json.loads(Path("examples/demo/keycloak/fixtures/demo_fixture.json").read_text())[
        "tables"
    ][0]
    seed._create_iceberg_table(fixture)
    with closing(load_catalog("retail_demo", **options)) as catalog:
        table = catalog.load_table(fixture["target"])
        assert len(list(table.scan().plan_files())) == 2
        _, schema = seed._schemas(fixture["schema"])
        table.append(
            pa.Table.from_pylist([fixture["rows"][0] | {"customer_id": 3001}], schema=schema)
        )
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
    monkeypatch.setenv("ICEBERG_CATALOG_URI", options["uri"])
    monkeypatch.setenv("ICEBERG_WAREHOUSE", options["warehouse"])
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
    with closing(load_catalog("retail_demo", **options)) as catalog:
        assert not catalog.table_exists(fixture["target"])
    monkeypatch.setattr(Transaction, "append", original)
    seed._create_iceberg_table(fixture)
    with closing(load_catalog("retail_demo", **options)) as catalog:
        assert catalog.load_table(fixture["target"]).scan().to_arrow().num_rows == 4


def test_failed_reset_preserves_credentials(demo_process):
    demo = demo_process(failure=True)
    _cli().initialize_config(demo.root, {})
    original = (demo.root / ".env").read_bytes()
    result = demo.run("reset")
    assert result.returncode == 1
    assert (demo.root / ".env").read_bytes() == original
    assert "down" in demo.commands()[0]


def test_reset_can_recover_incomplete_configuration(demo_process):
    demo = demo_process()
    (demo.root / ".env").write_text("ADMIN_TOKEN=interrupted\n")
    result = demo.run("reset")
    assert result.returncode == 0, result.stderr
    assert not (demo.root / ".env").exists()
    assert "Local example reset" in result.stdout
    command = demo.commands()[0]
    assert command[-3:] == ["down", "--volumes", "--remove-orphans"]
    assert command[command.index("--project-name") + 1].startswith("dal-obscura-local-")
