from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from types import SimpleNamespace

import pytest
from dal_obscura_iceberg_rest.catalog import RestCatalog
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier


def test_static_descriptor_uses_the_public_admission_shape() -> None:
    descriptor_path = (
        Path(__file__).parents[1] / "src" / "dal_obscura_iceberg_rest" / "dal_obscura-plugin.json"
    )
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    assert descriptor["plugin_id"] == "iceberg.rest"
    assert "distribution" not in descriptor
    assert "version" not in descriptor


def test_static_descriptor_advertises_all_supported_rest_auth_options() -> None:
    descriptor_path = (
        Path(__file__).parents[1] / "src" / "dal_obscura_iceberg_rest" / "dal_obscura-plugin.json"
    )
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    fields = {field["name"]: field for field in descriptor["config_schema"]["fields"]}

    assert fields["scope"]["type"] == "string"
    assert fields["oauth2-server-uri"] == {
        "name": "oauth2-server-uri",
        "type": "uri",
        "required": False,
        "secret": False,
    }
    assert fields["connect-timeout-ms"]["type"] == "integer"
    assert fields["read-timeout-ms"]["type"] == "integer"


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        correlation_id="rest-fixture",
    )


def _config(**options: object) -> CatalogConfig:
    return CatalogConfig(
        plugin_id="iceberg.rest",
        instance_id="fixture",
        revision=1,
        options={"uri": "https://catalog.example/v1", **options},
    )


def test_rest_catalog_rejects_credential_bearing_uri():
    with pytest.raises(ValueError, match="credentials"):
        RestCatalog(
            _config(uri="https://user:pass@catalog.example/v1"),
            _context(),
        )


def test_rest_catalog_rejects_unsupported_options():
    with pytest.raises(ValueError, match="unsupported keys"):
        RestCatalog(_config(script="import os"), _context())


def test_rest_catalog_rejects_credentials_over_plain_http():
    with pytest.raises(ValueError, match="HTTPS"):
        RestCatalog(_config(uri="http://catalog.example/v1", token="resolved"), _context())


def test_rest_catalog_rejects_credential_bearing_auxiliary_uris():
    with pytest.raises(ValueError, match="warehouse"):
        RestCatalog(_config(warehouse="s3://user:pass@bucket/warehouse"), _context())
    with pytest.raises(ValueError, match="oauth2-server-uri"):
        RestCatalog(_config(**{"oauth2-server-uri": "http://issuer.example/token"}), _context())


def test_rest_catalog_rejects_unresolved_secret_objects():
    with pytest.raises(ValueError, match="must be strings"):
        RestCatalog(_config(token={"secret": "REST_TOKEN"}), _context())


@pytest.mark.parametrize(
    "option",
    ["connect-timeout-ms", "read-timeout-ms"],
)
def test_rest_catalog_rejects_invalid_timeouts(option: str) -> None:
    with pytest.raises(ValueError, match="timeout"):
        RestCatalog(_config(**{option: "0"}), _context())
    with pytest.raises(ValueError, match="timeout"):
        RestCatalog(_config(**{option: "not-a-number"}), _context())


def test_rest_catalog_context_cancellation_is_fail_closed():
    context = ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        correlation_id="rest-cancel",
        cancel_check=lambda: True,
    )
    with pytest.raises(ValueError, match="cancelled"):
        RestCatalog(_config(), context)


def test_rest_catalog_requests_receive_deadline_bounded_timeout() -> None:
    from dal_obscura_iceberg_rest import catalog as module

    calls: list[tuple[object, object]] = []
    session = SimpleNamespace()

    def original_request(method, url, **kwargs):
        calls.append((kwargs["timeout"], kwargs.get("headers"), kwargs["allow_redirects"]))
        return "ok"

    session.request = original_request
    module._install_request_timeout(session, 5.0, 30.0)
    context = ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(seconds=2),
        correlation_id="rest-timeout",
    )
    token = module._ACTIVE_REQUEST_BUDGET.set((context.deadline, None, 5.0, 30.0))
    try:
        assert session.request("GET", "https://catalog.example", timeout=999) == "ok"
    finally:
        module._ACTIVE_REQUEST_BUDGET.reset(token)

    connect, read = calls[0][0]
    assert 0 < connect <= 2
    assert 0 < read <= 2
    assert calls[0][2] is False


def test_rest_catalog_paginates_bounded_sorted_identifiers():
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self):
            return [("default",)]

        def list_tables(self, namespace):
            assert namespace == ("default",)
            return [("default", "b"), ("default", "a")]

    plugin._catalog = FakeCatalog()
    first = plugin.list_tables(_context(), limit=1)
    second = plugin.list_tables(_context(), continuation=first.continuation, limit=1)
    assert [item.name for item in first.entries] == ["a"]
    assert [item.name for item in second.entries] == ["b"]
    assert second.continuation is None


def test_rest_catalog_exposes_validated_namespace_and_config_lifecycle():
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self):
            return [("z",), ("default",), ("z",)]

    plugin._catalog = FakeCatalog()
    plugin.validate_config(_context())
    assert plugin.list_namespaces(_context()) == (("default",), ("z",))
    assert plugin.list_namespaces(_context(), namespace=("default",)) == (("default",),)


def test_rest_catalog_rejects_non_string_provider_identifier_segments():
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self):
            return [("default",)]

        def list_tables(self, namespace):
            return [("default", 42)]

    plugin._catalog = FakeCatalog()
    with pytest.raises(ValueError, match="identifier"):
        plugin.list_tables(_context(), limit=10)


def test_rest_catalog_does_not_copy_provider_io_credentials_into_handle():
    plugin = RestCatalog(_config(), _context())

    class FakeSnapshot:
        snapshot_id = 42

    class FakeTable:
        metadata_location = "https://storage.example/metadata/v1.json"
        current_snapshot = FakeSnapshot()
        io = type("IO", (), {"properties": {"s3.access-key-id": "secret"}})()

    class FakeCatalog:
        def load_table(self, identifier):
            assert identifier == ("default", "users")
            return FakeTable()

    plugin._catalog = FakeCatalog()
    handle = plugin.resolve_table(
        TableIdentifier(namespace=("default",), name="users"),
        _context(),
    )
    assert handle.metadata == {"metadata_location": "https://storage.example/metadata/v1.json"}


def test_rest_catalog_bounds_namespace_listing():
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self):
            yield from (("ns", str(index)) for index in range(10_001))

        def list_tables(self, namespace):
            return []

    plugin._catalog = FakeCatalog()
    with pytest.raises(ValueError, match="namespaces"):
        plugin.list_tables(_context(), limit=10)


def test_rest_catalog_initializes_provider_once_under_concurrency(monkeypatch) -> None:
    plugin = RestCatalog(_config(), _context())
    calls = 0

    class FakeCatalog:
        pass

    def load_catalog(*args, **kwargs):
        nonlocal calls
        calls += 1
        sleep(0.01)
        return FakeCatalog()

    monkeypatch.setattr(
        "dal_obscura_iceberg_rest.catalog._create_catalog",
        lambda *args, **kwargs: load_catalog(*args, **kwargs),
    )
    with ThreadPoolExecutor(max_workers=8) as executor:
        values = list(executor.map(lambda _: plugin._load_catalog(_context()), range(8)))

    assert calls == 1
    assert all(value is values[0] for value in values)


def test_rest_catalog_close_releases_provider_session_and_is_terminal() -> None:
    plugin = RestCatalog(_config(), _context())
    closed = []

    class Session:
        def close(self):
            closed.append(True)

    class FakeCatalog:
        _session = Session()

    plugin._catalog = FakeCatalog()
    plugin.close()
    plugin.close()

    assert closed == [True]
    with pytest.raises(ValueError, match="closed"):
        plugin.list_tables(_context(), limit=1)
