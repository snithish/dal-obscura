from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from time import sleep
from types import SimpleNamespace
from typing import Any

import pytest
from dal_obscura_iceberg_rest.catalog import RestCatalog, _snapshot_id
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1), correlation_id="rest-fixture"
    )


def _config(**options: object) -> CatalogConfig:
    return CatalogConfig(
        plugin_id="iceberg.rest",
        instance_id="fixture",
        revision=1,
        options={"uri": "https://catalog.example/v1", **options},
    )


def test_rest_catalog_context_cancellation_is_fail_closed():
    context = ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        correlation_id="rest-cancel",
        cancel_check=lambda: True,
    )
    with pytest.raises(InterruptedError, match="cancelled"):
        RestCatalog(_config(), context)


def test_rest_catalog_requests_receive_deadline_bounded_timeout() -> None:
    from dal_obscura_iceberg_rest import catalog as module

    calls: list[tuple[tuple[float, float], object | None, bool]] = []
    session = SimpleNamespace()

    def original_request(method: str, url: str, **kwargs: Any):
        calls.append((kwargs["timeout"], kwargs.get("headers"), kwargs["allow_redirects"]))
        return "ok"

    session.request = original_request
    module._install_request_timeout(session, 5.0, 30.0)
    context = ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(seconds=2), correlation_id="rest-timeout"
    )
    token = module._ACTIVE_REQUEST_BUDGET.set((context, 5.0, 30.0))
    try:
        assert session.request("GET", "https://catalog.example", timeout=999) == "ok"
    finally:
        module._ACTIVE_REQUEST_BUDGET.reset(token)

    connect, read = calls[0][0]
    assert 0 < connect <= 2
    assert 0 < read <= 2
    assert calls[0][2] is False


def test_rest_catalog_closes_response_when_cancelled_after_request() -> None:
    from dal_obscura_iceberg_rest import catalog as module

    closed: list[bool] = []

    class Response:
        def close(self):
            closed.append(True)

    session = SimpleNamespace()
    session.request = lambda method, url, **kwargs: Response()
    checks = 0

    def cancel_check() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 2

    context = replace(_context(), cancel_check=cancel_check)
    token = module._ACTIVE_REQUEST_BUDGET.set((context, 5.0, 30.0))
    try:
        module._install_request_timeout(session, 5.0, 30.0)
        with pytest.raises(InterruptedError, match="cancelled"):
            session.request("GET", "https://catalog.example")
    finally:
        module._ACTIVE_REQUEST_BUDGET.reset(token)

    assert closed == [True]


def test_rest_catalog_paginates_bounded_sorted_identifiers():
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self, namespace):
            assert namespace == ()
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
        def list_namespaces(self, namespace):
            assert namespace in ((), ("default",))
            return [("z",), ("default",), ("z",)]

    plugin._catalog = FakeCatalog()
    assert plugin.list_namespaces(_context()) == (("default",), ("z",))
    assert plugin.list_namespaces(_context(), namespace=("default",)) == (("default",),)


def test_rest_catalog_rejects_root_only_namespace_signature():
    plugin = RestCatalog(_config(), _context())

    class RootOnlyCatalog:
        def list_namespaces(self):
            return [("default",)]

    plugin._catalog = RootOnlyCatalog()
    with pytest.raises(TypeError):
        plugin.list_namespaces(_context())


def test_rest_catalog_rejects_non_string_provider_identifier_segments():
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self, namespace):
            del namespace
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
    handle = plugin.resolve_table(TableIdentifier(namespace=("default",), name="users"), _context())
    assert handle.metadata == {"metadata_location": "https://storage.example/metadata/v1.json"}


def test_rest_catalog_rechecks_cancellation_after_table_load() -> None:
    plugin = RestCatalog(_config(), _context())

    class FakeTable:
        metadata_location = "https://storage.example/metadata/v1.json"

    class FakeCatalog:
        def load_table(self, identifier):
            assert identifier == ("default", "users")
            return FakeTable()

    plugin._catalog = FakeCatalog()
    checks = 0

    def cancelled() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 3

    context = replace(_context(), cancel_check=cancelled)
    with pytest.raises(InterruptedError, match="cancelled"):
        plugin.resolve_table(TableIdentifier(namespace=("default",), name="users"), context)


def test_snapshot_id_supports_pyiceberg_method_shape() -> None:
    class FakeSnapshot:
        snapshot_id = 42

    class FakeTable:
        def current_snapshot(self):
            return FakeSnapshot()

    assert _snapshot_id(FakeTable()) == "42"


def test_rest_catalog_bounds_namespace_listing():
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self, namespace):
            del namespace
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


def test_rest_catalog_close_attempts_session_when_provider_close_fails() -> None:
    closed: list[str] = []
    plugin = RestCatalog(_config(), _context())

    class Session:
        def close(self):
            closed.append("session")

    class FailingCatalog:
        _session = Session()

        def close(self):
            closed.append("catalog")
            raise RuntimeError("catalog close failed")

    plugin._catalog = FailingCatalog()
    with pytest.raises(RuntimeError, match="catalog close failed"):
        plugin.close()

    assert closed == ["catalog", "session"]
    with pytest.raises(ValueError, match="closed"):
        plugin.list_namespaces(_context())


@pytest.mark.parametrize(
    "options,message",
    [
        pytest.param(
            {"uri": "https://user:pass@catalog.example/v1"}, "credentials", id="uri-credentials"
        ),
        pytest.param({"script": "import os"}, "unsupported keys", id="unknown-option"),
        pytest.param(
            {"uri": "http://catalog.example/v1", "token": "resolved"}, "HTTPS", id="insecure-token"
        ),
        pytest.param(
            {"warehouse": "s3://user:pass@bucket/warehouse"},
            "warehouse",
            id="warehouse-credentials",
        ),
        pytest.param(
            {"oauth2-server-uri": "http://issuer.example/token"},
            "oauth2-server-uri",
            id="insecure-oauth",
        ),
        pytest.param(
            {"token": {"secret": "REST_TOKEN"}}, "must be strings", id="unresolved-secret"
        ),
        pytest.param({"connect-timeout-ms": "0"}, "timeout", id="zero-connect-timeout"),
        pytest.param(
            {"connect-timeout-ms": "not-a-number"}, "timeout", id="invalid-connect-timeout"
        ),
        pytest.param({"read-timeout-ms": "0"}, "timeout", id="zero-read-timeout"),
        pytest.param({"read-timeout-ms": "not-a-number"}, "timeout", id="invalid-read-timeout"),
    ],
)
def test_rest_catalog_rejects_invalid_configuration(options, message):
    with pytest.raises(ValueError, match=message):
        RestCatalog(_config(**options), _context())
