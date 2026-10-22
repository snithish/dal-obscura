from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from io import BytesIO
from threading import Barrier, Event

import pytest
from dal_obscura_iceberg_rest.catalog import RestCatalog
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier
from requests import Response as RequestsResponse
from requests import Session as RequestsSession


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(UTC) + timedelta(minutes=1), correlation_id="rest-fixture"
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
        deadline=datetime.now(UTC) + timedelta(minutes=1),
        correlation_id="rest-cancel",
        cancel_check=lambda: True,
    )
    with pytest.raises(InterruptedError, match="cancelled"):
        RestCatalog(_config(), context)


def test_rest_catalog_requests_receive_deadline_bounded_timeout(monkeypatch):
    calls = []

    def request(self, method, url, **kwargs):
        calls.append((url, kwargs["timeout"], kwargs["allow_redirects"]))
        return response(
            {"defaults": {}, "overrides": {}}
            if url.endswith("/config")
            else {"namespaces": [["default"]]}
        )

    monkeypatch.setattr(RequestsSession, "request", request)
    operation = ExecutionContext(datetime.now(UTC) + timedelta(seconds=2), "rest-timeout")
    plugin = RestCatalog(_config(), operation)
    try:
        assert plugin.list_namespaces(operation) == (("default",),)
    finally:
        plugin.close()
    assert any(url.endswith("/namespaces") for url, _, _ in calls)
    for _, (connect, read), redirects in calls:
        assert 0 < connect <= 2
        assert 0 < read <= 2
        assert redirects is False


def test_rest_catalog_closes_response_when_cancelled_after_request(monkeypatch):
    cancelled = Event()
    closed = []

    class Response(RequestsResponse):
        def close(self):
            closed.append(True)
            super().close()

    def request(self, method, url, **kwargs):
        if url.endswith("/config"):
            return response({"defaults": {}, "overrides": {}})
        cancelled.set()
        value = Response()
        value._content = b'{"namespaces":[]}'
        value.raw = BytesIO(value._content)
        return value

    monkeypatch.setattr(RequestsSession, "request", request)
    operation = replace(_context(), cancel_check=cancelled.is_set)
    plugin = RestCatalog(_config(), operation)
    try:
        with pytest.raises(InterruptedError, match="cancelled"):
            plugin.list_namespaces(operation)
    finally:
        plugin.close()
    assert closed == [True]


def test_rest_catalog_paginates_bounded_sorted_identifiers(monkeypatch):
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self, namespace):
            assert namespace == ()
            return [("default",)]

        def list_tables(self, namespace):
            assert namespace == ("default",)
            return [("default", "b"), ("default", "a")]

    install_provider(monkeypatch, FakeCatalog())
    first = plugin.list_tables(_context(), limit=1)
    second = plugin.list_tables(_context(), continuation=first.continuation, limit=1)
    assert [item.name for item in first.entries] == ["a"]
    assert [item.name for item in second.entries] == ["b"]
    assert second.continuation is None


def test_rest_catalog_exposes_validated_namespace_and_config_lifecycle(monkeypatch):
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self, namespace):
            assert namespace in ((), ("default",))
            return [("z",), ("default",), ("z",)]

    install_provider(monkeypatch, FakeCatalog())
    assert plugin.list_namespaces(_context()) == (("default",), ("z",))
    assert plugin.list_namespaces(_context(), namespace=("default",)) == (("default",),)


def test_rest_catalog_rejects_root_only_namespace_signature(monkeypatch):
    plugin = RestCatalog(_config(), _context())

    class RootOnlyCatalog:
        def list_namespaces(self):
            return [("default",)]

    install_provider(monkeypatch, RootOnlyCatalog())
    with pytest.raises(TypeError):
        plugin.list_namespaces(_context())


def test_rest_catalog_rejects_non_string_provider_identifier_segments(monkeypatch):
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return [("default",)]

        def list_tables(self, namespace):
            return [("default", 42)]

    install_provider(monkeypatch, FakeCatalog())
    with pytest.raises(ValueError, match="identifier"):
        plugin.list_tables(_context(), limit=10)


@pytest.mark.parametrize("snapshot_id", [42, None])
def test_rest_catalog_does_not_copy_provider_io_credentials_into_handle(snapshot_id, monkeypatch):
    plugin = RestCatalog(_config(), _context())

    class FakeSnapshot:
        snapshot_id = 42

    class FakeTable:
        metadata_location = "https://storage.example/metadata/v1.json"

        def current_snapshot(self):
            return FakeSnapshot() if snapshot_id is not None else None

        io = type("IO", (), {"properties": {"s3.access-key-id": "secret"}})()

    class FakeCatalog:
        def load_table(self, identifier):
            assert identifier == ("default", "users")
            return FakeTable()

    install_provider(monkeypatch, FakeCatalog())
    handle = plugin.resolve_table(TableIdentifier(namespace=("default",), name="users"), _context())
    assert handle.metadata == {"metadata_location": "https://storage.example/metadata/v1.json"}
    assert handle.snapshot_id == ("42" if snapshot_id is not None else None)


def test_rest_catalog_rechecks_cancellation_after_table_load(monkeypatch) -> None:
    plugin = RestCatalog(_config(), _context())
    cancelled = Event()

    class FakeTable:
        metadata_location = "https://storage.example/metadata/v1.json"

    class FakeCatalog:
        def load_table(self, identifier):
            assert identifier == ("default", "users")
            cancelled.set()
            return FakeTable()

    install_provider(monkeypatch, FakeCatalog())
    context = replace(_context(), cancel_check=cancelled.is_set)
    with pytest.raises(InterruptedError, match="cancelled"):
        plugin.resolve_table(TableIdentifier(namespace=("default",), name="users"), context)


def test_rest_catalog_bounds_namespace_listing(monkeypatch):
    plugin = RestCatalog(_config(), _context())

    class FakeCatalog:
        def list_namespaces(self, namespace):
            del namespace
            yield from (("ns", str(index)) for index in range(10_001))

        def list_tables(self, namespace):
            return []

    install_provider(monkeypatch, FakeCatalog())
    with pytest.raises(ValueError, match="namespaces"):
        plugin.list_tables(_context(), limit=10)


def test_rest_catalog_initializes_provider_once_under_concurrency(monkeypatch):
    plugin = RestCatalog(_config(), _context())
    entered, release = Event(), Event()
    ready = Barrier(8)
    created = []

    class Provider:
        def close(self):
            pass

        def list_namespaces(self, namespace):
            return [("default",)]

    def create(*args, **kwargs):
        created.append(Provider())
        entered.set()
        assert release.wait(5), "Provider release was not signalled"
        return created[-1]

    class ProviderRestCatalog:
        def __new__(cls, *args, **kwargs):
            return create(*args, **kwargs)

    monkeypatch.setattr(
        "dal_obscura_iceberg_rest.catalog.PyIcebergRestCatalog", ProviderRestCatalog
    )

    def list_namespaces():
        ready.wait(timeout=5)
        return plugin.list_namespaces(_context())

    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(list_namespaces) for _ in range(8)]
            try:
                assert entered.wait(5)
            finally:
                release.set()
            assert [future.result(timeout=5) for future in futures] == [(("default",),)] * 8
        assert len(created) == 1
    finally:
        release.set()
        plugin.close()


@pytest.mark.parametrize("close_fails", [False, True])
def test_rest_catalog_closes_owned_http_session_once_and_is_terminal(monkeypatch, close_fails):
    closed = []
    requested = []
    owned = None
    original_close = RequestsSession.close

    def close(session):
        closed.append(session)
        original_close(session)
        if session is owned and close_fails:
            raise RuntimeError("catalog close failed")

    def request(session, method, url, **kwargs):
        requested.append(session)
        return response(
            {"defaults": {}, "overrides": {}} if url.endswith("/config") else {"namespaces": []}
        )

    monkeypatch.setattr(RequestsSession, "close", close)
    monkeypatch.setattr(RequestsSession, "request", request)
    plugin = RestCatalog(_config(), _context())
    assert plugin.list_namespaces(_context()) == ()
    owned = requested[-1]
    assert owned not in closed
    if close_fails:
        with pytest.raises(RuntimeError, match="catalog close failed"):
            plugin.close()
    else:
        plugin.close()
    plugin.close()
    assert closed.count(owned) == 1
    assert len(closed) == len(set(closed))
    with pytest.raises(ValueError, match="closed"):
        plugin.list_tables(_context(), limit=1)


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


def install_provider(monkeypatch, provider):
    class ProviderRestCatalog:
        def __init__(self, *args, **kwargs):
            pass

        def list_namespaces(self, namespace):
            return (
                provider.list_namespaces(namespace) if hasattr(provider, "list_namespaces") else ()
            )

        def list_tables(self, namespace):
            return provider.list_tables(namespace)

        def load_table(self, identifier):
            return provider.load_table(identifier)

        def close(self):
            if hasattr(provider, "close"):
                provider.close()

    monkeypatch.setattr(
        "dal_obscura_iceberg_rest.catalog.PyIcebergRestCatalog", ProviderRestCatalog
    )


def response(payload):
    value = RequestsResponse()
    value.status_code = 200
    value._content = json.dumps(payload).encode()
    return value
