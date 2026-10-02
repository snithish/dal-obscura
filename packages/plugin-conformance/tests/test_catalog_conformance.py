from __future__ import annotations

from datetime import datetime, timezone
from typing import cast

import pytest
from dal_obscura_plugin_api import (
    CatalogPlugin,
    DiscoveryPage,
    TableIdentifier,
)
from dal_obscura_plugin_conformance import (
    run_catalog_checks,
)
from plugin_conformance_fakes import (
    _catalog_context,
    _catalog_descriptor,
)


def test_catalog_runner_validates_bounded_discovery_and_coverage():
    users = TableIdentifier(namespace=("default",), name="users")
    orders = TableIdentifier(namespace=("default",), name="orders")

    class _Catalog:
        descriptor = _catalog_descriptor()

        def list_namespaces(self, context):
            del context
            return (("default",),)

        def close(self):
            return None

        def list_tables(self, context, *, continuation, limit):
            del context, limit
            if continuation is None:
                return DiscoveryPage((users,), continuation="next")
            return DiscoveryPage((orders,))

    result = run_catalog_checks(
        cast(CatalogPlugin, _Catalog()),
        _catalog_context(),
        expected_identifiers={users, orders},
        artifact_identity="sha256:catalog",
    )

    assert result.to_dict()["status"] == "passed"
    assert result.checks["bounded_discovery"] == "passed"
    assert result.checks["discovery_coverage"] == "passed"
    assert result.artifact_identity == "sha256:catalog"


def test_catalog_runner_requires_lifecycle_operations() -> None:
    class NoLifecycle:
        descriptor = _catalog_descriptor()

        def list_tables(self, context, *, continuation, limit):
            del context, continuation, limit
            return DiscoveryPage(())

        def close(self):
            return None

    result = run_catalog_checks(cast(CatalogPlugin, NoLifecycle()), _catalog_context())

    assert result.to_dict()["status"] == "failed"
    assert any("list_namespaces" in failure for failure in result.failures)


def test_catalog_runner_rejects_oversized_namespace_discovery() -> None:
    class _WideCatalog:
        descriptor = _catalog_descriptor()

        def list_namespaces(self, context):
            del context
            return (("ns-a",), ("ns-b",))

        def list_tables(self, context, *, continuation, limit):
            del context, continuation, limit
            return DiscoveryPage(())

        def close(self):
            return None

    result = run_catalog_checks(
        cast(CatalogPlugin, _WideCatalog()), _catalog_context(), max_namespaces=1
    )

    assert result.to_dict()["status"] == "failed"
    assert any("more than 1 namespaces" in failure for failure in result.failures)


def test_catalog_runner_validates_budgets_before_provider_calls() -> None:
    calls = []

    class _Catalog:
        descriptor = _catalog_descriptor()

        def list_namespaces(self, context):
            del context
            calls.append("namespaces")
            return (("default",),)

        def list_tables(self, context, *, continuation, limit):
            del context, continuation, limit
            calls.append("tables")
            return DiscoveryPage(())

        def close(self):
            calls.append("close")

    result = run_catalog_checks(
        cast(CatalogPlugin, _Catalog()), _catalog_context(), max_namespaces=0
    )

    assert result.to_dict()["status"] == "failed"
    assert any("budgets must be positive" in failure for failure in result.failures)
    assert calls == ["close"]


def test_catalog_runner_honors_expired_context_before_lifecycle() -> None:
    called = []

    class ExpiredCatalog:
        descriptor = _catalog_descriptor()

        def list_namespaces(self, context):
            del context
            return (("default",),)

        def list_tables(self, context, *, continuation, limit):
            del context, continuation, limit
            return DiscoveryPage(())

        def close(self):
            return None

    result = run_catalog_checks(
        cast(CatalogPlugin, ExpiredCatalog()), _catalog_context(deadline=datetime.now(timezone.utc))
    )

    assert result.to_dict()["status"] == "failed"
    assert any("deadline" in failure for failure in result.failures)
    assert called == []


def test_catalog_runner_rejects_omitted_expected_table():
    users = TableIdentifier(namespace=("default",), name="users")

    class _IncompleteCatalog:
        descriptor = _catalog_descriptor()

        def list_namespaces(self, context):
            del context
            return (("default",),)

        def close(self):
            return None

        def list_tables(self, context, *, continuation, limit):
            del context, continuation, limit
            return DiscoveryPage((users,))

    result = run_catalog_checks(
        cast(CatalogPlugin, _IncompleteCatalog()),
        _catalog_context(),
        expected_identifiers={users, TableIdentifier(("default",), "orders")},
    )
    assert result.to_dict()["status"] == "failed"
    assert any("do not match expected coverage" in failure for failure in result.failures)


@pytest.mark.parametrize(
    ("plugin_type", "message"),
    [("duplicate", "duplicate table identities"), ("cycle", "repeated continuation")],
)
def test_catalog_runner_rejects_duplicate_and_cyclic_pages(plugin_type, message):
    users = TableIdentifier(namespace=("default",), name="users")

    class _BadCatalog:
        descriptor = _catalog_descriptor()

        def list_namespaces(self, context):
            del context
            return (("default",),)

        def close(self):
            return None

        def list_tables(self, context, *, continuation, limit):
            del context, limit
            if plugin_type == "duplicate":
                return DiscoveryPage((users, users))
            return DiscoveryPage((), continuation="same")

    result = run_catalog_checks(cast(CatalogPlugin, _BadCatalog()), _catalog_context())
    assert result.to_dict()["status"] == "failed"
    assert any(message in failure for failure in result.failures)


def test_catalog_runner_stops_before_requesting_after_cancellation():
    users = TableIdentifier(namespace=("default",), name="users")
    calls = 0
    requested = 0

    def cancelled() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 2

    class _CancelledCatalog:
        descriptor = _catalog_descriptor()

        def list_namespaces(self, context):
            del context
            return (("default",),)

        def close(self):
            return None

        def list_tables(self, context, *, continuation, limit):
            del context, continuation, limit
            nonlocal requested
            requested += 1
            return DiscoveryPage((users,), continuation="next")

    result = run_catalog_checks(
        cast(CatalogPlugin, _CancelledCatalog()), _catalog_context(cancel_check=cancelled)
    )
    assert result.to_dict()["status"] == "failed"
    assert any("cancelled" in failure for failure in result.failures)
    assert requested == 0
