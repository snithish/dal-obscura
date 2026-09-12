from __future__ import annotations

import pytest

import dal_obscura.control_plane.infrastructure.catalog_discovery as discovery
from dal_obscura.control_plane.infrastructure.catalog_discovery import (
    discover_iceberg_tables,
)


class FakeIcebergCatalog:
    def list_namespaces(self, namespace=()):
        if namespace == ():
            return [("default",), ("prod",)]
        return []

    def list_tables(self, namespace):
        if namespace == ("default",):
            return [("default", "users")]
        if namespace == ("prod",):
            return [("prod", "orders")]
        return []


def test_iceberg_discovery_lists_tables_across_namespaces():
    tables = discover_iceberg_tables(
        "analytics",
        {"type": "sql", "uri": "sqlite:///catalog.db"},
        load_catalog_fn=lambda name, **options: FakeIcebergCatalog(),
    )

    assert tables == [
        {"backend": "iceberg", "name": "default.users", "table_identifier": "default.users"},
        {"backend": "iceberg", "name": "prod.orders", "table_identifier": "prod.orders"},
    ]


def test_iceberg_discovery_rejects_namespace_explosion():
    with pytest.raises(ValueError, match="namespace limit"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: FakeIcebergCatalog(),
            max_namespaces=2,
        )


def test_iceberg_discovery_rejects_table_explosion():
    with pytest.raises(ValueError, match="table limit"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: FakeIcebergCatalog(),
            max_tables=1,
        )


def test_iceberg_discovery_stops_an_unbounded_provider_page():
    class EndlessCatalog:
        def list_namespaces(self, namespace=()):
            if namespace == ():
                return (("ns", index) for index in range(100_000))
            return ()

        def list_tables(self, namespace):
            return ()

    with pytest.raises(ValueError, match="namespace limit"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: EndlessCatalog(),
            max_namespaces=4,
        )


def test_iceberg_discovery_honors_cancellation_and_deadline():
    class SlowCatalog:
        def list_namespaces(self, namespace=()):
            return (("ns", index) for index in range(100_000))

        def list_tables(self, namespace):
            return ()

    with pytest.raises(RuntimeError, match="cancelled"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: SlowCatalog(),
            cancel_check=lambda: True,
        )

    with pytest.raises(TimeoutError, match="deadline"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: SlowCatalog(),
            deadline_at=0.0,
        )


def test_iceberg_discovery_rejects_when_process_capacity_is_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OccupiedSlots:
        def acquire(self, *, blocking: bool) -> bool:
            assert blocking is False
            return False

        def release(self) -> None:
            raise AssertionError("an unacquired slot must not be released")

    monkeypatch.setattr(discovery, "_DISCOVERY_SLOTS", OccupiedSlots())

    with pytest.raises(RuntimeError, match="capacity is exhausted"):
        discover_iceberg_tables("analytics", {})


def test_iceberg_discovery_releases_capacity_after_provider_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Slots:
        acquired = 0
        released = 0

        def acquire(self, *, blocking: bool) -> bool:
            assert blocking is False
            self.acquired += 1
            return True

        def release(self) -> None:
            self.released += 1

    slots = Slots()
    monkeypatch.setattr(discovery, "_DISCOVERY_SLOTS", slots)

    with pytest.raises(RuntimeError, match="provider failed"):
        discover_iceberg_tables(
            "analytics",
            {},
            load_catalog_fn=lambda name, **options: (_ for _ in ()).throw(
                RuntimeError("provider failed")
            ),
        )

    assert (slots.acquired, slots.released) == (1, 1)
