"""Request snapshots and bounded provider ownership are correctness contracts."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import ClassVar
from unittest.mock import patch

import pyarrow as pa
import pytest
from sqlalchemy import event, select

from dal_obscura.policy.models import Principal
from dal_obscura.sources.published import LiveConfigCatalogRegistry
from dal_obscura.storage.database.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.storage.database.orm import (
    AssetRecord,
    CatalogRecord,
    PolicyRuleRecord,
)
from dal_obscura.storage.snapshots import LiveConfigStore
from tests.support.live_config import seed_live_config
from tests.support.reads import make_read_service
from tests.support.use_cases import StubTableFormat


def _table():
    return StubTableFormat(
        catalog_name="analytics",
        table_name="prod.users",
        format="fake",
        schema=pa.schema([("id", pa.int64()), ("email", pa.string())]),
        batches=(),
    )


def test_schema_read_reuses_the_schema_admitted_with_the_source(setup, monkeypatch):
    from dal_obscura.identity.contracts import AuthenticationRequest
    from dal_obscura.read.request import PlanRequest
    from tests.support.use_cases import FakeIdentity

    _, _, store = setup
    calls = 0
    original = StubTableFormat.get_schema

    def counted(self):
        nonlocal calls
        calls += 1
        return original(self)

    monkeypatch.setattr(StubTableFormat, "get_schema", counted)
    registry = LiveConfigCatalogRegistry(store)
    reader = make_read_service(
        identity=FakeIdentity(Principal(id="user1", groups=[], attributes={})),
        access_context=registry,
    )
    try:
        reader.schema(
            PlanRequest(catalog="analytics", target="default.users", columns=["id"]),
            AuthenticationRequest(headers={}),
        )
    finally:
        registry.close()
    assert calls == 1


class Provider:
    created: ClassVar[list["Provider"]] = []

    def __init__(self, config, **kwargs):
        self.config = config
        self.closed = False
        self.targets = []
        self.created.append(self)

    def describe(self, catalog, target):
        assert not self.closed
        self.targets.append(target)
        return _table()

    def close(self):
        self.closed = True


@pytest.fixture
def setup(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path}/config.db")
    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL")
    migrate_config_store(engine)
    factory = session_factory(engine)
    with factory() as session:
        seed_live_config(session)
    Provider.created = []
    with patch("dal_obscura.sources.published.CatalogRegistry", Provider):
        yield engine, factory, LiveConfigStore(factory)
    engine.dispose()


def _edit(factory, *, policy=False, uri=None):
    with factory() as session:
        if policy:
            asset = session.scalar(select(AssetRecord))
            rule = session.scalar(select(PolicyRuleRecord))
            asset.policy_revision += 1
            rule.columns_json = ["email"]
        if uri is not None:
            catalog = session.scalar(select(CatalogRecord))
            catalog.options_json = {"type": "sql", "uri": uri}
            catalog.revision += 1
        session.commit()


def test_snapshot_keeps_binding_policy_and_schema_in_one_database_view(setup):
    engine, factory, store = setup
    changed = False

    def after_read(conn, cursor, statement, parameters, context, many):
        nonlocal changed
        if not changed and "FROM assets JOIN catalogs" in statement:
            changed = True
            _edit(factory, policy=True, uri="sqlite:///new.db")

    event.listen(engine, "after_cursor_execute", after_read)
    try:
        asset, catalog = store.get_asset_and_catalog(catalog="analytics", target="default.users")
    finally:
        event.remove(engine, "after_cursor_execute", after_read)
    assert changed
    assert asset.policy_version == 4
    assert asset.compiled_config["policy"]["rules"][0]["columns"] == ["id"]
    assert catalog.config["options"]["uri"] == "sqlite:///catalog.db"
    next_asset, next_catalog = store.get_asset_and_catalog(
        catalog="analytics", target="default.users"
    )
    assert next_asset.policy_version == 5
    assert next_catalog.config["options"]["uri"] == "sqlite:///new.db"


def test_capacity_waits_for_lease_and_eviction_closes_only_idle_provider(setup):
    _, factory, store = setup
    registry = LiveConfigCatalogRegistry(store, max_cached_providers=1)
    started = Event()
    finished = Event()

    def next_request():
        started.set()
        with registry.open("analytics", "default.users"):
            finished.set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        with registry.open("analytics", "default.users"):
            first = Provider.created[0]
            _edit(factory, uri="sqlite:///changed.db")
            future = executor.submit(next_request)
            assert started.wait(2)
            assert not finished.wait(0.1)
            assert not first.closed
            assert len(Provider.created) == 1
        future.result(timeout=5)
    assert first.closed
    assert len(Provider.created) == 2
    registry.close()
    assert all(provider.closed for provider in Provider.created)


def test_close_defers_in_flight_provider_and_rejects_new_requests(setup):
    _, _, store = setup
    registry = LiveConfigCatalogRegistry(store)
    with registry.open("analytics", "default.users"):
        provider = Provider.created[0]
        registry.close()
        assert not provider.closed
        with (
            pytest.raises(RuntimeError, match="closed"),
            registry.open("analytics", "default.users"),
        ):
            pass
    assert provider.closed


def test_many_concurrent_requests_share_one_provider(setup):
    _, _, store = setup
    registry = LiveConfigCatalogRegistry(store)

    def request(_):
        with registry.open("analytics", "default.users") as context:
            assert context.table_format.get_schema().names == ["id", "email"]

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(request, range(32)))
    assert len(Provider.created) == 1
    registry.close()


@pytest.mark.parametrize("operation", ["schema", "plan"])
def test_use_cases_authorize_same_snapshot_as_provider_discovery(setup, monkeypatch, operation):
    from dal_obscura.identity.contracts import AuthenticationRequest
    from dal_obscura.read.request import PlanRequest
    from tests.support.use_cases import FakeIdentity, FakeTicketCodec, FakeTicketStore

    engine, factory, store = setup
    checked_out = 0

    def checkout(*args):
        nonlocal checked_out
        checked_out += 1

    def checkin(*args):
        nonlocal checked_out
        checked_out -= 1

    event.listen(engine, "checkout", checkout)
    event.listen(engine, "checkin", checkin)
    original = Provider.describe
    edited = False

    def describe(self, catalog, target):
        nonlocal edited
        assert checked_out == 0, "Provider IO must not hold a configuration connection"
        if not edited:
            edited = True
            _edit(factory, policy=True)
        return original(self, catalog, target)

    monkeypatch.setattr(Provider, "describe", describe)
    registry = LiveConfigCatalogRegistry(store)
    identity = FakeIdentity(Principal(id="user1", groups=[], attributes={}))

    ticket_store = FakeTicketStore()
    if operation == "schema":
        use_case = make_read_service(identity=identity, access_context=registry)
    else:
        use_case = make_read_service(
            identity=identity,
            access_context=registry,
            ticket_codec=FakeTicketCodec(),
            ticket_store=ticket_store,
            ticket_ttl_seconds=300,
            max_tickets=1,
            max_ticket_exchanges=1,
        )
    request = PlanRequest(catalog="analytics", target="default.users", columns=["id", "email"])
    auth = AuthenticationRequest(headers={})
    first = getattr(use_case, operation)(request, auth)
    second = getattr(use_case, operation)(request, auth)
    assert first.policy_version == 4
    assert second.policy_version == 5
    if operation == "plan":
        assert ticket_store.stored[0][0].scan["masks"] == {"email": {"type": "null", "value": None}}
        assert ticket_store.stored[1][0].scan["masks"] == {"id": {"type": "null", "value": None}}
    assert len(Provider.created) == 1
    registry.close()


def test_cold_provider_creation_does_not_block_other_configuration(setup, monkeypatch):
    _, factory, store = setup
    registry = LiveConfigCatalogRegistry(store, max_cached_providers=2)
    entered = Event()
    proceed = Event()
    original = Provider.__init__

    def slow_init(self, config, **kwargs):
        if config.catalogs["analytics"].options["uri"] == "sqlite:///catalog.db":
            entered.set()
            assert proceed.wait(3)
        original(self, config, **kwargs)

    monkeypatch.setattr(Provider, "__init__", slow_init)

    def request():
        with registry.open("analytics", "default.users"):
            return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(request)
        assert entered.wait(2)
        _edit(factory, uri="sqlite:///another.db")
        try:
            assert executor.submit(request).result(timeout=2)
        finally:
            proceed.set()
        assert first.result(timeout=2)
    registry.close()


def test_factory_failure_releases_capacity_for_retry(setup, monkeypatch):
    _, _, store = setup
    registry = LiveConfigCatalogRegistry(store, max_cached_providers=1)
    original = Provider.__init__

    def fail(self, *args, **kwargs):
        raise RuntimeError("factory failed")

    monkeypatch.setattr(Provider, "__init__", fail)
    with (
        pytest.raises(RuntimeError, match="factory failed"),
        registry.open("analytics", "default.users"),
    ):
        pass
    monkeypatch.setattr(Provider, "__init__", original)
    with registry.open("analytics", "default.users"):
        assert len(Provider.created) == 1
    registry.close()


def test_full_capacity_wait_is_bounded(setup):
    from dal_obscura.sources.access import AccessContextUnavailable

    _, factory, store = setup
    registry = LiveConfigCatalogRegistry(store, max_cached_providers=1, provider_wait_seconds=0.02)
    with registry.open("analytics", "default.users"):
        _edit(factory, uri="sqlite:///changed.db")
        with pytest.raises(AccessContextUnavailable), registry.open("analytics", "default.users"):
            pass
    registry.close()


def test_close_attempts_all_idle_providers_after_one_close_fails(setup, monkeypatch):
    _, factory, store = setup
    registry = LiveConfigCatalogRegistry(store, max_cached_providers=2)
    with registry.open("analytics", "default.users"):
        pass
    _edit(factory, uri="sqlite:///changed.db")
    with registry.open("analytics", "default.users"):
        pass

    def close(self):
        self.closed = True
        if self is Provider.created[0]:
            raise RuntimeError("close failed")

    monkeypatch.setattr(Provider, "close", close)
    with pytest.raises(RuntimeError, match="close failed"):
        registry.close()
    assert all(provider.closed for provider in Provider.created)
    registry.close()


def test_assets_share_provider_but_use_their_own_physical_binding(setup):
    from uuid import uuid4

    _, factory, store = setup
    with factory() as session:
        catalog = session.scalar(select(CatalogRecord))
        assert catalog is not None
        session.add(
            AssetRecord(
                id=uuid4(),
                catalog_id=catalog.id,
                target="another.asset",
                backend="iceberg",
                table_identifier="prod.other",
                options_json={},
                revision=1,
                policy_revision=1,
            )
        )
        session.commit()
    registry = LiveConfigCatalogRegistry(store)
    with registry.open("analytics", "default.users"):
        pass
    with registry.open("analytics", "another.asset"):
        pass
    assert len(Provider.created) == 1
    assert Provider.created[0].targets == ["prod.users", "prod.other"]
    registry.close()


def test_target_snapshot_needs_three_selects_and_no_global_counter_or_inventory(setup):
    engine, _, store = setup
    statements = []

    def observe(conn, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    event.listen(engine, "after_cursor_execute", observe)
    try:
        store.get_asset_and_catalog(catalog="analytics", target="default.users")
    finally:
        event.remove(engine, "after_cursor_execute", observe)
    assert len(statements) == 3
    assert all("workspace" not in sql for sql in statements)
    assert "FROM assets JOIN catalogs" in statements[0]
    assert "WHERE catalogs.name" in statements[0]
    assert "WHERE policy_rules.asset_id" in statements[1]
    assert "WHERE asset_schema_fields.asset_id" in statements[2]


def test_slow_eviction_close_does_not_block_warm_provider(setup, monkeypatch):
    from unittest.mock import Mock

    _, factory, store = setup
    registry = LiveConfigCatalogRegistry(store, max_cached_providers=2)
    with registry.open("analytics", "default.users"):
        pass
    _edit(factory, uri="sqlite:///warm.db")
    warm_asset, warm_catalog = store.get_asset_and_catalog(
        catalog="analytics", target="default.users"
    )
    with registry.open("analytics", "default.users"):
        pass
    _edit(factory, uri="sqlite:///third.db")
    cold_asset, cold_catalog = store.get_asset_and_catalog(
        catalog="analytics", target="default.users"
    )
    closing = Event()
    proceed = Event()
    original_close = Provider.close

    def slow_close(self):
        if self is Provider.created[0]:
            closing.set()
            assert proceed.wait(3)
        original_close(self)

    monkeypatch.setattr(Provider, "close", slow_close)
    local_store = Mock(spec=LiveConfigStore)
    local_store.get_asset_and_catalog.side_effect = [
        (cold_asset, cold_catalog),
        (warm_asset, warm_catalog),
    ]
    monkeypatch.setattr(registry, "_store", local_store)

    def request():
        with registry.open("analytics", "default.users"):
            return True

    with ThreadPoolExecutor(max_workers=2) as executor:
        cold = executor.submit(request)
        assert closing.wait(2)
        try:
            assert executor.submit(request).result(timeout=1)
        finally:
            proceed.set()
        assert cold.result(timeout=2)
    registry.close()
