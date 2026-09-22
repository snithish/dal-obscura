from __future__ import annotations

import multiprocessing
import os
from collections.abc import Iterator
from multiprocessing.connection import Connection
from multiprocessing.synchronize import Barrier
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.db import create_engine_from_url, migrate_config_store
from dal_obscura.control_plane.application.errors import PublicationConflictError
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore

pytestmark = pytest.mark.integration


@pytest.fixture
def postgres_engine() -> Iterator[Engine]:
    database_url = os.environ.get("DAL_OBSCURA_POSTGRES_TEST_URL", "").strip()
    if not database_url:
        pytest.skip("set DAL_OBSCURA_POSTGRES_TEST_URL to run PostgreSQL race gates")
    if not database_url.startswith(("postgresql://", "postgresql+psycopg://")):
        pytest.fail("DAL_OBSCURA_POSTGRES_TEST_URL must use PostgreSQL")
    engine = create_engine_from_url(database_url)
    migrate_config_store(engine)
    try:
        yield engine
    finally:
        engine.dispose()


def _provision_asset(engine: Engine) -> tuple[UUID, UUID, UUID]:
    cell_id = uuid4()
    tenant_id = uuid4()
    with engine.begin() as connection:
        store = PublicationStore(Session(bind=connection, future=True))
        try:
            store.create_cell(cell_id=cell_id, name=f"race-{cell_id}", region="test")
            store.create_tenant(
                tenant_id=tenant_id,
                slug=f"race-{tenant_id}",
                display_name="PostgreSQL race fixture",
            )
            store.assign_tenant_to_cell(
                cell_id=cell_id,
                tenant_id=tenant_id,
                shard_key="race",
            )
            store.replace_auth_providers(
                cell_id=cell_id,
                providers=[
                    {
                        "ordinal": 1,
                        "module": "oidc.test.Provider",
                        "args": {"issuer": "https://issuer.example"},
                        "enabled": True,
                    }
                ],
            )
            store.upsert_catalog(
                cell_id=cell_id,
                tenant_id=tenant_id,
                name="analytics",
                module="iceberg.sql",
                options={"uri": "sqlite:///race-fixture.db"},
            )
            asset_id = store.upsert_asset(
                cell_id=cell_id,
                tenant_id=tenant_id,
                catalog="analytics",
                target="default.users",
                backend="iceberg",
                table_identifier="default.users",
                options={},
            )
            return cell_id, tenant_id, asset_id
        finally:
            store._session.close()


def _write_in_process(
    database_url: str,
    barrier: Barrier,
    result_pipe: Connection,
    method: str,
    arguments: dict[str, object],
) -> None:
    # Spawned workers create their own pools; no inherited engine or Python lock.
    engine = create_engine_from_url(database_url)
    try:
        with Session(engine, future=True) as session:
            barrier.wait(timeout=15)
            try:
                result = getattr(PublicationStore(session), method)(**arguments)
                session.commit()
                outcome = ("committed", result)
            except PublicationConflictError:
                session.rollback()
                outcome = ("conflict", None)
            result_pipe.send((os.getpid(), outcome))
    finally:
        result_pipe.close()
        engine.dispose()


def _race(engine: Engine, method: str, calls: list[dict[str, object]]) -> list[str]:
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    pipes = [context.Pipe(duplex=False) for _ in calls]
    processes = [
        context.Process(
            target=_write_in_process,
            args=(engine.url.render_as_string(hide_password=False), barrier, sender, method, args),
        )
        for (_, sender), args in zip(pipes, calls, strict=True)
    ]
    try:
        for process in processes:
            process.start()
        for _, sender in pipes:
            sender.close()
        results = []
        for receiver, _ in pipes:
            assert receiver.poll(25), "Database race worker timed out"
            results.append(receiver.recv())
        for process in processes:
            process.join(timeout=5)
            assert process.exitcode == 0, "Database race worker failed"
        assert len({pid for pid, _ in results}) == 2
        assert all(pid != os.getpid() for pid, _ in results)
        return [outcome[0] for _, outcome in results]
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        for receiver, sender in pipes:
            receiver.close()
            sender.close()


@pytest.mark.parametrize(
    "operation",
    ["draft", "grants", "binding", "runtime", "providers"],
)
def test_two_process_revision_cas(postgres_engine: Engine, operation: str) -> None:
    cell_id, tenant_id, asset_id = _provision_asset(postgres_engine)
    methods = {
        "draft": "save_asset_policy_draft",
        "grants": "replace_asset_grants",
        "binding": "upsert_asset",
        "runtime": "upsert_runtime_settings",
        "providers": "replace_auth_providers",
    }
    calls: list[dict[str, object]] = []
    for index in range(2):
        options: dict[str, object] = {}
        if operation == "draft":
            options = {
                "asset_id": asset_id,
                "author_principal": "same-author",
                "rules": [],
                "content_hash": str(index) * 64,
                "base_policy_version": 0,
            }
        elif operation == "grants":
            options = {
                "asset_id": asset_id,
                "grants": [{"principal": f"user:{index}", "capability": "read"}],
            }
        elif operation == "binding":
            options = {
                "cell_id": cell_id,
                "tenant_id": tenant_id,
                "catalog": "analytics",
                "target": "default.users",
                "backend": "iceberg",
                "table_identifier": f"default.users_{index}",
                "options": {},
            }
        elif operation == "runtime":
            options = {
                "cell_id": cell_id,
                "ticket_ttl_seconds": 300 + index,
                "max_tickets": 64,
                "max_ticket_exchanges": 2,
            }
        else:
            options = {
                "cell_id": cell_id,
                "providers": [
                    {
                        "ordinal": 1,
                        "module": "oidc.test.Provider",
                        "args": {"issuer": f"https://issuer-{index}.example"},
                        "enabled": True,
                    }
                ],
            }
        calls.append({**options, "expected_revision": 0})
    assert sorted(_race(postgres_engine, methods[operation], calls)) == ["committed", "conflict"]


def test_two_process_drafts_are_independent_per_author(postgres_engine: Engine) -> None:
    _, _, asset_id = _provision_asset(postgres_engine)
    calls: list[dict[str, object]] = [
        {
            "asset_id": asset_id,
            "author_principal": author,
            "expected_revision": 0,
            "rules": [],
            "content_hash": "a" * 64,
            "base_policy_version": 0,
        }
        for author in ("author-a", "author-b")
    ]
    assert _race(postgres_engine, "save_asset_policy_draft", calls) == ["committed", "committed"]
