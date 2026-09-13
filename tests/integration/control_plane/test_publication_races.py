from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
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


def _provision_asset(engine: Engine) -> tuple[UUID, UUID]:
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
            return cell_id, asset_id
        finally:
            store._session.close()


def test_concurrent_draft_cas_allows_one_revision_zero_writer(postgres_engine: Engine) -> None:
    _cell_id, asset_id = _provision_asset(postgres_engine)
    start_barrier = Barrier(2)

    def save_draft(principal: str) -> dict[str, object] | Exception:
        with Session(postgres_engine, future=True) as session:
            store = PublicationStore(session)
            try:
                start_barrier.wait(timeout=10)
                result = store.save_asset_policy_draft(
                    asset_id=asset_id,
                    author_principal=principal,
                    expected_revision=0,
                    rules=[
                        {
                            "ordinal": 10,
                            "effect": "allow",
                            "principals": [principal],
                            "columns": ["id"],
                        }
                    ],
                    content_hash=("a" if principal.endswith("a") else "b") * 64,
                    base_policy_version=0,
                )
                session.commit()
                return result
            except Exception as exc:  # assert the exact conflict below
                session.rollback()
                return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(save_draft, ("race-a", "race-b")))

    successes = [result for result in results if isinstance(result, dict)]
    conflicts = [result for result in results if isinstance(result, PublicationConflictError)]
    assert len(successes) == 2, "each principal has an independent draft row"
    assert not conflicts

    same_draft_barrier = Barrier(2)

    def save_same_draft(principal: str) -> dict[str, object] | Exception:
        with Session(postgres_engine, future=True) as session:
            store = PublicationStore(session)
            try:
                same_draft_barrier.wait(timeout=10)
                result = store.save_asset_policy_draft(
                    asset_id=asset_id,
                    author_principal=principal,
                    expected_revision=0,
                    rules=[],
                    content_hash="c" * 64,
                    base_policy_version=0,
                )
                session.commit()
                return result
            except Exception as exc:
                session.rollback()
                return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(save_same_draft, ("race-c", "race-c")))

    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, PublicationConflictError) for result in results) == 1


def test_concurrent_grant_replacements_use_asset_revision_cas(postgres_engine: Engine) -> None:
    _cell_id, asset_id = _provision_asset(postgres_engine)
    start_barrier = Barrier(2)

    def replace_grants(principal: str) -> list[dict[str, str]] | Exception:
        with Session(postgres_engine, future=True) as session:
            store = PublicationStore(session)
            try:
                start_barrier.wait(timeout=10)
                result = store.replace_asset_grants(
                    asset_id=asset_id,
                    expected_revision=0,
                    grants=[{"principal": principal, "capability": "read"}],
                )
                session.commit()
                return result
            except Exception as exc:
                session.rollback()
                return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(replace_grants, ("user:grant-a", "user:grant-b")))

    assert sum(isinstance(result, list) for result in results) == 1
    assert sum(isinstance(result, PublicationConflictError) for result in results) == 1
