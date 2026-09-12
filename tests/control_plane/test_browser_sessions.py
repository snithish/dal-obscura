from __future__ import annotations

from datetime import timedelta

from sqlalchemy import select

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.common.config_store.orm import BrowserSessionRecord, utcnow
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.infrastructure.session_store import BrowserSessionStore


def test_browser_session_is_opaque_and_revocable() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    actor = ControlPlaneActor(
        principal="owner@example.com",
        groups=("asset-owners",),
        platform_admin=False,
    )
    factory = session_factory(engine)

    with factory() as session:
        token = BrowserSessionStore(session).issue(actor, ttl_seconds=3600)
        record = session.scalar(select(BrowserSessionRecord))
        assert record is not None
        assert record.token_hash != token
        assert len(record.token_hash) == 64
        session.commit()

    with factory() as session:
        store = BrowserSessionStore(session)
        assert store.resolve(token) == actor
        assert store.revoke(token) is True
        session.commit()

    with factory() as session:
        assert BrowserSessionStore(session).resolve(token) is None


def test_browser_session_expiry_is_enforced(monkeypatch) -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    actor = ControlPlaneActor.for_platform_admin("admin")
    factory = session_factory(engine)
    now = utcnow()
    monkeypatch.setattr(
        "dal_obscura.control_plane.infrastructure.session_store.utcnow",
        lambda: now,
    )
    with factory() as session:
        token = BrowserSessionStore(session).issue(actor, ttl_seconds=1)
        session.commit()

    monkeypatch.setattr(
        "dal_obscura.control_plane.infrastructure.session_store.utcnow",
        lambda: now + timedelta(seconds=2),
    )
    with factory() as session:
        assert BrowserSessionStore(session).resolve(token) is None


def test_browser_session_csrf_secret_is_bound_to_session() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    actor = ControlPlaneActor.for_platform_admin("admin")
    factory = session_factory(engine)

    with factory() as session:
        token, csrf = BrowserSessionStore(session).issue_with_csrf(actor, ttl_seconds=3600)
        session.commit()

    with factory() as session:
        store = BrowserSessionStore(session)
        assert store.resolve(token, csrf_token=csrf) == actor
        try:
            store.resolve(token, csrf_token="wrong")
        except ValueError as exc:
            assert "CSRF" in str(exc)
        else:
            raise AssertionError("a CSRF secret from another session must be rejected")
