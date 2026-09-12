from __future__ import annotations

from unittest.mock import Mock
from uuid import uuid4

import pytest

from dal_obscura.control_plane.application import policy_version_service
from dal_obscura.control_plane.application.access import ControlPlaneActor


def test_publication_authorization_is_read_after_asset_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    store = Mock()
    store.lock_asset_for_publication.side_effect = lambda _asset_id: order.append("lock")

    def authorize_after_lock(_store, _asset_id, _actor, _capability) -> None:
        order.append("authorize")
        raise RuntimeError("stop after ordering assertion")

    monkeypatch.setattr(policy_version_service, "ensure_asset_capability", authorize_after_lock)

    with pytest.raises(RuntimeError, match="ordering assertion"):
        policy_version_service.create_asset_policy_version(
            store,
            uuid4(),
            actor=ControlPlaneActor.for_platform_admin("platform:admin"),
            create_publication=Mock(),
            activate_publication=Mock(),
        )

    assert order == ["lock", "authorize"]
