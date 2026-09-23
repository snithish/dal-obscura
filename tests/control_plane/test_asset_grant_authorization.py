from __future__ import annotations

from unittest.mock import Mock
from uuid import uuid4

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.assets import (
    _replace_authorized_asset_grants,
)
from dal_obscura.control_plane.interfaces.routes.schemas import AssetGrantsRequest


def test_grant_mutation_locks_before_authorization_and_write() -> None:
    service = Mock()
    service.replace_asset_grants.return_value = []
    actor = ControlPlaneActor.for_platform_admin("platform:admin")
    asset_id = uuid4()

    result = _replace_authorized_asset_grants(
        service,
        asset_id,
        AssetGrantsRequest(grants=[], expected_revision=0),
        actor,
    )

    assert result == {"asset_id": str(asset_id), "grants": []}
    assert service.method_calls == [
        ("lock_asset_for_update", (asset_id,), {}),
        ("ensure_asset_capability", (asset_id, actor, "grant"), {}),
        (
            "replace_asset_grants",
            (asset_id, []),
            {"expected_revision": 0, "actor": actor},
        ),
    ]
