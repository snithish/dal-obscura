from unittest.mock import Mock, call
from uuid import uuid4

from dal_obscura.control import asset_service, policy_service
from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.runtime import ControlContext
from dal_obscura.interfaces.http.routes.assets import _replace_authorized_asset_grants
from dal_obscura.interfaces.http.routes.schemas import AssetGrantsRequest
from dal_obscura.storage import assets


def test_grant_mutation_locks_before_authorization_and_write(monkeypatch, db_session):
    operations = Mock()
    operations.write.return_value = []
    monkeypatch.setattr(assets, "lock_asset_for_update", operations.lock)
    monkeypatch.setattr(policy_service, "ensure_asset_capability", operations.authorize)
    monkeypatch.setattr(asset_service, "replace_asset_grants", operations.write)
    actor = ControlPlaneActor.for_platform_admin("platform:admin")
    asset_id = uuid4()
    result = _replace_authorized_asset_grants(
        ControlContext(db_session),
        asset_id,
        AssetGrantsRequest(grants=[], expected_revision=0),
        actor,
    )
    assert result == {"asset_id": str(asset_id), "grants": []}
    assert operations.mock_calls == [
        call.lock(db_session, asset_id),
        call.authorize(db_session, asset_id, actor, "grant"),
        call.write(db_session, asset_id, [], expected_revision=0, actor=actor),
    ]
