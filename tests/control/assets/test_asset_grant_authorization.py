from unittest.mock import MagicMock, Mock, call
from uuid import uuid4

from dal_obscura.application.governance.assets import service as asset_service
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def test_grant_mutation_locks_before_authorization_and_write(monkeypatch):
    operations = Mock()
    operations.write.return_value = []
    factory = MagicMock()
    work = factory.return_value.__enter__.return_value
    work.assets.lock_asset_for_update = operations.lock
    monkeypatch.setattr(asset_service, "ensure_asset_capability", operations.authorize)
    monkeypatch.setattr(asset_service, "replace_asset_grants", operations.write)
    actor = ControlPlaneActor.for_platform_admin("platform:admin")
    asset_id = uuid4()
    commands = asset_service.AssetsCommands(factory, schema_loader=Mock(), plugin_registry=None)
    result = commands.replace_asset_grants(
        asset_id,
        [],
        expected_revision=0,
        actor=actor,
        correlation_id=None,
    )
    assert result == {"asset_id": str(asset_id), "grants": []}
    assert operations.mock_calls == [
        call.lock(asset_id),
        call.authorize(work.assets, asset_id, actor, "grant"),
        call.write(work, asset_id, [], 0, actor, correlation_id=None),
    ]
