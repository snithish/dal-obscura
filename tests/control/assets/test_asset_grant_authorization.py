"""Grant authority is checked against persisted capabilities before disclosure or mutation."""

from uuid import UUID

import pytest

from dal_obscura.application.governance.errors import AuthorizationFailure
from dal_obscura.kernel.identity.actor import ControlPlaneActor


@pytest.mark.parametrize(
    "principal,authorized",
    [("admin", True), ("manager", True), ("owner", False), ("reader", False)],
)
def test_grant_mutation_preserves_state_unless_actor_holds_grant_authority(
    asset_commands, principal, authorized
):
    commands, work_factory, admin = asset_commands
    asset = commands.upsert_workspace_asset(
        "analytics", "events", "iceberg", "default.events", {}, actor=admin, correlation_id=None
    )
    asset_id = UUID(asset["id"])
    commands.replace_asset_owners(asset_id, ["owner"], 0, actor=admin, correlation_id=None)
    initial = [{"principal": "manager", "capability": "grant"}]
    commands.replace_asset_grants(asset_id, initial, 1, actor=admin, correlation_id=None)
    desired = [*initial, {"principal": "reader", "capability": "read"}]
    actor = admin if principal == "admin" else ControlPlaneActor(principal, ())
    if authorized:
        assert commands.replace_asset_grants(
            asset_id, desired, 2, actor=actor, correlation_id="grant-change"
        ) == {"asset_id": str(asset_id), "grants": desired}
    else:
        with pytest.raises(AuthorizationFailure, match="grant"):
            commands.replace_asset_grants(
                asset_id, desired, 2, actor=actor, correlation_id="grant-change"
            )
    with work_factory() as work:
        persisted = [
            {"principal": grant["principal"], "capability": grant["capability"]}
            for grant in work.assets.list_asset_grants(asset_id)
        ]
        assert persisted == (desired if authorized else initial)
        assert work.assets.get_workspace_asset(asset_id)["revision"] == (3 if authorized else 2)
