from __future__ import annotations

from typing import cast

from dal_obscura_plugin_api import PluginKind

from dal_obscura.adapters.sql import audit as _db_audit
from dal_obscura.adapters.sql import workspace as _db_workspace
from dal_obscura.application.governance.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control.runtime import ControlContext
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from dal_obscura.plugins import PluginLifecycleState


def set_plugin_lifecycle(
    context: ControlContext,
    *,
    kind: str,
    plugin_id: str,
    target: PluginLifecycleState,
    actor: ControlPlaneActor,
    correlation_id: str | None = None,
) -> dict[str, str]:
    """Apply and audit one explicit process-local plugin lifecycle change."""

    if not actor.platform_admin:
        raise AuthorizationFailure("Platform admin required")
    if kind not in {"catalog", "table_format", "secret_backend"}:
        raise ValidationFailure("Unsupported plugin kind")
    if context.plugin_registry is None:
        raise ValidationFailure("Plugin registry was not admitted during application startup")
    try:
        lifecycle = context.plugin_registry.set_lifecycle(cast(PluginKind, kind), plugin_id, target)
    except ValueError as exc:
        raise ValidationFailure(str(exc)) from exc
    _db_workspace.ensure_workspace(context.session)
    _db_audit.record_workspace_audit_event(
        context.session,
        actor_principal=actor.identity_key(),
        action="plugin.lifecycle.update",
        resource_type="plugin",
        resource_id=f"{kind}:{plugin_id}",
        details={"target": lifecycle.value},
        correlation_id=correlation_id,
    )
    return {"kind": kind, "plugin_id": plugin_id, "lifecycle": lifecycle.value}
