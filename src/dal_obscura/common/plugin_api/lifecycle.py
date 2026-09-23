"""Explicit plugin admission lifecycle transitions.

Lifecycle state is process-local control metadata. It never changes the
immutable plugin lock or the persisted live configuration.
"""

from __future__ import annotations

from enum import Enum


class PluginLifecycleState(str, Enum):
    ENABLED = "enabled"
    DRAINING = "draining"
    DISABLED = "disabled"
    REVOKED = "revoked"
    REMOVED = "removed"


class PluginLifecycleError(ValueError):
    """Raised when a lifecycle transition would weaken admission safety."""


_TRANSITIONS: dict[PluginLifecycleState, frozenset[PluginLifecycleState]] = {
    PluginLifecycleState.ENABLED: frozenset(
        {
            PluginLifecycleState.DRAINING,
            PluginLifecycleState.DISABLED,
            PluginLifecycleState.REVOKED,
        }
    ),
    PluginLifecycleState.DRAINING: frozenset(
        {PluginLifecycleState.DISABLED, PluginLifecycleState.REVOKED}
    ),
    PluginLifecycleState.DISABLED: frozenset({PluginLifecycleState.ENABLED}),
    PluginLifecycleState.REVOKED: frozenset({PluginLifecycleState.REMOVED}),
    PluginLifecycleState.REMOVED: frozenset(),
}


def transition_plugin_lifecycle(
    current: PluginLifecycleState, target: PluginLifecycleState
) -> PluginLifecycleState:
    """Validate and return one monotonic, explicit lifecycle transition."""

    if target is current:
        return current
    if target not in _TRANSITIONS[current]:
        raise PluginLifecycleError(f"Invalid plugin lifecycle transition: {current} -> {target}")
    return target
