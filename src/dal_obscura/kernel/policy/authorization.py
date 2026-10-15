from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from dal_obscura_plugin_api.paths import path_covers

from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.policy.filters import RowFilter
from dal_obscura.kernel.policy.models import AccessDecision


class AuthorizationPort(Protocol):
    """Resolves which columns, masks, and row filters a principal may use."""

    def authorize(
        self,
        principal: Principal,
        target: str,
        catalog: str | None,
        requested_columns: Iterable[str],
    ) -> AccessDecision: ...


def resolve_visible_columns(
    requested_columns: list[str],
    decision: AccessDecision,
    *,
    wildcard_requested: bool,
) -> list[str]:
    if wildcard_requested:
        visible_columns = list(decision.allowed_columns)
    else:
        visible_columns = []
        for requested in requested_columns:
            authorized_descendants = [
                allowed for allowed in decision.allowed_columns if path_covers(requested, allowed)
            ]
            if not authorized_descendants:
                raise PermissionError("Requested columns are not authorized")
            for allowed in authorized_descendants:
                if allowed not in visible_columns:
                    visible_columns.append(allowed)
    if not visible_columns:
        raise PermissionError("No allowed columns for principal")
    return visible_columns


def authorize_requested_row_filter(
    row_filter: RowFilter | None,
    decision: AccessDecision,
) -> None:
    if row_filter is None:
        return

    dependencies = list(row_filter.dependencies)

    masked = [
        column
        for column in dependencies
        if any(_paths_overlap(column, mask_path) for mask_path in decision.masks)
    ]
    if masked:
        raise PermissionError(
            "Requested row filter may not reference masked columns: " + ", ".join(masked)
        )

    invisible = [column for column in dependencies if column not in decision.allowed_columns]
    if invisible:
        raise PermissionError(
            "Requested row filter may only reference visible unmasked columns: "
            + ", ".join(invisible)
        )


def _paths_overlap(first: str, second: str) -> bool:
    return path_covers(first, second) or path_covers(second, first)
