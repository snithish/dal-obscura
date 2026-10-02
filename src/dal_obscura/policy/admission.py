from __future__ import annotations

import pyarrow as pa

from dal_obscura.policy.filters import (
    RowFilter,
    extract_row_filter_dependencies,
    parse_row_filter,
    validate_row_filter_against_schema,
)
from dal_obscura.policy.models import AccessDecision
from dal_obscura.policy.paths import path_covers
from dal_obscura.read.request import ExecutionProjection


def _build_execution_projection(
    visible_columns: list[str],
    row_filter: RowFilter | None,
) -> ExecutionProjection:
    """Builds the visible and internal columns needed to enforce policy safely."""
    internal_dependency_columns = [
        column
        for column in _extract_filter_dependencies(row_filter)
        if column not in visible_columns
    ]
    return ExecutionProjection(
        visible_columns=visible_columns,
        internal_dependency_columns=internal_dependency_columns,
        execution_columns=[*visible_columns, *internal_dependency_columns],
    )


def _extract_filter_dependencies(row_filter: RowFilter | None) -> list[str]:
    if not row_filter:
        return []
    return extract_row_filter_dependencies(row_filter)


def _build_authorization_columns(
    requested_columns: list[str],
    row_filter: RowFilter | None,
) -> list[str]:
    authorization_columns = list(requested_columns)
    for dependency in _extract_filter_dependencies(row_filter):
        if dependency not in authorization_columns:
            authorization_columns.append(dependency)
    return authorization_columns


def _visible_columns(
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


def _authorize_requested_row_filter(
    row_filter: RowFilter | None,
    decision: AccessDecision,
) -> None:
    if row_filter is None:
        return

    dependencies = _extract_filter_dependencies(row_filter)

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


def _validate_policy_row_filter(schema: pa.Schema, row_filter: str | None) -> RowFilter | None:
    if row_filter is None:
        return None
    return parse_row_filter(row_filter, schema)


def _validate_requested_row_filter(
    schema: pa.Schema,
    row_filter: RowFilter | None,
) -> RowFilter | None:
    if row_filter is None:
        return None
    return validate_row_filter_against_schema(row_filter, schema)
