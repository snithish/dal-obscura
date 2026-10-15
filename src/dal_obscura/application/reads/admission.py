from dal_obscura_plugin_api.schema.bounds import validate_native_schema
from dal_obscura_plugin_api.schema.index import SchemaIndex

from dal_obscura.application.reads.ports import ProjectionCompiler
from dal_obscura.application.reads.sources import AccessContext
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.governed import GovernedReadPlan
from dal_obscura.kernel.plans.projection import ExecutionProjection
from dal_obscura.kernel.plans.request import PlanRequest
from dal_obscura.kernel.policy.authorization import (
    authorize_requested_row_filter,
    resolve_visible_columns,
)
from dal_obscura.kernel.policy.filters import (
    RowFilter,
    combine_row_filters,
    parse_row_filter,
    validate_row_filter_against_schema,
)


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
        execution_columns=[*visible_columns, *internal_dependency_columns],
    )


def _extract_filter_dependencies(row_filter: RowFilter | None) -> list[str]:
    if not row_filter:
        return []
    return list(row_filter.dependencies)


def _build_authorization_columns(
    requested_columns: list[str],
    row_filter: RowFilter | None,
) -> list[str]:
    authorization_columns = list(requested_columns)
    for dependency in _extract_filter_dependencies(row_filter):
        if dependency not in authorization_columns:
            authorization_columns.append(dependency)
    return authorization_columns


def admit_read(
    context: AccessContext, principal: Principal, request: PlanRequest, compiler: ProjectionCompiler
) -> GovernedReadPlan:
    base_schema = context.schema

    requested_columns = SchemaIndex(base_schema).expand(request.columns)
    requested_row_filter = (
        validate_row_filter_against_schema(request.row_filter, base_schema)
        if request.row_filter is not None
        else None
    )

    authorization_columns = _build_authorization_columns(requested_columns, requested_row_filter)
    decision = context.authorizer.authorize(
        principal=principal,
        target=request.target,
        catalog=request.catalog,
        requested_columns=authorization_columns,
    )
    asset_id = decision.asset_id
    if asset_id is None:
        raise PermissionError("Authorization did not resolve a governed asset identity")

    visible_columns = resolve_visible_columns(
        requested_columns, decision, wildcard_requested=tuple(request.columns) == ("*",)
    )
    authorize_requested_row_filter(requested_row_filter, decision)

    policy_row_filter = (
        parse_row_filter(decision.row_filter, base_schema)
        if decision.row_filter is not None
        else None
    )
    effective_row_filter = combine_row_filters(policy_row_filter, requested_row_filter)
    execution_projection = _build_execution_projection(visible_columns, effective_row_filter)
    compiled_projection = compiler.compile(base_schema, visible_columns, decision.masks)
    validate_native_schema(compiled_projection.output_schema)
    return GovernedReadPlan(
        base_schema,
        decision,
        execution_projection,
        tuple(authorization_columns),
        requested_row_filter,
        effective_row_filter,
        compiled_projection,
    )
