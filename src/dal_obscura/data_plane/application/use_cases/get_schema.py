from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.data_plane.application.ports.access_context import AccessContextPort
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest, IdentityPort
from dal_obscura.data_plane.application.ports.masking import MaskingPort
from dal_obscura.data_plane.application.use_cases.plan_access import (
    _authorize_requested_row_filter,
    _build_authorization_columns,
    _expand_requested_columns,
    _expand_to_leaves,
    _validate_requested_row_filter,
    _visible_columns,
    _with_required_map_keys,
)


@dataclass(frozen=True)
class GetSchemaResult:
    """Authorized schema response returned by GetSchemaUseCase.

    Example:
        ```python
        result = get_schema.execute(plan_request, auth_request)
        print(result.output_schema, result.policy_version)
        ```
    """

    output_schema: pa.Schema
    target: str
    columns: list[str]
    principal_id: str
    policy_version: int
    catalog: str | None = None


class GetSchemaUseCase:
    """Authenticates the caller and returns the masked authorized output schema."""

    def __init__(
        self,
        identity: IdentityPort,
        access_context: AccessContextPort,
        masking: MaskingPort,
    ) -> None:
        self._identity = identity
        self._access_context = access_context
        self._masking = masking

    def execute(self, request: PlanRequest, auth_request: AuthenticationRequest) -> GetSchemaResult:
        principal = self._identity.authenticate(auth_request)

        with self._access_context.open(request.catalog, request.target) as context:
            table_format = context.table_format
            base_schema = table_format.get_schema()

            requested_columns = _with_required_map_keys(
                base_schema,
                _expand_to_leaves(
                    base_schema, _expand_requested_columns(base_schema, request.columns)
                ),
            )
            requested_row_filter = _validate_requested_row_filter(base_schema, request.row_filter)

            decision = context.authorizer.authorize(
                principal=principal,
                target=request.target,
                catalog=request.catalog,
                requested_columns=_build_authorization_columns(
                    requested_columns, requested_row_filter
                ),
            )

            visible_columns = _visible_columns(
                requested_columns, decision, wildcard_requested=request.columns == ["*"]
            )
            _authorize_requested_row_filter(requested_row_filter, decision)

            output_schema = self._masking.masked_schema(
                base_schema, visible_columns, decision.masks
            )
            return GetSchemaResult(
                output_schema=output_schema,
                target=request.target,
                columns=visible_columns,
                principal_id=principal.id,
                policy_version=decision.policy_version,
                catalog=request.catalog,
            )
