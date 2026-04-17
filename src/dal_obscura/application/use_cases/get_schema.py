from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa

from dal_obscura.application.ports.authorization import AuthorizationPort
from dal_obscura.application.ports.identity import IdentityPort
from dal_obscura.application.ports.masking import MaskingPort
from dal_obscura.application.use_cases.plan_access import (
    _authorize_requested_row_filter,
    _build_authorization_columns,
    _expand_requested_columns,
    _validate_requested_row_filter,
    _visible_columns,
)
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.infrastructure.adapters.catalog_registry import DynamicCatalogRegistry


@dataclass(frozen=True)
class GetSchemaResult:
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
        authorizer: AuthorizationPort,
        catalog_registry: DynamicCatalogRegistry,
        masking: MaskingPort,
    ) -> None:
        self._identity = identity
        self._authorizer = authorizer
        self._catalog_registry = catalog_registry
        self._masking = masking

    def execute(self, request: PlanRequest, headers: Mapping[str, str]) -> GetSchemaResult:
        principal = self._identity.authenticate(headers)

        table_format = self._catalog_registry.describe(request.catalog, request.target)
        base_schema = table_format.get_schema()

        requested_columns = _expand_requested_columns(base_schema, request.columns)
        requested_row_filter = _validate_requested_row_filter(base_schema, request.row_filter)

        decision = self._authorizer.authorize(
            principal=principal,
            target=request.target,
            catalog=request.catalog,
            requested_columns=_build_authorization_columns(requested_columns, requested_row_filter),
        )

        visible_columns = _visible_columns(requested_columns, decision)
        _authorize_requested_row_filter(requested_row_filter, decision)

        output_schema = self._masking.masked_schema(base_schema, visible_columns, decision.masks)
        return GetSchemaResult(
            output_schema=output_schema,
            target=request.target,
            columns=visible_columns,
            principal_id=principal.id,
            policy_version=decision.policy_version,
            catalog=request.catalog,
        )
