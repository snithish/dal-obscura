"""Pydantic request models for control-plane routes.

Example:
    ```python
    request = RuntimeSettingsRequest(
        ticket_ttl_seconds=300,
        max_tickets=32,
        max_ticket_exchanges=1,
    )
    ```
"""

from __future__ import annotations

from typing import Any, Literal, cast
from urllib.parse import parse_qs
from uuid import UUID

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    """Base model that rejects unknown request fields.

    Example:
        ```python
        class RequestModel(StrictModel):
            name: str
        ```
    """

    model_config = ConfigDict(extra="forbid")


class TenantRequest(StrictModel):
    """Tenant/workspace creation request.

    Example:
        ```python
        TenantRequest(slug="default", display_name="Default workspace")
        ```
    """

    slug: str = Field(min_length=1)
    display_name: str = Field(min_length=1)


class CellRequest(StrictModel):
    """Data-plane cell creation request.

    Example:
        ```python
        CellRequest(name="default", region="local")
        ```
    """

    name: str = Field(min_length=1)
    region: str = Field(min_length=1)


class TenantCellRequest(CellRequest):
    """Combined tenant and cell assignment request.

    Example:
        ```python
        TenantCellRequest(name="default", region="local", shard_key="default")
        ```
    """

    shard_key: str = Field(default="default", min_length=1)


class TenantCellAssignmentRequest(StrictModel):
    """Assigns an existing cell to a tenant.

    Example:
        ```python
        TenantCellAssignmentRequest(cell_id=cell_id, shard_key="default")
        ```
    """

    cell_id: UUID
    shard_key: str = Field(default="default", min_length=1)


class CellTenantRequest(StrictModel):
    """Assigns the workspace tenant to an existing cell.

    Example:
        ```python
        CellTenantRequest(shard_key="default")
        ```
    """

    shard_key: str = Field(default="default", min_length=1)


class RuntimeSettingsRequest(StrictModel):
    """Runtime ticket and scan fan-out settings request.

    Example:
        ```python
        RuntimeSettingsRequest(
            ticket_ttl_seconds=300,
            max_tickets=32,
            max_ticket_exchanges=1,
        )
        ```
    """

    ticket_ttl_seconds: int = Field(gt=0)
    max_tickets: int = Field(gt=0)
    max_ticket_exchanges: int = Field(gt=0)


class CatalogRequest(StrictModel):
    """Catalog configuration request.

    Example:
        ```python
        CatalogRequest(module="iceberg", options={"uri": "sqlite:///catalog.db"})
        ```
    """

    module: Literal[
        "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
    ]
    options: dict[str, Any] = Field(default_factory=dict)


class AssetRequest(StrictModel):
    """Governed asset backend binding request.

    Example:
        ```python
        AssetRequest(backend="iceberg", table_identifier="default.orders")
        ```
    """

    backend: Literal["iceberg"] = "iceberg"
    table_identifier: str = Field(min_length=1, max_length=1_024)
    options: dict[str, Any] = Field(default_factory=dict)


class PolicyRulesRequest(StrictModel):
    """Ordered policy-rule replacement request.

    Example:
        ```python
        PolicyRulesRequest(rules=[{"effect": "allow", "columns": ["id"]}])
        ```
    """

    rules: list[dict[str, Any]]


class PolicyDraftRequest(StrictModel):
    """Revision-preconditioned policy draft replacement."""

    expected_revision: int = Field(ge=0)
    rules: list[dict[str, Any]]


class PolicyVersionPublishRequest(StrictModel):
    """Optional generation preconditions for publishing one asset draft."""

    expected_draft_revision: int | None = Field(default=None, ge=0)
    expected_publication_id: UUID | None = None
    review_token: str | None = Field(default=None, min_length=16, max_length=4096)


class PolicyRestoreRequest(StrictModel):
    """Revision-preconditioned request to restore immutable policy history."""

    expected_revision: int = Field(ge=0)


class PolicyPreviewRequest(StrictModel):
    """Policy-preview request for a candidate principal.

    Example:
        ```python
        PolicyPreviewRequest(principal="alice", groups=["analytics"], claims={})
        ```
    """

    principal: str = Field(min_length=1)
    groups: list[str] = Field(default_factory=list)
    claims: dict[str, object] = Field(default_factory=dict)
    columns: list[str] = Field(default_factory=list, max_length=512)


class PolicyEvaluationRequest(PolicyPreviewRequest):
    """Bounded synthetic rows for server-side DuckDB policy evaluation."""

    rows: list[dict[str, object]] = Field(default_factory=list, max_length=100)


class AssetOwnersRequest(StrictModel):
    """Asset owner-principal replacement request.

    Example:
        ```python
        AssetOwnersRequest(owners=["alice", "group:analytics"])
        ```
    """

    owners: list[str] = Field(default_factory=list)


class AssetGrantRequest(StrictModel):
    """Single explicit asset capability assignment."""

    principal: str = Field(min_length=1)
    capability: Literal["read", "edit", "publish", "grant"]


class AssetGrantsRequest(StrictModel):
    """Replaces explicit capability assignments for one asset."""

    grants: list[AssetGrantRequest] = Field(default_factory=list)


class AssetSchemaFieldRequest(StrictModel):
    """Single asset schema-field request.

    Example:
        ```python
        AssetSchemaFieldRequest(name="customer_id", type="string", nullable=False)
        ```
    """

    name: str = Field(min_length=1)
    type: str = Field(default="string", min_length=1)
    nullable: bool = True


class AssetSchemaFieldsRequest(StrictModel):
    """Asset schema-field replacement request.

    Example:
        ```python
        AssetSchemaFieldsRequest(fields=[AssetSchemaFieldRequest(name="id")])
        ```
    """

    fields: list[AssetSchemaFieldRequest] = Field(default_factory=list)


class AuthProvidersRequest(StrictModel):
    """Authentication provider-chain replacement request.

    Example:
        ```python
        AuthProvidersRequest(providers=[{"module": "example.Provider", "args": {}}])
        ```
    """

    providers: list[dict[str, Any]]


class DemoLoginRequest(StrictModel):
    """Local demo-login request.

    Example:
        ```python
        DemoLoginRequest(login_hint="alice")
        ```
    """

    login_hint: str = Field(min_length=1)


async def request_payload(request: Request) -> dict[str, object]:
    """Parses JSON or form-encoded route payloads into a mapping.

    Example:
        ```python
        payload = await request_payload(request)
        ```
    """

    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        raw = await request.json()
        return cast(dict[str, object], raw) if isinstance(raw, dict) else {}
    if "application/x-www-form-urlencoded" in content_type:
        raw = (await request.body()).decode("utf-8")
        payload: dict[str, object] = {
            key: values[-1]
            for key, values in parse_qs(raw, keep_blank_values=True).items()
            if values
        }
        if isinstance(payload.get("options"), str):
            payload["options"] = _json_object(payload["options"])
        return payload
    return {}


def _json_object(raw: object) -> dict[str, object]:
    if not isinstance(raw, str) or not raw.strip():
        return {}
    import json

    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("options must be a JSON object")
    return {str(key): item for key, item in value.items()}
