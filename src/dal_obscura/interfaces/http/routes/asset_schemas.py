"""HTTP asset request and response contracts."""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from dal_obscura_plugin_api.schema.bounds import MAX_SCHEMA_ADMISSION_FIELDS
from pydantic import (
    BaseModel,
    Field,
)

from dal_obscura.interfaces.http.routes.policy_schemas import (
    PolicyRuleSchema,
)
from dal_obscura.interfaces.http.routes.schemas import (
    StrictModel,
)


class AssetCapabilityResponse(BaseModel):
    """One server-resolved capability and its explainable reasons."""

    capability: Literal["read", "edit", "grant"]
    allowed: bool
    reasons: list[str]


class AssetAccessResponse(BaseModel):
    """Effective capabilities and owner-only operations for one governed asset."""

    asset_id: str
    principal: str
    issuer: str | None = None
    can_revoke_tokens: bool
    capabilities: list[AssetCapabilityResponse]


class AssetMutationResponse(BaseModel):
    """Stable identity returned after creating or updating an asset."""

    id: str
    catalog: str
    target: str


class AssetOwnersResponse(BaseModel):
    """Authoritative owner set after an asset owner replacement."""

    asset_id: str
    owners: list[str]


class AssetGrantResponse(BaseModel):
    """One explicit delegated asset capability."""

    principal: str
    capability: Literal["read", "edit", "grant"]


class AssetGrantsResponse(BaseModel):
    """Authoritative delegated capabilities for one asset."""

    asset_id: str
    grants: list[AssetGrantResponse]


class SchemaFieldResponse(BaseModel):
    """Admitted flat field metadata used by the policy draft."""

    name: str
    type: str
    nullable: bool
    field_id: str | None = Field(default=None, exclude_if=lambda value: value is None)
    path: list[str] | None = Field(default=None, exclude_if=lambda value: value is None)


class FieldPathSegmentResponse(BaseModel):
    kind: Literal["field", "list_element", "map_key", "map_value"]
    name: str | None = Field(default=None, exclude_if=lambda value: value is None)
    field_id: int | None = Field(default=None, exclude_if=lambda value: value is None)


class FieldPathResponse(BaseModel):
    version: Literal[1]
    segments: list[FieldPathSegmentResponse]


class SchemaNodeResponse(BaseModel):
    """Typed nested schema tree, shared with generated browser clients."""

    field_id: int
    name: str
    path: FieldPathResponse
    human_path: str
    type: str
    nullable: bool
    kind: Literal["scalar", "struct", "list", "map"]
    children: list[SchemaNodeResponse] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )


class AssetSchemaFieldsResponse(BaseModel):
    """Admitted schema metadata after a replacement."""

    asset_id: str
    fields: list[SchemaFieldResponse]


class AssetInventoryResponse(BaseModel):
    """Bounded workspace inventory row with authoritative serving state."""

    id: UUID
    name: str
    catalog: str
    backend: str
    table_identifier: str
    owner_count: int
    owners: list[str]
    policy_status: str
    policy_revision: int


class AssetInventoryPageResponse(BaseModel):
    """Cursor page for workspace asset inventory."""

    items: list[AssetInventoryResponse]
    next_cursor: str | None = None


class AssetDetailResponse(BaseModel):
    """Full governed asset record used by the policy editor."""

    id: str
    name: str
    catalog: str
    backend: str
    table_identifier: str
    owners: list[str]
    policy_status: str
    owner_count: int
    revision: int
    policy_revision: int
    options: dict[str, Any]
    schema_fields: list[SchemaFieldResponse]
    policy_rules: list[PolicyRuleSchema]


class AssetSchemaResponse(BaseModel):
    """Authoritative nested schema metadata for one governed asset."""

    asset_id: str
    catalog: str
    target: str
    schema_version: int
    schema_fingerprint: str
    stable_field_ids: bool | None = None
    supported_masks: list[Literal["null", "redact", "hash", "email", "keep_last", "default"]]
    fields: list[SchemaNodeResponse]


class AssetRequest(StrictModel):
    """Governed asset backend binding request.

    Example:
        ```python
        AssetRequest(backend="iceberg", table_identifier="default.orders")
        ```
    """

    backend: str = Field(
        default="iceberg", min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_.-]{0,63}$"
    )
    table_identifier: str = Field(min_length=1, max_length=1_024)
    options: dict[str, Any] = Field(default_factory=dict)
    expected_revision: int | None = Field(default=None, ge=0)


class AssetTokenRevocationResponse(BaseModel):
    asset_id: str
    revoked_token_count: int


class AssetOwnersRequest(StrictModel):
    """Asset owner-principal replacement request.

    Example:
        ```python
        AssetOwnersRequest(owners=["alice", "group:analytics"])
        ```
    """

    owners: list[str] = Field(default_factory=list, max_length=64)
    expected_revision: int | None = Field(default=None, ge=0)


class AssetGrantRequest(StrictModel):
    """Single explicit asset capability assignment."""

    principal: str = Field(min_length=1)
    capability: Literal["read", "edit", "grant"]


class AssetGrantsRequest(StrictModel):
    """Replaces explicit capability assignments for one asset."""

    grants: list[AssetGrantRequest] = Field(default_factory=list, max_length=256)
    expected_revision: int | None = Field(default=None, ge=0)


class AssetSchemaFieldRequest(StrictModel):
    """Single asset schema-field request.

    Example:
        ```python
        AssetSchemaFieldRequest(name="customer_id", type="string", nullable=False)
        ```
    """

    name: str = Field(min_length=1)
    field_id: str | None = Field(default=None, min_length=1, max_length=128)
    path: list[str] | None = Field(default=None, min_length=1)
    type: str = Field(default="string", min_length=1)
    nullable: bool = Field(default=True, strict=True)


class AssetSchemaFieldsRequest(StrictModel):
    """Asset schema-field replacement request.

    Example:
        ```python
        AssetSchemaFieldsRequest(fields=[AssetSchemaFieldRequest(name="id")])
        ```
    """

    fields: list[AssetSchemaFieldRequest] = Field(
        default_factory=list, max_length=MAX_SCHEMA_ADMISSION_FIELDS
    )
    expected_revision: int | None = Field(default=None, ge=0)
