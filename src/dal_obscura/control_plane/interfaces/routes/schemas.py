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
    path_rules: list[dict[str, object]] = Field(default_factory=list, max_length=256)
    expected_revision: int | None = Field(default=None, ge=0)


class RuntimeSettingsResponse(BaseModel):
    """Authoritative runtime settings returned to management clients."""

    ticket_ttl_seconds: int
    max_tickets: int
    max_ticket_exchanges: int
    path_rules: list[dict[str, str]]
    revision: int


class SessionResponse(BaseModel):
    """Safe authenticated actor metadata for browser and API clients."""

    principal: str
    groups: list[str]
    platform_admin: bool
    capabilities: list[str]
    issuer: str | None = None


class LoginShortcutResponse(BaseModel):
    """Browser-safe named login shortcut."""

    label: str
    login_hint: str
    demo_login_path: str | None = None


class UiAuthConfigResponse(BaseModel):
    """Browser-safe OIDC settings; secrets never appear here."""

    authority: str | None = None
    client_id: str | None = None
    redirect_uri: str | None = None
    post_logout_redirect_uri: str | None = None
    scope: str | None = None
    login_shortcuts: list[LoginShortcutResponse] | None = None


class SessionOptionsResponse(BaseModel):
    """Available browser authentication methods."""

    bootstrap_enabled: bool
    oidc: UiAuthConfigResponse | None = None


class AuthenticationMutationResponse(BaseModel):
    """Result of a browser session login or logout mutation."""

    authenticated: bool


class AssetCapabilityResponse(BaseModel):
    """One server-resolved capability and its explainable reasons."""

    capability: Literal["read", "edit", "publish", "grant"]
    allowed: bool
    reasons: list[str]


class AssetAccessResponse(BaseModel):
    """Effective capabilities for one governed asset and actor."""

    asset_id: str
    principal: str
    issuer: str | None = None
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
    capability: Literal["read", "edit", "publish", "grant"]


class AssetGrantsResponse(BaseModel):
    """Authoritative delegated capabilities for one asset."""

    asset_id: str
    grants: list[AssetGrantResponse]


class AssetSchemaFieldsResponse(BaseModel):
    """Admitted schema metadata after a replacement."""

    asset_id: str
    fields: list[dict[str, Any]]


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
    draft_status: str
    active_policy_version: int | None = None
    last_published_at: str | None = None


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
    draft_status: str
    owner_count: int
    active_policy_version: int | None = None
    last_published_at: str | None = None
    revision: int
    options: dict[str, Any]
    schema_fields: list[dict[str, Any]]
    policy_rules: list[dict[str, Any]]


class AssetSchemaResponse(BaseModel):
    """Authoritative nested schema metadata for one governed asset."""

    asset_id: str
    catalog: str
    target: str
    schema_version: int
    schema_fingerprint: str
    stable_field_ids: bool | None = None
    supported_masks: list[Literal["null", "redact", "hash", "email", "keep_last", "default"]]
    fields: list[dict[str, Any]]


class CatalogInventoryResponse(BaseModel):
    """Workspace catalog summary safe for management clients."""

    id: UUID
    name: str
    module: str
    plugin_id: str | None = None
    options: dict[str, Any]
    status: str
    revision: int
    discovered_table_count: int = 0
    governed_asset_count: int = 0


class CatalogMutationResponse(BaseModel):
    """Stable identity returned after creating or updating a catalog."""

    id: str
    name: str


class CatalogTablesResponse(BaseModel):
    """Bounded catalog discovery result returned to management clients."""

    catalog: str
    tables: list[dict[str, Any]]


class CatalogDiagnosticResponse(BaseModel):
    """Bounded catalog connectivity diagnostic."""

    catalog: str
    status: Literal["ready", "unavailable"]
    message: str
    checked_at: str
    table_count: int | None = None
    sample_tables: list[str] | None = None


class PluginDescriptorResponse(BaseModel):
    """Admitted plugin descriptor exposed to operator tooling."""

    kind: Literal["catalog", "table_format"]
    plugin_id: str
    api_version: str
    config_version: int
    distribution: str
    version: str
    display_name: str
    capabilities: list[str]
    output_formats: list[str]
    handle_versions: list[int]
    config_schema: dict[str, Any]
    status: Literal["admitted", "incompatible"]


class PluginPairResponse(BaseModel):
    """One catalog/table-format compatibility result."""

    catalog_plugin_id: str
    format_plugin_id: str
    capabilities: list[str]
    handle_versions: list[int]
    status: Literal["admitted", "incompatible"]


class PluginStateResponse(BaseModel):
    """Allowlisted plugin lifecycle status without factory imports."""

    kind: Literal["catalog", "table_format"]
    plugin_id: str
    status: Literal["enabled", "not_installed", "incompatible"]
    reason: str | None = None
    lifecycle: Literal["enabled", "draining", "disabled", "revoked", "removed"] | None = None


class PluginLifecycleRequest(StrictModel):
    """Requested process-local admission lifecycle transition."""

    target: Literal["enabled", "draining", "disabled", "revoked", "removed"]


class PluginLifecycleResponse(BaseModel):
    """Result of one explicit plugin lifecycle transition."""

    kind: Literal["catalog", "table_format"]
    plugin_id: str
    lifecycle: Literal["enabled", "draining", "disabled", "revoked", "removed"]


class PluginListResponse(BaseModel):
    """Bounded admitted plugin registry response."""

    plugins: list[PluginDescriptorResponse]
    pairs: list[PluginPairResponse]
    states: list[PluginStateResponse]


class WorkspaceSummaryResponse(BaseModel):
    """Permission-scoped workspace counts."""

    catalog_count: int
    asset_count: int
    unowned_asset_count: int
    missing_policy_count: int
    draft_change_count: int
    runtime_configured: bool
    enabled_auth_provider_count: int


class WorkspaceGenerationResponse(BaseModel):
    """Active immutable generation summary."""

    cell_id: str
    publication_id: str
    manifest_hash: str
    status: str


class DataPlaneObservationResponse(BaseModel):
    """Explicit data-plane health observation state."""

    status: str
    reason: str


class WorkspaceObservationsResponse(BaseModel):
    """Control-plane observations kept separate from Flight health."""

    available: bool
    observed_at: str
    source: str
    generation: WorkspaceGenerationResponse | None = None
    data_plane: DataPlaneObservationResponse


class WorkspacePublicationResponse(BaseModel):
    """Immutable workspace publication listing row."""

    id: str
    schema_version: int
    status: str
    manifest_hash: str
    active: bool
    asset_count: int
    catalog_count: int
    created_at: str


class WorkspacePublicationCreateResponse(BaseModel):
    """Created staged publication summary."""

    publication_id: str
    asset_count: int
    catalog_count: int
    manifest_hash: str


class PublicationActivationResponse(BaseModel):
    """Activation result bound to one immutable generation."""

    publication_id: str


class AuditEventResponse(BaseModel):
    """Redacted audit event safe for management clients."""

    id: str
    actor: str
    action: str
    resource_type: str
    resource_id: str
    outcome: str
    details: dict[str, Any]
    correlation_id: str | None = None
    created_at: str


class AuditEventPageResponse(BaseModel):
    """Keyset-paginated redacted audit events."""

    items: list[AuditEventResponse]
    next_cursor: str | None = None


class PolicyVersionResponse(BaseModel):
    """Immutable published policy history row."""

    asset_id: str
    asset_name: str
    catalog: str
    target: str
    policy_version: int
    active: bool
    created_at: str


class PolicyVersionPageResponse(BaseModel):
    """Keyset-paginated immutable policy history."""

    items: list[PolicyVersionResponse]
    next_cursor: str | None = None


class PolicyVersionDetailResponse(BaseModel):
    """Published policy body without compiled catalog configuration."""

    asset_id: str
    policy_version: int
    rules: list[dict[str, Any]]


class PolicyVersionCreateResponse(BaseModel):
    """Result of publishing one asset policy version."""

    asset_id: str
    policy_version: int


class PolicyDraftResponse(BaseModel):
    """Revisioned policy draft returned to editors and reviewers."""

    id: str | None = None
    asset_id: str
    author_principal: str
    revision: int
    base_policy_version: int
    rules: list[dict[str, Any]]
    content_hash: str
    created_at: str | None = None
    updated_at: str | None = None


class PolicyEvaluationResponse(BaseModel):
    """Bounded server-side policy evaluation evidence."""

    status: Literal["completed"]
    decision: Literal["allow", "deny"]
    allowed_columns: list[str]
    masks: list[dict[str, Any]]
    row_filter: str | None = None
    input_rows: int
    output_rows: int
    schema_text: str = Field(alias="schema")
    rows: list[dict[str, Any]]
    evidence: dict[str, Any]

    model_config = ConfigDict(populate_by_name=True)


class PolicyReviewResponse(PolicyEvaluationResponse):
    """Evaluation evidence plus optional server review authority."""

    review_token: str | None = None
    review_expires_at: int | None = None
    review_draft_id: str | None = None
    review_draft_author: str | None = None
    reviewer: str | None = None


class PolicyOperationResponse(BaseModel):
    """Caller-scoped idempotent publication operation."""

    id: str
    status: str
    result: PolicyVersionCreateResponse


class AuthProviderResponse(BaseModel):
    """Redacted authentication-provider record returned to management clients."""

    id: str
    ordinal: int
    module: str
    args: dict[str, Any]
    enabled: bool
    revision: int


class AuthProviderRevisionResponse(BaseModel):
    revision: int


class CatalogRequest(StrictModel):
    """Catalog configuration request.

    Example:
        ```python
        CatalogRequest(module="iceberg", options={"uri": "sqlite:///catalog.db"})
        ```
    """

    module: str = Field(
        min_length=1,
        max_length=256,
        pattern=r"^[a-z][a-z0-9_.-]{0,63}$|^dal_obscura\.[A-Za-z0-9_.-]{1,255}$",
    )
    options: dict[str, Any] = Field(default_factory=dict)
    expected_revision: int | None = Field(default=None, ge=0)


class AssetRequest(StrictModel):
    """Governed asset backend binding request.

    Example:
        ```python
        AssetRequest(backend="iceberg", table_identifier="default.orders")
        ```
    """

    backend: str = Field(
        default="iceberg",
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_.-]{0,63}$",
    )
    table_identifier: str = Field(min_length=1, max_length=1_024)
    options: dict[str, Any] = Field(default_factory=dict)
    expected_revision: int | None = Field(default=None, ge=0)


class PolicyDraftRequest(StrictModel):
    """Revision-preconditioned policy draft replacement."""

    expected_revision: int = Field(ge=0)
    rules: list[dict[str, Any]] = Field(max_length=100)


class PolicyVersionPublishRequest(StrictModel):
    """Optional generation preconditions for publishing one asset draft."""

    draft_id: UUID | None = None
    expected_draft_revision: int | None = Field(default=None, ge=0)
    expected_publication_id: UUID | None = None
    review_token: str | None = Field(default=None, min_length=16, max_length=4096)


class PolicyRestoreRequest(StrictModel):
    """Revision-preconditioned request to restore immutable policy history."""

    expected_revision: int = Field(ge=0)


class PublicationActivationRequest(StrictModel):
    """Optional active-generation precondition for workspace activation."""

    expected_publication_id: UUID | None = None


class PolicyEvaluationRequest(StrictModel):
    """Bounded synthetic rows for server-side DuckDB policy evaluation."""

    draft_id: UUID | None = None
    draft_revision: int | None = Field(default=None, ge=0)
    principal: str = Field(min_length=1)
    groups: list[str] = Field(default_factory=list, max_length=64)
    claims: dict[str, object] = Field(default_factory=dict)
    columns: list[str] = Field(default_factory=list, max_length=512)

    # ``None`` means use the documented synthetic fixture.  An explicit empty
    # list is a real zero-row evaluation and must preserve the output schema.
    rows: list[dict[str, object]] | None = Field(default=None, max_length=100)


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
    capability: Literal["read", "edit", "publish", "grant"]


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
    path: list[str] | None = Field(default=None, min_length=1, max_length=64)
    type: str = Field(default="string", min_length=1)
    nullable: bool = True


class AssetSchemaFieldsRequest(StrictModel):
    """Asset schema-field replacement request.

    Example:
        ```python
        AssetSchemaFieldsRequest(fields=[AssetSchemaFieldRequest(name="id")])
        ```
    """

    fields: list[AssetSchemaFieldRequest] = Field(default_factory=list, max_length=5_000)
    expected_revision: int | None = Field(default=None, ge=0)


class AuthProvidersRequest(StrictModel):
    """Authentication provider-chain replacement request.

    Example:
        ```python
        AuthProvidersRequest(providers=[{"module": "example.Provider", "args": {}}])
        ```
    """

    providers: list[dict[str, Any]] = Field(max_length=16)
    expected_revision: int | None = Field(default=None, ge=0)


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
