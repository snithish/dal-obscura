from __future__ import annotations

from typing import Any, cast
from urllib.parse import parse_qs
from uuid import UUID

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TenantRequest(StrictModel):
    slug: str = Field(min_length=1)
    display_name: str = Field(min_length=1)


class CellRequest(StrictModel):
    name: str = Field(min_length=1)
    region: str = Field(min_length=1)


class TenantCellRequest(CellRequest):
    shard_key: str = Field(default="default", min_length=1)


class TenantCellAssignmentRequest(StrictModel):
    cell_id: UUID
    shard_key: str = Field(default="default", min_length=1)


class CellTenantRequest(StrictModel):
    shard_key: str = Field(default="default", min_length=1)


class RuntimeSettingsRequest(StrictModel):
    ticket_ttl_seconds: int = Field(gt=0)
    max_tickets: int = Field(gt=0)
    max_ticket_exchanges: int = Field(gt=0)


class CatalogRequest(StrictModel):
    module: str = Field(min_length=1)
    options: dict[str, Any] = Field(default_factory=dict)


class AssetRequest(StrictModel):
    backend: str = Field(min_length=1)
    table_identifier: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class PolicyRulesRequest(StrictModel):
    rules: list[dict[str, Any]]


class PolicyPreviewRequest(StrictModel):
    principal: str = Field(min_length=1)
    groups: list[str] = Field(default_factory=list)
    claims: dict[str, object] = Field(default_factory=dict)


class AssetOwnersRequest(StrictModel):
    owners: list[str] = Field(default_factory=list)


class AssetSchemaFieldRequest(StrictModel):
    name: str = Field(min_length=1)
    type: str = Field(default="string", min_length=1)
    nullable: bool = True


class AssetSchemaFieldsRequest(StrictModel):
    fields: list[AssetSchemaFieldRequest] = Field(default_factory=list)


class AuthProvidersRequest(StrictModel):
    providers: list[dict[str, Any]]


class DemoLoginRequest(StrictModel):
    login_hint: str = Field(min_length=1)


async def request_payload(request: Request) -> dict[str, object]:
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
