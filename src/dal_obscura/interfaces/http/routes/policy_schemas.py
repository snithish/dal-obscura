"""HTTP policy request and response contracts."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    StringConstraints,
    model_serializer,
)

from dal_obscura.interfaces.http.routes.schemas import (
    StrictModel,
)


class PolicyMaskSchema(BaseModel):
    """Scalar mask configuration with optional rule-local exemptions."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["null", "redact", "hash", "email", "keep_last", "default"]
    value: str | int | float | bool | None = None
    exempt_principals: list[str] = Field(default_factory=list)

    # A return annotation replaces the model response schema with the serializer type.
    @model_serializer(mode="wrap")
    def omit_absent_optional_metadata(self, handler: SerializerFunctionWrapHandler):
        payload = handler(self)
        if "value" not in self.model_fields_set:
            payload.pop("value", None)
        if "exempt_principals" not in self.model_fields_set:
            payload.pop("exempt_principals", None)
        return payload


class _PolicyRuleFields(BaseModel):
    """Shared rule fields used by strict request and response models."""

    model_config = ConfigDict(extra="forbid")

    ordinal: int | None = Field(default=None, ge=0)
    effect: Literal["allow", "allow_all"] = "allow"
    name: str = Field(default="", max_length=160)
    description: str = Field(default="", max_length=2000)
    principals: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    masks: dict[str, PolicyMaskSchema] = Field(default_factory=dict)
    row_filter: str | None = None
    when: dict[str, str | list[str]] = Field(default_factory=dict)


class PolicyRuleSchema(_PolicyRuleFields):
    """One explicit grant returned with its canonical storage identity."""

    id: str | None = None
    asset_id: str | None = None


class PolicyRuleRequest(_PolicyRuleFields):
    """One editable grant supplied by a policy author."""


class EvaluationMaskResponse(BaseModel):
    column: str
    type: Literal["null", "redact", "hash", "email", "keep_last", "default"]
    value: str | int | float | bool | None = None


class PolicyEvaluationResponse(BaseModel):
    """Bounded server-side policy evaluation evidence."""

    status: Literal["completed"]
    decision: Literal["allow", "deny"]
    allowed_columns: list[str]
    masks: list[EvaluationMaskResponse]
    row_filter: str | None = None
    input_rows: int
    output_rows: int
    schema_text: str = Field(alias="schema")
    policy_revision: int
    rows: list[dict[str, Any]]
    evidence: dict[str, Any]


class PolicyRulesRequest(StrictModel):
    """Optimistic direct policy replacement."""

    expected_revision: int = Field(ge=0)
    rules: list[PolicyRuleRequest] = Field(max_length=100)
    revoke_existing_tokens: bool = False


class PolicyMutationResponse(BaseModel):
    asset_id: str
    policy_revision: int
    revoked_token_count: int


class PolicyEvaluationRequest(StrictModel):
    """Bounded synthetic rows for server-side DuckDB policy evaluation."""

    principal: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    groups: list[Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]] = Field(
        default_factory=list, max_length=64
    )
    claims: dict[str, object] = Field(default_factory=dict)
    provider_ordinal: int | None = Field(default=None, ge=1, le=1000)

    # ``None`` means use the documented synthetic fixture.  An explicit empty
    # list is a real zero-row evaluation and must preserve the output schema.
    rows: list[dict[str, object]] | None = Field(default=None, max_length=100)
