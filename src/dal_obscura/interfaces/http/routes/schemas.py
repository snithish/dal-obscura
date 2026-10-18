"""Strict request base and shared HTTP error envelopes."""

from __future__ import annotations

from pydantic import (
    BaseModel,
    ConfigDict,
)


class StrictModel(BaseModel):
    """Base model that rejects unknown request fields.

    Example:
        ```python
        class RequestModel(StrictModel):
            name: str
        ```
    """

    model_config = ConfigDict(extra="forbid")


class ApiFieldError(BaseModel):
    """Validation evidence without submitted values or secrets."""

    field: str
    message: str
    type: str


class ApiError(BaseModel):
    """Machine-readable error metadata shared by all HTTP rejections."""

    code: str
    message: str
    request_id: str
    current_revision: int | None = None
    field_errors: list[ApiFieldError] | None = None


class ApiErrorResponse(BaseModel):
    """Public HTTP error envelope documented for generated clients."""

    error: ApiError
