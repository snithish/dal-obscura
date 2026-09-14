"""Revisioned policy draft, evaluation, review, and publication routes.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    AuditEventPageResponse,
    AuditEventResponse,
    PolicyDraftRequest,
    PolicyEvaluationRequest,
    PolicyRestoreRequest,
    PolicyVersionCreateResponse,
    PolicyVersionDetailResponse,
    PolicyVersionPageResponse,
    PolicyVersionPublishRequest,
    PolicyVersionResponse,
)


def router(deps: ControlPlaneDeps) -> APIRouter:  # noqa: C901
    """Builds asset policy routes.

    Example:
        ```python
        policy_router = router(deps)
        ```
    """

    api = APIRouter()

    @api.get("/v1/assets/{asset_id}/draft")
    def get_policy_draft(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.get_policy_draft(asset_id, actor))

    @api.put("/v1/assets/{asset_id}/draft")
    def save_policy_draft(
        asset_id: UUID,
        request: PolicyDraftRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.save_policy_draft(
                asset_id,
                actor,
                expected_revision=request.expected_revision,
                rules=request.rules,
            )
        )

    @api.get("/v1/assets/{asset_id}/draft/{draft_id}")
    def get_policy_draft_by_id(
        asset_id: UUID,
        draft_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.get_policy_draft_by_id(asset_id, draft_id, actor)
        )

    @api.post("/v1/assets/{asset_id}/policy-evaluate")
    def evaluate_asset_policy(
        asset_id: UUID,
        request: PolicyEvaluationRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.evaluate_asset_policy(
                asset_id,
                actor,
                principal=request.principal,
                groups=request.groups,
                claims=request.claims,
                rows=request.rows,
                draft_id=request.draft_id,
                draft_revision=request.draft_revision,
            )
        )

    @api.post("/v1/assets/{asset_id}/policy-review")
    def review_asset_policy(
        asset_id: UUID,
        request: PolicyEvaluationRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.review_asset_policy(
                asset_id,
                actor,
                principal=request.principal,
                groups=request.groups,
                claims=request.claims,
                rows=request.rows,
                draft_id=request.draft_id,
                draft_revision=request.draft_revision,
            )
        )

    @api.post("/v1/assets/{asset_id}/policy-versions", response_model=PolicyVersionCreateResponse)
    def create_asset_policy_version(
        asset_id: UUID,
        request: PolicyVersionPublishRequest | None = None,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> PolicyVersionCreateResponse:
        if idempotency_key is not None and len(idempotency_key.strip()) > 128:
            raise HTTPException(status_code=422, detail="Idempotency-Key is too long")
        return deps.with_service(
            lambda service: service.create_asset_policy_version(
                asset_id=asset_id,
                actor=actor,
                expected_draft_revision=(
                    None if request is None else request.expected_draft_revision
                ),
                draft_id=None if request is None else request.draft_id,
                expected_publication_id=(
                    None if request is None else request.expected_publication_id
                ),
                review_token=None if request is None else request.review_token,
                idempotency_key=idempotency_key.strip() if idempotency_key else None,
            )
        )

    @api.get("/v1/assets/{asset_id}/policy-versions", response_model=list[PolicyVersionResponse])
    def list_asset_policy_version_history(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> list[PolicyVersionResponse]:
        return deps.with_service(
            lambda service: service.list_asset_policy_version_history(asset_id, actor=actor)
        )

    @api.get("/v1/assets/{asset_id}/policy-operations/{idempotency_key}")
    def get_policy_operation(
        asset_id: UUID,
        idempotency_key: str,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        key = idempotency_key.strip()
        if not 1 <= len(key) <= 128:
            raise HTTPException(
                status_code=422,
                detail="Idempotency key must contain 1-128 characters",
            )
        return deps.with_service(
            lambda service: service.get_publication_operation(
                asset_id,
                key,
                actor=actor,
            )
        )

    @api.get(
        "/v1/assets/{asset_id}/policy-versions/{policy_version}",
        response_model=PolicyVersionDetailResponse,
    )
    def get_asset_policy_version(
        asset_id: UUID,
        policy_version: int,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> PolicyVersionDetailResponse:
        return deps.with_service(
            lambda service: service.get_asset_policy_version(
                asset_id,
                policy_version,
                actor=actor,
            )
        )

    @api.post("/v1/assets/{asset_id}/policy-versions/{policy_version}/restore")
    def restore_policy_version(
        asset_id: UUID,
        policy_version: int,
        request: PolicyRestoreRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.restore_policy_version(
                asset_id,
                policy_version,
                actor=actor,
                expected_revision=request.expected_revision,
            )
        )

    @api.get("/v1/policy-versions", response_model=list[PolicyVersionResponse])
    def list_policy_version_history(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> list[PolicyVersionResponse]:
        return deps.with_service(lambda service: service.list_policy_version_history(actor=actor))

    @api.get("/v1/policy-versions/page", response_model=PolicyVersionPageResponse)
    def list_policy_version_history_page(
        limit: int = Query(default=50, ge=1, le=200),
        cursor: str | None = Query(default=None, max_length=512),
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> PolicyVersionPageResponse:
        return deps.with_service(
            lambda service: service.list_policy_version_history_page(
                actor=actor,
                limit=limit,
                cursor=cursor,
            )
        )

    @api.get("/v1/audit/events", response_model=list[AuditEventResponse])
    def list_audit_events(
        asset_id: UUID | None = None,
        limit: int = Query(default=100, ge=1, le=200),
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> list[AuditEventResponse]:
        return deps.with_service(
            lambda service: service.list_audit_events(
                actor=actor,
                asset_id=asset_id,
                limit=limit,
            )
        )

    @api.get("/v1/audit/events/page", response_model=AuditEventPageResponse)
    def list_audit_events_page(
        asset_id: UUID | None = None,
        actor_filter: str | None = Query(default=None, alias="actor", max_length=200),
        action: str | None = Query(default=None, max_length=200),
        resource_type: str | None = Query(default=None, max_length=48),
        outcome: str | None = Query(default=None, max_length=24),
        correlation_id: str | None = Query(default=None, max_length=96),
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = Query(default=100, ge=1, le=200),
        cursor: str | None = Query(default=None, max_length=512),
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AuditEventPageResponse:
        return deps.with_service(
            lambda service: service.list_audit_events_page(
                actor=actor,
                asset_id=asset_id,
                actor_filter=actor_filter,
                action=action,
                resource_type=resource_type,
                outcome=outcome,
                correlation_id=correlation_id,
                created_after=created_after,
                created_before=created_before,
                limit=limit,
                cursor=cursor,
            )
        )

    return api
