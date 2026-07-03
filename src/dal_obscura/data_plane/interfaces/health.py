from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol, cast

from fastapi import FastAPI
from fastapi.responses import JSONResponse


class RuntimeStore(Protocol):
    def get_runtime(self) -> object: ...


HealthPayload = Mapping[str, object]


def create_health_app(*, readiness: Callable[[], HealthPayload]) -> FastAPI:
    app = FastAPI(title="dal-obscura data plane health")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", response_model=None)
    def readyz() -> HealthPayload | JSONResponse:
        try:
            payload = dict(readiness())
        except Exception as exc:
            payload = {"status": "not_ready", "reason": str(exc)}
        if payload.get("status") != "ready":
            return JSONResponse(status_code=503, content=payload)
        return payload

    return app


def published_runtime_readiness(store: RuntimeStore) -> dict[str, object]:
    checks: dict[str, str] = {}
    try:
        runtime = store.get_runtime()
    except Exception as exc:
        return {
            "status": "not_ready",
            "checks": {
                "active_publication": "failed",
                "runtime": "failed",
                "auth_chain": "unknown",
            },
            "reason": str(exc),
        }

    publication_id = getattr(runtime, "publication_id", None)
    checks["active_publication"] = "ok" if publication_id else "missing"
    ticket = getattr(runtime, "ticket", {})
    checks["runtime"] = (
        "ok" if isinstance(ticket, Mapping) and ticket else "missing_ticket_settings"
    )

    auth_chain = getattr(runtime, "auth_chain", {})
    providers = _providers(auth_chain)
    checks["auth_chain"] = "ok" if _has_enabled_provider(providers) else "missing_enabled_provider"

    status = "ready" if all(value == "ok" for value in checks.values()) else "not_ready"
    payload: dict[str, object] = {"status": status, "checks": checks}
    if publication_id:
        payload["publication_id"] = str(publication_id)
    return payload


def _providers(auth_chain: object) -> list[Mapping[str, Any]]:
    if not isinstance(auth_chain, Mapping):
        return []
    raw_auth_chain = cast(Mapping[str, object], auth_chain)
    raw_providers = raw_auth_chain.get("providers")
    if not isinstance(raw_providers, list):
        return []
    return [cast(Mapping[str, Any], item) for item in raw_providers if isinstance(item, Mapping)]


def _has_enabled_provider(providers: list[Mapping[str, Any]]) -> bool:
    return any(bool(provider.get("enabled", True)) for provider in providers)
