"""Validate deterministic local-demo startup dependencies without a daemon."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
KEYCLOAK_READY_CHECK = (
    "{ printf 'HEAD /health/ready HTTP/1.0\\r\\n\\r\\n' >&0; "
    "grep 'HTTP/1.0 200'; } 0<>/dev/tcp/localhost/9000"
)


def _http_ready_check(port: int) -> str:
    return (
        "python -c \"from urllib.request import urlopen; raise "
        f"SystemExit(urlopen('http://127.0.0.1:{port}/readyz', timeout=2).status != 200)\""
    )


def test_demo_services_wait_for_real_readiness_boundaries() -> None:
    """Compose must not provision or expose consumers after mere process start."""
    document = cast(
        dict[str, Any],
        yaml.safe_load((REPOSITORY_ROOT / "examples/demo/keycloak/compose.yaml").read_text()),
    )
    services = cast(dict[str, dict[str, Any]], document["services"])

    keycloak = services["keycloak"]
    assert "--health-enabled=true" in keycloak["command"]
    assert keycloak["healthcheck"]["test"] == [
        "CMD-SHELL",
        KEYCLOAK_READY_CHECK,
    ]

    control_plane = services["control-plane"]
    assert control_plane["depends_on"]["keycloak"]["condition"] == "service_healthy"
    assert control_plane["healthcheck"]["test"] == [
        "CMD-SHELL",
        _http_ready_check(8820),
    ]

    assert services["setup"]["depends_on"]["control-plane"]["condition"] == "service_healthy"
    assert services["setup"]["depends_on"]["keycloak"]["condition"] == "service_healthy"

    data_plane = services["dal-obscura"]
    assert data_plane["depends_on"]["setup"]["condition"] == "service_healthy"
    assert data_plane["depends_on"]["keycloak"]["condition"] == "service_healthy"
    assert data_plane["healthcheck"]["test"] == [
        "CMD-SHELL",
        _http_ready_check(8816),
    ]

    assert services["client"]["depends_on"]["dal-obscura"]["condition"] == "service_healthy"
    assert services["client"]["depends_on"]["keycloak"]["condition"] == "service_healthy"
