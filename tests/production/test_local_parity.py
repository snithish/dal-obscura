"""Secure-local topology parity checks.

These checks validate the versioned profile contract without pretending to be a
Docker/TLS/OIDC runtime drill.  The latter remains an operator-enabled gate.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[2]
PRODUCTION = ROOT / "deployment" / "production"
LOCAL = ROOT / "deployment" / "local-secure"


def test_secure_local_profile_layers_the_production_security_boundary() -> None:
    production = (PRODUCTION / "compose.yaml").read_text(encoding="utf-8")
    local = (LOCAL / "compose.yaml").read_text(encoding="utf-8")
    runbook = (LOCAL / "run").read_text(encoding="utf-8")
    env_example = (LOCAL / ".env.example").read_text(encoding="utf-8")

    assert "services:" in production
    for service in ("migrate:", "postgres:", "control-plane:", "data-plane:", "ui:"):
        assert f"  {service}" in production
    assert "127.0.0.1:8815:8815" in local
    assert "127.0.0.1:8816:8816" in local
    assert "127.0.0.1:8443:8443" in local
    assert "condition: service_healthy" in local
    assert "networks:" in local and "internal: true" in local
    assert "read_only: true" in local
    assert 'cap_drop: ["ALL"]' in local
    assert "../production/compose.yaml" in runbook
    assert "validate_profile" in runbook
    assert "DAL_OBSCURA_CONTROL_PLANE_PROFILE=production" in env_example
    assert "DAL_OBSCURA_CONTROL_PLANE_BOOTSTRAP_ENABLED=false" in env_example
    assert "DAL_OBSCURA_PLUGIN_LOCK_FILE" in env_example
    assert "@sha256:REPLACE_WITH_VERIFIED_DIGEST" in env_example


def test_secure_local_runbook_fails_closed_before_compose_operations() -> None:
    runbook = (LOCAL / "run").read_text(encoding="utf-8")

    validation = runbook.split("validate_profile()", 1)[1].split("usage()", 1)[0]
    assert "REPLACE_WITH" in validation
    assert "Missing readable TLS material" in validation
    assert "validate_profile; compose config --quiet" in runbook
    assert "validate_profile; compose up -d --wait" in runbook
