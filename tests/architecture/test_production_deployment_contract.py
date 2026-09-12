from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[2]
PRODUCTION = ROOT / "deployment" / "production"


def test_production_reference_contains_immutable_and_private_topology() -> None:
    compose = (PRODUCTION / "compose.yaml").read_text(encoding="utf-8")
    env_example = (PRODUCTION / ".env.example").read_text(encoding="utf-8")
    readme = (PRODUCTION / "README.md").read_text(encoding="utf-8")

    assert "${DAL_OBSCURA_IMAGE:?set DAL_OBSCURA_IMAGE}" in compose
    assert "${DAL_OBSCURA_UI_IMAGE:?set DAL_OBSCURA_UI_IMAGE}" in compose
    assert '"127.0.0.1:8080:8080"' in compose
    control_block = compose.split("  control-plane:", 1)[1].split("  data-plane:", 1)[0]
    data_block = compose.split("  data-plane:", 1)[1].split("  ui:", 1)[0]
    assert "condition: service_completed_successfully" not in control_block
    assert "condition: service_completed_successfully" not in data_block
    assert "read_only: true" in compose
    assert 'cap_drop: ["ALL"]' in compose
    assert "internal: true" in compose
    assert "flight_cert:" in compose
    assert "condition: service_healthy" in compose
    postgres_block = compose.split("  migrate:", 1)[0]
    assert "env_file: .env" not in postgres_block
    assert "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN" not in postgres_block
    assert 'user: "10001:10001"' in compose
    assert 'memory: "2G"' in compose
    assert "DAL_OBSCURA_CONTROL_PLANE_PROFILE=production" in env_example
    assert "DAL_OBSCURA_CONTROL_PLANE_REVIEW_SECRET=" in env_example
    assert "DAL_OBSCURA_CONTROL_PLANE_REVIEW_SECRET" in compose
    assert "DAL_OBSCURA_TLS_VERIFY_CLIENT=true" in env_example
    assert "Routine restart" in readme
    assert "does not seed" in readme
    assert "republish" in readme
