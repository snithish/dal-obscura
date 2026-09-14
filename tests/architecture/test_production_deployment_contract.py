from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[2]
PRODUCTION = ROOT / "deployment" / "production"


def test_production_reference_contains_immutable_and_private_topology() -> None:
    compose = (PRODUCTION / "compose.yaml").read_text(encoding="utf-8")
    env_example = (PRODUCTION / ".env.example").read_text(encoding="utf-8")
    readme = (PRODUCTION / "README.md").read_text(encoding="utf-8")
    role_init = (PRODUCTION / "postgres-init" / "01-roles.sh").read_text(encoding="utf-8")

    assert "${DAL_OBSCURA_IMAGE:?set DAL_OBSCURA_IMAGE}" in compose
    assert "${DAL_OBSCURA_UI_IMAGE:?set DAL_OBSCURA_UI_IMAGE}" in compose
    assert "${DAL_OBSCURA_POSTGRES_IMAGE:?set DAL_OBSCURA_POSTGRES_IMAGE}" in compose
    assert '"127.0.0.1:8080:8080"' in compose
    control_block = compose.split("  control-plane:", 1)[1].split("  data-plane:", 1)[0]
    data_block = compose.split("  data-plane:", 1)[1].split("  ui:", 1)[0]
    assert "condition: service_completed_successfully" in control_block
    assert "condition: service_completed_successfully" in data_block
    assert "postgres-grants:" in compose
    assert (
        "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.data_plane_tickets "
        "TO dal_obscura_reader"
    ) in compose
    assert "read_only: true" in compose
    assert 'cap_drop: ["ALL"]' in compose
    assert "internal: true" in compose
    assert "flight_cert:" in compose
    assert "condition: service_healthy" in compose
    assert "127.0.0.1:8820/readyz" in compose
    assert "127.0.0.1:8816/readyz" in compose
    postgres_block = compose.split("  migrate:", 1)[0]
    assert "env_file: .env" not in postgres_block
    assert "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN" not in postgres_block
    assert 'user: "10001:10001"' in compose
    assert 'memory: "2G"' in compose
    assert "DAL_OBSCURA_CONTROL_PLANE_PROFILE=production" in env_example
    assert "DAL_OBSCURA_CONTROL_PLANE_REVIEW_SECRET=" in env_example
    assert "DAL_OBSCURA_CONTROL_PLANE_REVIEW_SECRET" in compose
    assert "DAL_OBSCURA_TLS_VERIFY_CLIENT=true" in env_example
    assert "DAL_OBSCURA_POSTGRES_IMAGE=postgres@sha256:" in env_example
    assert "DAL_OBSCURA_MIGRATION_DATABASE_URL" in compose
    assert "DAL_OBSCURA_CONTROL_PLANE_DATABASE_URL" in compose
    assert "DAL_OBSCURA_DATA_PLANE_DATABASE_URL" in compose
    assert "DAL_OBSCURA_MIGRATION_DATABASE_URL" in env_example
    assert "DAL_OBSCURA_CONTROL_PLANE_DATABASE_URL" in env_example
    assert "DAL_OBSCURA_DATA_PLANE_DATABASE_URL" in env_example
    assert "DAL_OBSCURA_PLUGIN_LOCK_FILE" in env_example
    assert "DAL_OBSCURA_PLUGIN_LOCK_FILE" in control_block
    assert "DAL_OBSCURA_PLUGIN_LOCK_FILE" in data_block
    assert "DAL_OBSCURA_SECRET_PROVIDER_CONFIG" in env_example
    assert "DAL_OBSCURA_SECRET_PROVIDER_CONFIG" in control_block
    assert "DAL_OBSCURA_SECRET_PROVIDER_CONFIG" in data_block
    assert "Provision separate PostgreSQL roles" in readme
    assert "./postgres-init:/docker-entrypoint-initdb.d:ro" in compose
    assert "DAL_OBSCURA_MIGRATION_DB_PASSWORD" in compose
    assert "DAL_OBSCURA_CONTROL_PLANE_DB_PASSWORD" in compose
    assert "DAL_OBSCURA_DATA_PLANE_DB_PASSWORD" in compose
    assert "CREATE ROLE dal_obscura_migrator" in role_init
    assert "CREATE ROLE dal_obscura_control" in role_init
    assert "CREATE ROLE dal_obscura_reader" in role_init
    assert "GRANT USAGE, CREATE ON SCHEMA public TO dal_obscura_migrator" in role_init
    assert "GRANT SELECT ON TABLES TO dal_obscura_reader" in role_init
    assert "Routine restart" in readme
    assert "does not seed" in readme
    assert "republish" in readme


def test_recovery_scripts_require_encryption_and_isolated_restore_confirmation() -> None:
    backup = Path("scripts/backup_postgres.sh").read_text()
    restore = Path("scripts/restore_postgres.sh").read_text()

    assert "pg_dump --format=custom" in backup
    assert "age --encrypt" in backup
    assert 'sha256sum "$temporary"' in backup
    assert "digest=$(sha256sum" in backup
    assert "${output}.sha256" in backup
    assert "refusing to overwrite existing backup" in backup
    assert "age --decrypt" in restore
    assert "sha256sum --check" in restore
    assert "backup checksum verification failed" in restore
    assert "age identity is not readable" in restore
    assert "pg_restore --single-transaction" in restore
    assert "I_UNDERSTAND_ISOLATED_RESTORE" in restore
    assert "dal-obscura-maintenance invalidate-access" in restore
