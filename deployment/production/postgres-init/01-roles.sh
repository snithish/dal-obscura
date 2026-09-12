#!/usr/bin/env bash
set -euo pipefail

: "${DAL_OBSCURA_MIGRATION_DB_PASSWORD:?set DAL_OBSCURA_MIGRATION_DB_PASSWORD}"
: "${DAL_OBSCURA_CONTROL_PLANE_DB_PASSWORD:?set DAL_OBSCURA_CONTROL_PLANE_DB_PASSWORD}"
: "${DAL_OBSCURA_DATA_PLANE_DB_PASSWORD:?set DAL_OBSCURA_DATA_PLANE_DB_PASSWORD}"

# This script runs only on first initialization of the PostgreSQL volume. Role
# grants are intentionally narrow; migrations own schema changes, while the
# applications receive only the data operations needed after migration.
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v db_name="$POSTGRES_DB" \
  -v migration_password="$DAL_OBSCURA_MIGRATION_DB_PASSWORD" \
  -v control_password="$DAL_OBSCURA_CONTROL_PLANE_DB_PASSWORD" \
  -v data_password="$DAL_OBSCURA_DATA_PLANE_DB_PASSWORD" <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dal_obscura_migrator') THEN
    CREATE ROLE dal_obscura_migrator LOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dal_obscura_control') THEN
    CREATE ROLE dal_obscura_control LOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dal_obscura_reader') THEN
    CREATE ROLE dal_obscura_reader LOGIN;
  END IF;
END
$$;
ALTER ROLE dal_obscura_migrator PASSWORD :'migration_password';
ALTER ROLE dal_obscura_control PASSWORD :'control_password';
ALTER ROLE dal_obscura_reader PASSWORD :'data_password';

GRANT CONNECT ON DATABASE :"db_name" TO dal_obscura_migrator;
GRANT CONNECT ON DATABASE :"db_name" TO dal_obscura_control;
GRANT CONNECT ON DATABASE :"db_name" TO dal_obscura_reader;
GRANT USAGE, CREATE ON SCHEMA public TO dal_obscura_migrator;
GRANT USAGE ON SCHEMA public TO dal_obscura_control;
GRANT USAGE ON SCHEMA public TO dal_obscura_reader;

-- Migration-created objects are owned by the migrator. Control-plane writes
-- are granted through default privileges; data-plane starts read-only.
ALTER DEFAULT PRIVILEGES FOR ROLE dal_obscura_migrator IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dal_obscura_control;
ALTER DEFAULT PRIVILEGES FOR ROLE dal_obscura_migrator IN SCHEMA public
  GRANT SELECT ON TABLES TO dal_obscura_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dal_obscura_migrator IN SCHEMA public
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO dal_obscura_control;
ALTER DEFAULT PRIVILEGES FOR ROLE dal_obscura_migrator IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO dal_obscura_reader;

SQL
