#!/usr/bin/env sh
set -eu
psql --username "$POSTGRES_USER" --dbname postgres --set ON_ERROR_STOP=1 \
  --set kc_password="$KEYCLOAK_DB_PASSWORD" --set iceberg_password="$ICEBERG_DB_PASSWORD" \
  --set migration_password="$MIGRATION_DB_PASSWORD" --set control_password="$APP_DB_PASSWORD" \
  --set data_password="$DATA_DB_PASSWORD" <<'SQL'
CREATE ROLE keycloak LOGIN PASSWORD :'kc_password';
CREATE DATABASE keycloak OWNER keycloak;
CREATE ROLE iceberg LOGIN PASSWORD :'iceberg_password';
CREATE DATABASE iceberg OWNER iceberg;
CREATE ROLE dal_obscura_migrator LOGIN PASSWORD :'migration_password';
CREATE ROLE dal_obscura_control LOGIN PASSWORD :'control_password';
CREATE ROLE dal_obscura_reader LOGIN PASSWORD :'data_password';
CREATE DATABASE obscura OWNER dal_obscura_migrator;
REVOKE CONNECT ON DATABASE obscura FROM PUBLIC;
GRANT CONNECT ON DATABASE obscura TO dal_obscura_control, dal_obscura_reader;
\connect obscura
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO dal_obscura_control, dal_obscura_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dal_obscura_migrator IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO dal_obscura_control;
ALTER DEFAULT PRIVILEGES FOR ROLE dal_obscura_migrator IN SCHEMA public
  GRANT SELECT ON TABLES TO dal_obscura_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dal_obscura_migrator IN SCHEMA public
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO dal_obscura_control;
ALTER DEFAULT PRIVILEGES FOR ROLE dal_obscura_migrator IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO dal_obscura_reader;
SQL
