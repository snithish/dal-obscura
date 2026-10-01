#!/usr/bin/env sh
set -eu
# Values are psql variables, never interpolated into SQL syntax.
psql --username "$POSTGRES_USER" --dbname postgres --set ON_ERROR_STOP=1 \
  --set kc_password="$KEYCLOAK_DB_PASSWORD" --set app_password="$APP_DB_PASSWORD" \
  --set iceberg_password="$ICEBERG_DB_PASSWORD" <<'SQL'
CREATE USER keycloak WITH PASSWORD :'kc_password';
CREATE DATABASE keycloak OWNER keycloak;
CREATE USER obscura WITH PASSWORD :'app_password';
CREATE DATABASE obscura OWNER obscura;
CREATE USER iceberg WITH PASSWORD :'iceberg_password';
CREATE DATABASE iceberg OWNER iceberg;
SQL
