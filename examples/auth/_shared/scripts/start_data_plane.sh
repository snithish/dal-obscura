#!/usr/bin/env sh
set -eu

. /workspace/runtime/data-plane.env

if [ -n "${DAL_OBSCURA_TLS_CERT_FILE:-}" ]; then
  DAL_OBSCURA_TLS_CERT="$(cat "$DAL_OBSCURA_TLS_CERT_FILE")"
  export DAL_OBSCURA_TLS_CERT
fi

if [ -n "${DAL_OBSCURA_TLS_KEY_FILE:-}" ]; then
  DAL_OBSCURA_TLS_KEY="$(cat "$DAL_OBSCURA_TLS_KEY_FILE")"
  export DAL_OBSCURA_TLS_KEY
fi

if [ -n "${DAL_OBSCURA_TLS_CLIENT_CA_FILE:-}" ]; then
  DAL_OBSCURA_TLS_CLIENT_CA="$(cat "$DAL_OBSCURA_TLS_CLIENT_CA_FILE")"
  export DAL_OBSCURA_TLS_CLIENT_CA
fi

exec /workspace/.venv/bin/dal-obscura
