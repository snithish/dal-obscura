"""Read-service process entry point."""

from __future__ import annotations

import sys

from dal_obscura.bootstrap.read import run_read_service_from_environment


def main() -> None:
    """Parse help and environment, then enter owned read-service startup."""
    if any(argument in {"-h", "--help"} for argument in sys.argv[1:]):
        print(_HELP_TEXT)
        return
    run_read_service_from_environment()


_HELP_TEXT = """dal-obscura — governed Arrow Flight data plane

Starts the data plane from an already migrated database with live control-plane
configuration. Runtime settings are read from DAL_OBSCURA_* environment variables.

Required variables:
  DAL_OBSCURA_DATABASE_URL   PostgreSQL (production) or SQLite (local) URL
  DAL_OBSCURA_TICKET_SECRET  HMAC ticket secret

Common variables:
  DAL_OBSCURA_LOCATION           Flight location (default: grpc://0.0.0.0:8815)
  DAL_OBSCURA_DATA_PLANE_PROFILE  local or production
  DAL_OBSCURA_TLS_CERT / DAL_OBSCURA_TLS_KEY  TLS files in production

Use dal-obscura-migrate to initialize the schema before starting the service.
"""
