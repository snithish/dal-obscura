from __future__ import annotations

import argparse
import json
import logging

from dal_obscura.auth import AuthConfig, DefaultAuthenticator
from dal_obscura.authorization import PolicyAuthorizer
from dal_obscura.backend.iceberg import IcebergBackend, IcebergConfig
from dal_obscura.logging_config import LoggingConfig, setup_logging
from dal_obscura.masking import DefaultMaskApplier
from dal_obscura.service import DataAccessFlightService, ServerConfig

LOGGER = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="dal-obscura Flight service")
    parser.add_argument("--location", default="grpc://0.0.0.0:8815")
    parser.add_argument("--policy", required=True)
    parser.add_argument("--ticket-secret", required=True)
    parser.add_argument("--ticket-ttl", type=int, default=900)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--catalog-options", default="{}")
    parser.add_argument("--api-keys", default="{}")
    parser.add_argument("--jwt-secret")
    parser.add_argument("--jwt-issuer")
    parser.add_argument("--jwt-audience")
    parser.add_argument("--log-level", default=None)
    parser.add_argument("--log-json", action="store_true")
    parser.add_argument("--log-plain", action="store_true")
    args = parser.parse_args()

    log_json = args.log_json or (not args.log_plain)
    setup_logging(
        LoggingConfig(
            level=args.log_level or "INFO",
            json=log_json,
        )
    )
    LOGGER.info("Starting dal-obscura service")

    auth_config = AuthConfig(
        api_keys=json.loads(args.api_keys),
        jwt_secret=args.jwt_secret,
        jwt_issuer=args.jwt_issuer,
        jwt_audience=args.jwt_audience,
    )
    backend = IcebergBackend(
        IcebergConfig(catalog_name=args.catalog, catalog_options=json.loads(args.catalog_options))
    )
    server = DataAccessFlightService(
        location=args.location,
        backend=backend,
        config=ServerConfig(
            ticket_secret=args.ticket_secret,
            ticket_ttl_seconds=args.ticket_ttl,
            authenticator=DefaultAuthenticator(auth_config),
            authorizer=PolicyAuthorizer(args.policy),
            mask_applier=DefaultMaskApplier(),
        ),
    )
    server.serve()


if __name__ == "__main__":
    main()
