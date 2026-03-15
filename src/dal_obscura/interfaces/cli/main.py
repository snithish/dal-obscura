from __future__ import annotations

import argparse
import json
import logging

from dal_obscura.application.use_cases import FetchStreamUseCase, PlanAccessUseCase
from dal_obscura.infrastructure.adapters import (
    AuthConfig,
    DefaultIdentityAdapter,
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
    HmacTicketCodecAdapter,
    IcebergBackend,
    IcebergConfig,
    PolicyFileAuthorizer,
)
from dal_obscura.interfaces.flight import DataAccessFlightService
from dal_obscura.logging_config import LoggingConfig, setup_logging

LOGGER = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="dal-obscura Flight service")
    parser.add_argument("--location", default="grpc://0.0.0.0:8815")
    parser.add_argument("--policy", required=True)
    parser.add_argument("--ticket-secret", required=True)
    parser.add_argument("--ticket-ttl", type=int, default=900)
    parser.add_argument("--max-tickets", type=int, default=64)
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

    identity = DefaultIdentityAdapter(
        AuthConfig(
            api_keys=json.loads(args.api_keys),
            jwt_secret=args.jwt_secret,
            jwt_issuer=args.jwt_issuer,
            jwt_audience=args.jwt_audience,
        )
    )
    authorizer = PolicyFileAuthorizer(args.policy)
    backend = IcebergBackend(
        IcebergConfig(catalog_name=args.catalog, catalog_options=json.loads(args.catalog_options))
    )
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter(args.ticket_secret)

    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        planning_backend=backend,
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=args.ticket_ttl,
        max_tickets=args.max_tickets,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
        planning_backend=backend,
        read_backend=backend,
        masking=masking,
        row_transform=row_transform,
        ticket_codec=ticket_codec,
    )
    server = DataAccessFlightService(
        location=args.location,
        plan_access_use_case=plan_access,
        fetch_stream_use_case=fetch_stream,
    )
    server.serve()
