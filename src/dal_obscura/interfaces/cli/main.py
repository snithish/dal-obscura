from __future__ import annotations

import argparse
import logging

from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.get_schema import GetSchemaUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.infrastructure.adapters.app_config import load_app_config
from dal_obscura.infrastructure.adapters.catalog_registry import DynamicCatalogRegistry
from dal_obscura.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.infrastructure.adapters.policy_file_authorizer import PolicyFileAuthorizer
from dal_obscura.infrastructure.adapters.service_config import load_catalog_config
from dal_obscura.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter
from dal_obscura.interfaces.flight.server import DataAccessFlightService
from dal_obscura.logging_config import LoggingConfig, setup_logging

LOGGER = logging.getLogger(__name__)


def main() -> None:
    """CLI entry point that wires adapters together and starts the Flight server."""
    parser = argparse.ArgumentParser(description="dal-obscura Flight service")
    parser.add_argument("--app-config", required=True)
    args = parser.parse_args()

    app_config = load_app_config(args.app_config)
    setup_logging(
        LoggingConfig(level=app_config.logging.level, json=app_config.logging.json_output)
    )
    LOGGER.info("Starting dal-obscura service")

    # The CLI is the composition root for the hexagonal application. Everything
    # below is pure wiring so the use cases can stay free of transport details.
    identity = app_config.auth.identity_provider
    authorizer = PolicyFileAuthorizer(app_config.policy_file)
    service_config = load_catalog_config(app_config.catalog_file)
    catalog_registry = DynamicCatalogRegistry(service_config)
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter(app_config.ticket.secret)

    get_schema = GetSchemaUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=catalog_registry,
        masking=masking,
    )
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=catalog_registry,
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=app_config.ticket.ttl_seconds,
        max_tickets=app_config.ticket.max_tickets,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
        masking=masking,
        row_transform=row_transform,
        ticket_codec=ticket_codec,
    )
    server = DataAccessFlightService(
        location=app_config.location,
        get_schema_use_case=get_schema,
        plan_access_use_case=plan_access,
        fetch_stream_use_case=fetch_stream,
    )
    server.serve()
