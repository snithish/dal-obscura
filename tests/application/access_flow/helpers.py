from typing import Any, cast

from dal_obscura.common.access_control.models import AccessDecision, MaskRule, Principal
from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.common.ticket_delivery.models import TicketPayload
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest
from dal_obscura.data_plane.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.data_plane.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.data_plane.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter
from tests.support.arrow import id_region_batch, id_region_schema
from tests.support.use_cases import (
    FakeAuthorizer,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeTicketStore,
    StubTableFormat,
)

AUTHORIZATION_HEADER = AuthenticationRequest(headers={"authorization": "Bearer jwt-token"})


def _build_end_to_end_access_flow(table_format: TableFormat, decision: AccessDecision):
    ticket_codec = HmacTicketCodecAdapter("secret")
    ticket_store = FakeTicketStore()
    masking = DefaultMaskingAdapter()
    authorizer = FakeAuthorizer(decision=decision, current_version=decision.policy_version)
    catalog_registry = FakeCatalogRegistry(table_format)
    principal = Principal(id="user1", groups=[], attributes={})
    plan_access = PlanAccessUseCase(
        identity=FakeIdentity(principal=principal),
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )
    fetch_stream = FetchStreamUseCase(
        identity=FakeIdentity(principal=principal),
        authorizer=authorizer,
        masking=masking,
        row_transform=DuckDBRowTransformAdapter(masking),
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
    )
    return plan_access, fetch_stream


def _build_use_case_dependencies():
    schema = id_region_schema()
    batches = (id_region_batch([1], ["us"]),)
    decision = AccessDecision(
        allowed_columns=["id", "region"],
        masks={"region": MaskRule(type="redact", value="***")},
        row_filter="region = 'us'",
        policy_version=100,
    )
    table_format = StubTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        batches=batches,
    )
    return schema, decision, table_format


def _ticket_store_with(payload: TicketPayload, *, max_exchanges: int = 1) -> FakeTicketStore:
    ticket_store = FakeTicketStore()
    ticket_store.store(payload, max_exchanges=max_exchanges)
    return ticket_store
