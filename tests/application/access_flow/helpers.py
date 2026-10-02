from typing import Any, cast

from dal_obscura.identity.contracts import AuthenticationRequest
from dal_obscura.policy.models import AccessDecision, MaskRule, Principal
from dal_obscura.read.signing import HmacTicketCodecAdapter
from dal_obscura.read.tickets import TicketPayload
from dal_obscura.sources.contracts import TableFormat
from tests.support.arrow import id_region_batch, id_region_schema
from tests.support.reads import StaticAccessContext, make_read_service
from tests.support.use_cases import (
    FakeAuthorizer,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeTicketStore,
    StubTableFormat,
)

AUTHORIZATION_HEADER = AuthenticationRequest(headers={"authorization": "Bearer jwt-token"})


def _build_end_to_end_access_flow(table_format: TableFormat, decision: AccessDecision, **options):
    ticket_codec = HmacTicketCodecAdapter("secret")
    ticket_store = FakeTicketStore()

    authorizer = FakeAuthorizer(decision=decision, current_version=decision.policy_version)
    catalog_registry = FakeCatalogRegistry(table_format)
    principal = Principal(id="user1", groups=[], attributes={})
    plan_access = make_read_service(
        identity=FakeIdentity(principal=principal),
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=cast(Any, catalog_registry)
        ),
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
        **options,
    )
    return plan_access, plan_access


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
