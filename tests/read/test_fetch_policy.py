import pyarrow as pa

from dal_obscura.adapters.duckdb.transform import (
    DuckDBRowTransformAdapter,
)
from dal_obscura.adapters.tickets.signing import HmacTicketCodecAdapter
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.request import PlanRequest
from dal_obscura.kernel.policy.filters import deserialize_row_filter
from dal_obscura.kernel.policy.models import AccessDecision
from tests.support.flight.flight import InMemoryPolicyAuthorizer
from tests.support.read.fakes import (
    AUTHORIZATION_HEADER,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeTicketStore,
    PretendPushdownTableFormat,
    StaticAccessContext,
    StubTableFormat,
    governed_read_service,
    make_read_service,
)


def test_exempt_reader_keeps_rule_row_filter_through_plan_and_fetch():
    schema = pa.schema([pa.field("email", pa.string()), pa.field("country", pa.string())])
    batches = (
        pa.record_batch(
            [
                pa.array(["alice@example.com", "bob@example.com"]),
                pa.array(["US", "CA"]),
            ],
            schema=schema,
        ),
    )
    table_format = StubTableFormat(
        catalog_name="catalog1", table_name="users", format="stub", schema=schema, batches=batches
    )
    principal = Principal(id="alice", groups=["analysts", "privacy-reviewers"], attributes={})
    authorizer = InMemoryPolicyAuthorizer(
        catalog="catalog1",
        target="users",
        rules=[
            {
                "principals": ["group:analysts"],
                "columns": ["email", "country"],
                "masks": {
                    "email": {"type": "hash", "exempt_principals": ["group:privacy-reviewers"]}
                },
                "row_filter": "country = 'US'",
            }
        ],
    )
    ticket_codec = HmacTicketCodecAdapter("secret")
    ticket_store = FakeTicketStore()

    identity = FakeIdentity(principal=principal)
    plan_access = make_read_service(
        identity=identity,
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=FakeCatalogRegistry(table_format)
        ),
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )
    fetch_stream = make_read_service(
        identity=identity,
        row_transform=DuckDBRowTransformAdapter(),
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
    )
    planned = plan_access.plan(
        PlanRequest(catalog="catalog1", target="users", columns=["email"]), AUTHORIZATION_HEADER
    )
    result = fetch_stream.fetch(planned.ticket_tokens[0], AUTHORIZATION_HEADER)
    output = pa.Table.from_batches(list(result.result_batches), schema=result.output_schema)
    assert output.schema.names == ["email"]
    assert output.column("email").to_pylist() == ["alice@example.com"]


def test_fetch_stream_reapplies_fully_pushed_row_filter_after_backend_execution():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batches = (
        pa.record_batch(
            [
                pa.array([1, 2], type=pa.int64()),
                pa.array(["us", "eu"], type=pa.string()),
            ],
            schema=schema,
        ),
    )
    table_format = PretendPushdownTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="pretend_pushdown",
        schema=schema,
        batches=batches,
        backend_pushdown_sql="region = 'us'",
        residual_sql=None,
    )
    decision = AccessDecision(
        allowed_columns=["id", "region"], masks={}, row_filter=None, policy_version=100
    )
    reader = governed_read_service(table_format, decision)

    plan_result = reader.plan(
        PlanRequest(
            catalog="catalog1",
            target="users",
            columns=["id"],
            row_filter=deserialize_row_filter("region = 'us'"),
        ),
        AUTHORIZATION_HEADER,
    )
    fetch_result = reader.fetch(plan_result.ticket_tokens[0], AUTHORIZATION_HEADER)
    table = pa.Table.from_batches(
        list(fetch_result.result_batches), schema=fetch_result.output_schema
    )

    assert table.schema.names == ["id"]
    assert table.column("id").to_pylist() == [1]


def test_fetch_stream_reapplies_full_policy_and_requested_filter_after_partial_pushdown():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("status", pa.string()),
        ]
    )
    batches = (
        pa.record_batch(
            [
                pa.array([1, 2, 3], type=pa.int64()),
                pa.array(["us", "eu", "us"], type=pa.string()),
                pa.array(["active", "active", "inactive"], type=pa.string()),
            ],
            schema=schema,
        ),
    )
    table_format = PretendPushdownTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="pretend_pushdown",
        schema=schema,
        batches=batches,
        backend_pushdown_sql="region = 'us'",
        residual_sql="LOWER(status) = 'active'",
    )
    decision = AccessDecision(
        allowed_columns=["id", "region", "status"],
        masks={},
        row_filter="LOWER(status) = 'active'",
        policy_version=100,
    )
    reader = governed_read_service(table_format, decision)

    plan_result = reader.plan(
        PlanRequest(
            catalog="catalog1",
            target="users",
            columns=["id"],
            row_filter=deserialize_row_filter("region = 'us'"),
        ),
        AUTHORIZATION_HEADER,
    )
    fetch_result = reader.fetch(plan_result.ticket_tokens[0], AUTHORIZATION_HEADER)
    table = pa.Table.from_batches(
        list(fetch_result.result_batches), schema=fetch_result.output_schema
    )

    assert table.schema.names == ["id"]
    assert table.column("id").to_pylist() == [1]
