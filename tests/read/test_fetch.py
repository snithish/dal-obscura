from typing import Any, cast

import pyarrow as pa
import pytest

from dal_obscura.adapters.duckdb.transform import (
    DuckDBRowTransformAdapter,
)
from dal_obscura.adapters.tickets.signing import HmacTicketCodecAdapter
from dal_obscura.application.reads.sources import ScanTask
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.request import PlanRequest
from dal_obscura.kernel.policy.filters import deserialize_row_filter
from dal_obscura.kernel.policy.models import AccessDecision
from tests.support.flight.flight import InMemoryPolicyAuthorizer
from tests.support.read.read_scenarios import (
    AUTHORIZATION_HEADER,
    _build_use_case_dependencies,
    _ticket_store_with,
    governed_read_service,
)
from tests.support.read.reads import StaticAccessContext, make_read_service
from tests.support.read.tickets import ticket_payload
from tests.support.read.use_cases import (
    FakeCatalogRegistry,
    FakeIdentity,
    FakeRowTransform,
    FakeTicketCodec,
    FakeTicketStore,
    PretendPushdownTableFormat,
    StubTableFormat,
    encode_scan_task,
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


def test_fetch_stream_principal_mismatch():
    schema, _decision, table_format = _build_use_case_dependencies()
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        columns=["id", "region"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
    )
    ticket_store = _ticket_store_with(payload)
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user2", groups=[], attributes={})),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    with pytest.raises(PermissionError):
        use_case.fetch("token", AUTHORIZATION_HEADER)


@pytest.mark.parametrize(
    ("before", "after"),
    [
        (
            Principal(id="user1", groups=["analyst"], attributes={}),
            Principal(id="user1", groups=[], attributes={}),
        ),
        (
            Principal(id="user1", groups=["analyst"], attributes={"department": "research"}),
            Principal(id="user1", groups=["analyst"], attributes={"department": "sales"}),
        ),
    ],
)
def test_fetch_stream_rejects_ticket_when_identity_context_changes(before, after):
    schema = pa.schema([pa.field("id", pa.int64())])
    table_format = PretendPushdownTableFormat(
        catalog_name="analytics",
        table_name="users",
        format="test",
        schema=schema,
        batches=(pa.record_batch([pa.array([1], type=pa.int64())], schema=schema),),
    )
    identity = FakeIdentity(principal=before)
    authorizer = InMemoryPolicyAuthorizer(
        catalog="analytics",
        target="users",
        rules=[{"principals": ["group:analyst"], "columns": ["id"]}],
    )

    ticket_codec = HmacTicketCodecAdapter("secret")
    ticket_store = FakeTicketStore()
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
        PlanRequest(catalog="analytics", target="users", columns=["id"]), AUTHORIZATION_HEADER
    )
    identity._principal = after

    with pytest.raises(PermissionError):
        fetch_stream.fetch(planned.ticket_tokens[0], AUTHORIZATION_HEADER)


def test_fetch_stream_rejects_matching_subject_from_another_issuer():
    schema, _decision, table_format = _build_use_case_dependencies()
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        columns=["id", "region"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        issuer="https://issuer-a.example",
    )
    ticket_store = _ticket_store_with(payload)
    use_case = make_read_service(
        identity=FakeIdentity(
            principal=Principal(
                id="user1", groups=[], attributes={}, issuer="https://issuer-b.example"
            )
        ),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    with pytest.raises(PermissionError, match="Unauthorized"):
        use_case.fetch("token", AUTHORIZATION_HEADER)

    assert ticket_store.reserve_calls == []


def test_fetch_stream_stops_before_emitting_batches_after_identity_expiry():
    schema = pa.schema([pa.field("id", pa.int64())])
    table_format = PretendPushdownTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="test",
        schema=schema,
        batches=(
            pa.record_batch([pa.array([1])], schema=schema),
            pa.record_batch([pa.array([2])], schema=schema),
        ),
    )
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000001",
        columns=["id"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        expires_at=1001,
        nonce="nonce",
    )
    clock = [999]
    use_case = make_read_service(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={}, expires_at=1000)
        ),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=_ticket_store_with(payload),
        now=lambda: clock[0],
    )

    result = use_case.fetch("ticket", AUTHORIZATION_HEADER)
    batches = iter(result.result_batches)
    assert next(batches).column("id").to_pylist() == [1]
    clock[0] = 1000
    with pytest.raises(PermissionError, match="Identity expired"):
        next(batches)


def test_fetch_stream_stops_after_ticket_is_revoked_between_batches():
    schema = pa.schema([pa.field("id", pa.int64())])
    table_format = PretendPushdownTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="test",
        schema=schema,
        batches=(
            pa.record_batch([pa.array([1])], schema=schema),
            pa.record_batch([pa.array([2])], schema=schema),
        ),
    )
    payload = ticket_payload(
        ticket_id="00000000-0000-0000-0000-000000000002",
        columns=["id"],
        scan={
            "authorization_columns": ["id", "region"],
            "read_payload": encode_scan_task(table_format, schema),
            "full_row_filter": None,
            "masks": {},
        },
        nonce="nonce",
    )
    ticket_store = _ticket_store_with(payload)
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
        ticket_store=ticket_store,
    )

    batches = iter(use_case.fetch("ticket", AUTHORIZATION_HEADER).result_batches)
    assert next(batches).column("id").to_pylist() == [1]
    assert payload.ticket_id is not None
    ticket_store.revoked.add(payload.ticket_id)
    with pytest.raises(PermissionError, match="revoked"):
        next(batches)


def test_stream_expiry_guard_rejects_ticket_expiry_before_identity_expiry():
    from tests.support.read.reads import expiry_stream

    batch = pa.record_batch([pa.array([1])], names=["id"])
    guarded = expiry_stream(
        [batch],
        ticket_expires_at=1000,
        identity_expires_at=2000,
        stream_deadline_at=2000,
        now=lambda: 1000,
    )

    with pytest.raises(PermissionError, match="Ticket expired"):
        next(guarded)


def test_stream_deadline_guard_stops_before_emitting_a_late_batch():
    from tests.support.read.reads import expiry_stream

    batch = pa.record_batch([pa.array([1])], names=["id"])
    guarded = expiry_stream(
        [batch],
        ticket_expires_at=1000,
        identity_expires_at=None,
        stream_deadline_at=100,
        now=lambda: 100,
    )

    with pytest.raises(TimeoutError, match="Stream deadline exceeded"):
        next(guarded)


def test_stream_guard_closes_upstream_when_consumer_stops_early() -> None:
    from tests.support.read.reads import expiry_stream

    class ClosableBatches:
        def __init__(self) -> None:
            self.closed = False

        def __iter__(self):
            yield pa.record_batch([pa.array([1])], names=["id"])
            yield pa.record_batch([pa.array([2])], names=["id"])

        def close(self) -> None:
            self.closed = True

    upstream = ClosableBatches()
    guarded = expiry_stream(
        upstream,
        ticket_expires_at=9999,
        identity_expires_at=None,
        stream_deadline_at=9999,
        now=lambda: 1,
    )
    next(guarded)
    cast(Any, guarded).close()

    assert upstream.closed is True


def test_zero_output_scan_stops_at_input_deadline_before_another_pull(monkeypatch):
    from types import SimpleNamespace

    import dal_obscura.adapters.tickets.signing as signer
    from tests.support.read.use_cases import FakeAuthorizer

    clock = [0]
    monkeypatch.setattr(signer, "time", SimpleNamespace(time=lambda: clock[0]))
    counts = {"pulls": 0, "closed": 0}
    schema = pa.schema([("id", pa.int64())])

    class AdvancingSource(StubTableFormat):
        def execute(self, partition, *, deadline):
            def batches():
                try:
                    for value in range(4):
                        counts["pulls"] += 1
                        clock[0] += 1
                        yield pa.record_batch([[value]], schema=schema)
                finally:
                    counts["closed"] += 1

            return schema, batches()

    source = AdvancingSource(
        catalog_name="catalog1", table_name="users", format="fixture", schema=schema, batches=()
    )
    task = ScanTask(source, schema, None)

    class CountingTickets(FakeTicketStore):
        def __init__(self):
            super().__init__()
            self.disclosures = 0

        def ensure_active(self, ticket_id):
            self.disclosures += 1
            return super().ensure_active(ticket_id)

    store = CountingTickets()
    decision = AccessDecision(
        allowed_columns=["id"], masks={}, row_filter="id < 0", policy_version=1
    )
    reader = make_read_service(
        identity=FakeIdentity(Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(decision, 1), catalog_registry=FakeCatalogRegistry(source)
        ),
        ticket_codec=HmacTicketCodecAdapter("secret"),
        ticket_store=store,
        task_codec=_ControlledTaskCodec(task),
        now=lambda: clock[0],
        max_stream_seconds=2,
    )
    plan = reader.plan(
        PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
    )
    result = reader.fetch(plan.ticket_tokens[0], AUTHORIZATION_HEADER)
    store.disclosures = 0
    with pytest.raises(TimeoutError, match="deadline"):
        list(result.result_batches)
    assert counts == {"pulls": 2, "closed": 1}
    assert store.disclosures == 0


class _ControlledTaskCodec:
    def __init__(self, task: ScanTask) -> None:
        self._task = task

    def encode(self, value: ScanTask) -> str:
        return "controlled-source"

    def decode(self, payload: str) -> ScanTask:
        assert payload == "controlled-source"
        return self._task


def test_fetch_validates_projected_execution_schema_instead_of_full_base_schema():
    base = pa.schema(
        [
            pa.field("id", pa.int64(), metadata={b"field_id": b"1"}),
            pa.field("private", pa.string()),
        ],
        metadata={b"source": b"captured"},
    )
    projected = pa.schema([base.field("id")], metadata=base.metadata)

    class ProjectedSource(StubTableFormat):
        def execute(self, partition, *, deadline):
            return projected, [pa.record_batch([[7]], schema=projected)]

    source = ProjectedSource(
        catalog_name="catalog1", table_name="users", format="fixture", schema=base, batches=()
    )
    reader = governed_read_service(
        source,
        AccessDecision(allowed_columns=["id"], masks={}, row_filter=None, policy_version=1),
        task_codec=_ControlledTaskCodec(ScanTask(source, base, None)),
    )
    planned = reader.plan(
        PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
    )
    result = reader.fetch(planned.ticket_tokens[0], AUTHORIZATION_HEADER)
    table = pa.Table.from_batches(list(result.result_batches), schema=result.output_schema)
    assert table.schema.equals(pa.schema([("id", pa.int64())]), check_metadata=True)
    assert table.to_pylist() == [{"id": 7}]


def test_fetch_is_lazy_and_checks_injected_cancellation_after_source_pull():
    from threading import Event

    from dal_obscura.application.reads.execution import StreamCancelledError

    schema = pa.schema([("id", pa.int64())])
    cancelled = Event()
    counts = {"opened": 0, "pulls": 0, "closed": 0}

    class CancellingSource(StubTableFormat):
        def execute(self, partition, *, deadline):
            counts["opened"] += 1

            def batches():
                try:
                    for value in range(4):
                        counts["pulls"] += 1
                        cancelled.set()
                        yield pa.record_batch([[value]], schema=schema)
                finally:
                    counts["closed"] += 1

            return schema, batches()

    source = CancellingSource(
        catalog_name="catalog1", table_name="users", format="fixture", schema=schema, batches=()
    )
    reader = governed_read_service(
        source,
        AccessDecision(allowed_columns=["id"], masks={}, row_filter="id < 0", policy_version=1),
        task_codec=_ControlledTaskCodec(ScanTask(source, schema, None)),
    )
    planned = reader.plan(
        PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
    )
    result = reader.fetch(
        planned.ticket_tokens[0], AUTHORIZATION_HEADER, cancel_check=cancelled.is_set
    )
    assert counts == {"opened": 0, "pulls": 0, "closed": 0}
    with pytest.raises(StreamCancelledError, match="cancelled"):
        list(result.result_batches)
    assert counts == {"opened": 1, "pulls": 1, "closed": 1}


@pytest.mark.parametrize("denied", [False, True])
def test_lazy_provider_initialization_preserves_error_kind_and_releases_slot(denied):
    from dal_obscura.application.reads.execution import (
        ExecutionSupervisor,
        SourceInitializationPermissionError,
        SourceInitializationValueError,
    )

    schema = pa.schema([("id", pa.int64())])

    class FailedSource(StubTableFormat):
        def execute(self, partition, *, deadline):
            if denied:
                raise PermissionError("provider denied initialization")
            raise ValueError("provider rejected initialization")

    source = FailedSource(
        catalog_name="catalog1", table_name="users", format="fixture", schema=schema, batches=()
    )
    supervisor = ExecutionSupervisor(max_active_streams=1)
    reader = governed_read_service(
        source,
        AccessDecision(allowed_columns=["id"], masks={}, row_filter=None, policy_version=1),
        task_codec=_ControlledTaskCodec(ScanTask(source, schema, None)),
        execution_supervisor=supervisor,
    )
    planned = reader.plan(
        PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
    )
    result = reader.fetch(planned.ticket_tokens[0], AUTHORIZATION_HEADER)
    with pytest.raises(
        SourceInitializationPermissionError if denied else SourceInitializationValueError
    ):
        list(result.result_batches)
    replacement = supervisor.open(
        lambda: (schema, [pa.record_batch([[7]], schema=schema)]), lambda batches: batches, None
    )
    assert [batch.to_pylist() for batch in replacement] == [[{"id": 7}]]
