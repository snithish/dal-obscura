import pyarrow as pa
import pytest

from dal_obscura.adapters.tickets.signing import HmacTicketCodecAdapter
from dal_obscura.application.reads.sources import ScanTask
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.request import PlanRequest
from dal_obscura.kernel.policy.models import AccessDecision
from tests.support.read.fakes import (
    AUTHORIZATION_HEADER,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeTicketStore,
    StaticAccessContext,
    StubTableFormat,
    governed_read_service,
    make_read_service,
)


def test_zero_output_scan_stops_at_input_deadline_before_another_pull(monkeypatch):
    from types import SimpleNamespace

    import dal_obscura.adapters.tickets.signing as signer
    from tests.support.read.fakes import FakeAuthorizer

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
