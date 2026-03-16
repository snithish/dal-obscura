import base64

import pyarrow as pa
import pytest

from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.domain.access_control.models import AccessDecision, MaskRule, Principal
from dal_obscura.domain.query_planning.models import (
    BackendReference,
    DatasetSelector,
    Plan,
    PlanRequest,
    ReadPayload,
    ReadSpec,
    ResolvedBackendTarget,
)
from dal_obscura.domain.ticket_delivery.models import TicketPayload

AUTHORIZATION_HEADER = {"authorization": "Bearer jwt-token"}


class FakeIdentity:
    def __init__(self, principal: Principal | None) -> None:
        self._principal = principal

    def authenticate(self, headers):
        if self._principal is None:
            raise PermissionError("Unauthorized")
        return self._principal


class FakeAuthorizer:
    def __init__(self, decision: AccessDecision | None, current_version: int | None = None) -> None:
        self._decision = decision
        self._current_version = current_version
        self.last_requested_columns: list[str] | None = None

    def authorize(self, principal, dataset, requested_columns):
        self.last_requested_columns = list(requested_columns)
        if self._decision is None:
            raise PermissionError("Unauthorized")
        return self._decision

    def current_policy_version(self, dataset):
        return self._current_version


class FakeBackend:
    def __init__(
        self,
        schema: pa.Schema,
        plan: Plan,
        read_spec: ReadSpec,
        target: ResolvedBackendTarget,
    ) -> None:
        self._schema = schema
        self._plan = plan
        self._read_spec = read_spec
        self._target = target

    def resolve(self, catalog: str | None, target: str) -> ResolvedBackendTarget:
        return self._target

    def get_schema(self, target: ResolvedBackendTarget):
        return self._schema

    def plan(self, target: ResolvedBackendTarget, columns, max_tickets: int) -> Plan:
        return self._plan

    def read_spec(self, backend: BackendReference, read_payload: bytes) -> ReadSpec:
        return self._read_spec

    def read_stream(self, backend: BackendReference, read_payload: bytes):
        return iter(
            [
                pa.record_batch(
                    [pa.array([1]), pa.array(["us"])],
                    schema=pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())]),
                )
            ]
        )


class FakeMasking:
    def apply(self, columns, masks):
        return None

    def masked_schema(self, base_schema, columns, masks):
        return base_schema


class FakeTicketCodec:
    def __init__(self, payload: TicketPayload) -> None:
        self._payload = payload
        self.signed_payloads: list[TicketPayload] = []

    def sign_payload(self, payload: TicketPayload) -> str:
        self.signed_payloads.append(payload)
        return "signed-token"

    def verify(self, token: str) -> TicketPayload:
        return self._payload


class FakeRowTransform:
    def apply_filters_and_masks_stream(self, batches, columns, row_filter, masks):
        return batches


def _build_use_case_dependencies():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    selector = DatasetSelector(target="users", catalog="catalog1")
    read_payload = b"payload"
    plan = Plan(schema=schema, tasks=[ReadPayload(payload=read_payload)])
    read_spec = ReadSpec(dataset=selector, columns=["id", "region"], schema=schema)
    decision = AccessDecision(
        allowed_columns=["id", "region"],
        masks={"region": MaskRule(type="redact", value="***")},
        row_filter="region = 'us'",
        policy_version=100,
    )
    target = ResolvedBackendTarget(
        dataset_identity=selector,
        backend=BackendReference(backend_id="duckdb_file", generation=1),
        handle={"format": "csv", "paths": ["users.csv"], "options": {}},
    )
    return schema, plan, read_spec, decision, target


def test_plan_access_auth_failure():
    schema, plan, read_spec, decision, target = _build_use_case_dependencies()
    backend = FakeBackend(schema=schema, plan=plan, read_spec=read_spec, target=target)
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan={},
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
            backend_id="duckdb_file",
            backend_generation=1,
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=None),
        authorizer=authorizer,
        backend=backend,
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(PlanRequest(catalog="catalog1", target="users", columns=["id"]), {})


def test_plan_access_authz_failure():
    schema, plan, read_spec, _, target = _build_use_case_dependencies()
    backend = FakeBackend(schema=schema, plan=plan, read_spec=read_spec, target=target)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan={},
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
            backend_id="duckdb_file",
            backend_generation=1,
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=None),
        backend=backend,
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_expands_wildcard_columns():
    schema, plan, read_spec, decision, target = _build_use_case_dependencies()
    backend = FakeBackend(schema=schema, plan=plan, read_spec=read_spec, target=target)
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan={},
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
            backend_id="duckdb_file",
            backend_generation=1,
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        backend=backend,
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )
    use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["*"]),
        AUTHORIZATION_HEADER,
    )

    assert authorizer.last_requested_columns == ["id", "region"]


def test_fetch_stream_policy_version_mismatch():
    schema, plan, read_spec, decision, target = _build_use_case_dependencies()
    backend = FakeBackend(schema=schema, plan=plan, read_spec=read_spec, target=target)
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan={
            "read_payload": base64.b64encode(b"payload").decode("utf-8"),
            "row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
        backend_id="duckdb_file",
        backend_generation=1,
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=101),
        backend=backend,
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_principal_mismatch():
    schema, plan, read_spec, decision, target = _build_use_case_dependencies()
    backend = FakeBackend(schema=schema, plan=plan, read_spec=read_spec, target=target)
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan={
            "read_payload": base64.b64encode(b"payload").decode("utf-8"),
            "row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
        backend_id="duckdb_file",
        backend_generation=1,
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user2", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        backend=backend,
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_read_spec_mismatch():
    schema, plan, _, decision, target = _build_use_case_dependencies()
    mismatched_backend = FakeBackend(
        schema=schema,
        plan=plan,
        read_spec=ReadSpec(
            dataset=DatasetSelector(target="different_table", catalog="catalog1"),
            columns=["id", "region"],
            schema=schema,
        ),
        target=target,
    )
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan={
            "read_payload": base64.b64encode(b"payload").decode("utf-8"),
            "row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
        backend_id="duckdb_file",
        backend_generation=1,
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        backend=mismatched_backend,
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(ValueError):
        use_case.execute("token", AUTHORIZATION_HEADER)
