import base64

import pyarrow as pa
import pytest

from dal_obscura.application.use_cases import FetchStreamUseCase, PlanAccessUseCase
from dal_obscura.domain.access_control import AccessDecision, MaskRule, Principal
from dal_obscura.domain.query_planning import Plan, PlanRequest, ReadPayload, ReadSpec
from dal_obscura.domain.ticket_delivery import TicketPayload


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

    def authorize(self, principal, table_identifier, requested_columns):
        self.last_requested_columns = list(requested_columns)
        if self._decision is None:
            raise PermissionError("Unauthorized")
        return self._decision

    def current_policy_version(self, table_identifier):
        return self._current_version


class FakeBackend:
    def __init__(self, schema: pa.Schema, plan: Plan, read_spec: ReadSpec) -> None:
        self._schema = schema
        self._plan = plan
        self._read_spec = read_spec

    def get_schema(self, table: str):
        return self._schema

    def plan(self, table: str, columns, max_tickets: int) -> Plan:
        return self._plan

    def read_spec(self, read_payload: bytes) -> ReadSpec:
        return self._read_spec

    def read_stream(self, read_payload: bytes):
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
    read_payload = b'{"table":"users","columns":["id","region"]}'
    plan = Plan(schema=schema, tasks=[ReadPayload(payload=read_payload)])
    read_spec = ReadSpec(table="users", columns=["id", "region"])
    decision = AccessDecision(
        allowed_columns=["id", "region"],
        masks={"region": MaskRule(type="redact", value="***")},
        row_filter="region = 'us'",
        policy_version=100,
    )
    return schema, plan, read_spec, decision


def test_plan_access_auth_failure():
    schema, plan, read_spec, decision = _build_use_case_dependencies()
    backend = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            table="users",
            columns=["id", "region"],
            scan={},
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=None),
        authorizer=authorizer,
        planning_backend=backend,
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(PlanRequest(table="users", columns=["id"]), {})


def test_plan_access_authz_failure():
    schema, plan, read_spec, _ = _build_use_case_dependencies()
    backend = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            table="users",
            columns=["id", "region"],
            scan={},
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=None),
        planning_backend=backend,
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(
            PlanRequest(table="users", columns=["id"]), {"authorization": "ApiKey key"}
        )


def test_plan_access_expands_wildcard_columns():
    schema, plan, read_spec, decision = _build_use_case_dependencies()
    backend = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            table="users",
            columns=["id", "region"],
            scan={},
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        planning_backend=backend,
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )
    use_case.execute(
        PlanRequest(table="users", columns=["*"]),
        {"authorization": "ApiKey key"},
    )

    assert authorizer.last_requested_columns == ["id", "region"]


def test_fetch_stream_policy_version_mismatch():
    schema, plan, read_spec, decision = _build_use_case_dependencies()
    backend = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
    payload = TicketPayload(
        table="users",
        columns=["id", "region"],
        scan={
            "read_payload": base64.b64encode(b'{"table":"users","columns":["id","region"]}').decode(
                "utf-8"
            ),
            "row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=101),
        planning_backend=backend,
        read_backend=backend,
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", {"authorization": "ApiKey key"})


def test_fetch_stream_principal_mismatch():
    schema, plan, read_spec, decision = _build_use_case_dependencies()
    backend = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
    payload = TicketPayload(
        table="users",
        columns=["id", "region"],
        scan={
            "read_payload": base64.b64encode(b'{"table":"users","columns":["id","region"]}').decode(
                "utf-8"
            ),
            "row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user2", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        planning_backend=backend,
        read_backend=backend,
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", {"authorization": "ApiKey key"})


def test_fetch_stream_read_spec_mismatch():
    schema, plan, _, decision = _build_use_case_dependencies()
    mismatched_backend = FakeBackend(
        schema=schema,
        plan=plan,
        read_spec=ReadSpec(table="different_table", columns=["id", "region"]),
    )
    payload = TicketPayload(
        table="users",
        columns=["id", "region"],
        scan={
            "read_payload": base64.b64encode(
                b'{"table":"different_table","columns":["id","region"]}'
            ).decode("utf-8"),
            "row_filter": None,
            "masks": {},
        },
        policy_version=100,
        principal_id="user1",
        expires_at=9999999999,
        nonce="abc",
    )
    use_case = FetchStreamUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, current_version=100),
        planning_backend=mismatched_backend,
        read_backend=mismatched_backend,
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(ValueError):
        use_case.execute("token", {"authorization": "ApiKey key"})
