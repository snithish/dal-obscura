import base64
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, cast

import pyarrow as pa
import pytest

from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.domain.access_control.models import AccessDecision, MaskRule, Principal
from dal_obscura.domain.query_planning.models import (
    BackendBinding,
    BackendDescriptor,
    BackendReference,
    BoundBackendTarget,
    DatasetSelector,
    Plan,
    PlanRequest,
    ReadPayload,
    ReadSpec,
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


class FakeCatalogRegistry:
    def __init__(self, descriptor: BackendDescriptor) -> None:
        self._descriptor = descriptor

    def describe(self, catalog: str | None, target: str) -> BackendDescriptor:
        return self._descriptor


@dataclass(frozen=True)
class FakeDescriptor:
    dataset_identity: DatasetSelector
    backend_id: str = field(init=False, default="duckdb_file")


@dataclass(frozen=True)
class FakeBinding:
    backend_id: str = field(init=False, default="duckdb_file")


class FakeBackend:
    def __init__(
        self,
        schema: pa.Schema,
        plan: Plan,
        read_spec: ReadSpec,
    ) -> None:
        self._schema = schema
        self._plan = plan
        self._read_spec = read_spec

    def bind_descriptor(self, descriptor: BackendDescriptor) -> BoundBackendTarget:
        return BoundBackendTarget(
            dataset_identity=descriptor.dataset_identity,
            backend=BackendReference(backend_id=descriptor.backend_id, generation=1),
            binding=FakeBinding(),
        )

    def schema_for(self, bound_target: BoundBackendTarget) -> Any:
        return self._schema

    def plan_for(
        self, bound_target: BoundBackendTarget, columns: Iterable[str], max_tickets: int
    ) -> Plan:
        return self._plan

    def read_spec_for(self, backend: BackendReference, read_payload: bytes) -> ReadSpec:
        return self._read_spec

    def read_stream_for(self, backend: BackendReference, read_payload: bytes) -> Iterable[Any]:
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
    descriptor = FakeDescriptor(dataset_identity=selector)
    return schema, plan, read_spec, decision, descriptor


def test_plan_access_auth_failure():
    schema, plan, read_spec, decision, descriptor = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(descriptor=descriptor)
    backend_registry = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
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
        catalog_registry=cast(Any, catalog_registry),
        backend_registry=cast(Any, backend_registry),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(PlanRequest(catalog="catalog1", target="users", columns=["id"]), {})


def test_plan_access_authz_failure():
    schema, plan, read_spec, _, descriptor = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(descriptor=descriptor)
    backend_registry = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
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
        catalog_registry=cast(Any, catalog_registry),
        backend_registry=cast(Any, backend_registry),
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
    schema, plan, read_spec, decision, descriptor = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(descriptor=descriptor)
    backend_registry = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
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
        catalog_registry=cast(Any, catalog_registry),
        backend_registry=cast(Any, backend_registry),
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
    schema, plan, read_spec, decision, _ = _build_use_case_dependencies()
    backend_registry = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
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
        backend_registry=cast(Any, backend_registry),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_principal_mismatch():
    schema, plan, read_spec, decision, _ = _build_use_case_dependencies()
    backend_registry = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
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
        backend_registry=cast(Any, backend_registry),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(PermissionError):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_read_spec_mismatch():
    schema, plan, _, decision, _ = _build_use_case_dependencies()
    mismatched_backend_registry = FakeBackend(
        schema=schema,
        plan=plan,
        read_spec=ReadSpec(
            dataset=DatasetSelector(target="different_table", catalog="catalog1"),
            columns=["id", "region"],
            schema=schema,
        ),
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
        backend_registry=cast(Any, mismatched_backend_registry),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(ValueError):
        use_case.execute("token", AUTHORIZATION_HEADER)


def test_fetch_stream_rejects_invalid_mask_payload():
    schema, plan, read_spec, decision, _ = _build_use_case_dependencies()
    backend_registry = FakeBackend(schema=schema, plan=plan, read_spec=read_spec)
    payload = TicketPayload(
        catalog="catalog1",
        target="users",
        columns=["id", "region"],
        scan={
            "read_payload": base64.b64encode(b"payload").decode("utf-8"),
            "row_filter": None,
            "masks": {"region": "not-an-object"},
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
        backend_registry=cast(Any, backend_registry),
        masking=FakeMasking(),
        row_transform=FakeRowTransform(),
        ticket_codec=FakeTicketCodec(payload),
    )

    with pytest.raises(ValueError, match="Invalid mask payload"):
        use_case.execute("token", AUTHORIZATION_HEADER)
