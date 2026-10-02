from typing import Any, cast

import pyarrow as pa
import pytest

from dal_obscura.policy.models import AccessDecision, MaskRule, Principal
from dal_obscura.read.request import PlanRequest
from tests.application.access_flow.helpers import (
    AUTHORIZATION_HEADER,
    _build_end_to_end_access_flow,
    _build_use_case_dependencies,
)
from tests.support.reads import StaticAccessContext, make_read_service
from tests.support.tickets import ticket_payload
from tests.support.use_cases import (
    FakeAuthorizer,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeTicketCodec,
    FakeTicketStore,
    StubTableFormat,
    TrackingTableFormat,
    scan_payload,
)


def test_plan_access_expands_wildcard_columns():
    _schema, decision, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(ticket_payload(columns=["id", "region"], scan=scan_payload()))
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=cast(Any, catalog_registry)
        ),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )
    use_case.plan(
        PlanRequest(catalog="catalog1", target="users", columns=["*"]), AUTHORIZATION_HEADER
    )

    assert authorizer.last_requested_columns == ["id", "region"]
    assert ticket_codec.signed_payloads[0].scan["full_row_filter"] == "region = 'us'"
    assert ticket_codec.signed_payloads[0].scan["full_row_filter"] is not None


def test_wildcard_preserves_literal_field_names_during_authorization() -> None:
    schema = pa.schema(
        [pa.field("a.b", pa.int64()), pa.field("*", pa.string()), pa.field("$value", pa.bool_())]
    )
    paths = ['["a.b"]', '["*"]', '["$value"]']
    source = StubTableFormat(
        catalog_name="catalog1", table_name="users", format="fake_format", schema=schema, batches=()
    )
    authorizer = FakeAuthorizer(
        decision=AccessDecision(allowed_columns=paths, masks={}, row_filter=None, policy_version=1)
    )
    service = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=cast(Any, FakeCatalogRegistry(source))
        ),
    )

    result = service.schema(
        PlanRequest(catalog="catalog1", target="users", columns=["*"]), AUTHORIZATION_HEADER
    )

    assert authorizer.last_requested_columns == paths
    assert result.output_schema.names == schema.names


@pytest.mark.parametrize("columns", [[], ["*", "id"], ["id", "id"]])
def test_plan_access_rejects_ambiguous_or_empty_column_requests(columns):
    _schema, decision, table_format = _build_use_case_dependencies()
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(decision=decision),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(ValueError, match="columns"):
        use_case.plan(
            PlanRequest(catalog="catalog1", target="users", columns=columns), AUTHORIZATION_HEADER
        )


def test_plan_access_accepts_nested_requested_columns():
    schema = pa.schema(
        [
            pa.field(
                "user", pa.struct([pa.field("address", pa.struct([pa.field("zip", pa.int64())]))])
            )
        ]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1", table_name="users", format="fake_format", schema=schema, batches=()
    )
    decision = AccessDecision(
        allowed_columns=["user.address.zip"],
        masks={"user.address.zip": MaskRule(type="hash")},
        row_filter=None,
        policy_version=100,
    )
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        ticket_payload(columns=["user.address.zip"], scan=scan_payload())
    )
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=cast(Any, FakeCatalogRegistry(table_format))
        ),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    use_case.plan(
        PlanRequest(catalog="catalog1", target="users", columns=["user.address.zip"]),
        AUTHORIZATION_HEADER,
    )

    assert authorizer.last_requested_columns == ["user.address.zip"]


def test_plan_access_expands_parent_request_with_null_masked_siblings():
    schema = pa.schema(
        [
            pa.field(
                "profile", pa.struct([pa.field("name", pa.string()), pa.field("ssn", pa.string())])
            )
        ]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1", table_name="users", format="fake_format", schema=schema, batches=()
    )
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(
                decision=AccessDecision(
                    allowed_columns=["profile.name", "profile.ssn"],
                    masks={"profile.ssn": MaskRule(type="null")},
                    row_filter=None,
                    policy_version=100,
                )
            ),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    result = use_case.plan(
        PlanRequest(catalog="catalog1", target="users", columns=["profile"]), AUTHORIZATION_HEADER
    )

    assert result.columns == ["profile.name", "profile.ssn"]


def test_plan_access_requires_map_key_permission_for_map_value_projection():
    schema = pa.schema(
        [pa.field("contacts", pa.map_(pa.string(), pa.struct([pa.field("name", pa.string())])))]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1", table_name="users", format="fake_format", schema=schema, batches=()
    )
    authorizer = FakeAuthorizer(
        decision=AccessDecision(
            allowed_columns=["contacts.$value.name"], masks={}, row_filter=None, policy_version=100
        )
    )
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=cast(Any, FakeCatalogRegistry(table_format))
        ),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError, match="Requested columns are not authorized"):
        use_case.plan(
            PlanRequest(catalog="catalog1", target="users", columns=["contacts.$value.name"]),
            AUTHORIZATION_HEADER,
        )

    assert authorizer.last_requested_columns == ["contacts.$value.name", "contacts.$key"]


def test_plan_access_includes_authorized_map_keys_with_value_projection():
    schema = pa.schema(
        [pa.field("contacts", pa.map_(pa.string(), pa.struct([pa.field("name", pa.string())])))]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1", table_name="users", format="fake_format", schema=schema, batches=()
    )
    authorizer = FakeAuthorizer(
        decision=AccessDecision(
            allowed_columns=["contacts.$value.name", "contacts.$key"],
            masks={},
            row_filter=None,
            policy_version=100,
        )
    )
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=cast(Any, FakeCatalogRegistry(table_format))
        ),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    result = use_case.plan(
        PlanRequest(catalog="catalog1", target="users", columns=["contacts.$value.name"]),
        AUTHORIZATION_HEADER,
    )

    assert result.columns == ["contacts.$value.name", "contacts.$key"]


def test_plan_access_rejects_unknown_requested_columns():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "user", pa.struct([pa.field("address", pa.struct([pa.field("zip", pa.int64())]))])
            ),
        ]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1", table_name="users", format="fake_format", schema=schema, batches=()
    )
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(
                decision=AccessDecision(
                    allowed_columns=["id", "user.address.zip"],
                    masks={},
                    row_filter=None,
                    policy_version=100,
                )
            ),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
        ticket_codec=FakeTicketCodec(ticket_payload(columns=["id"], scan=scan_payload())),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    for requested_columns in (["missing"], ["id.value"], ["user.missing"]):
        with pytest.raises(ValueError, match="Unknown columns requested"):
            use_case.plan(
                PlanRequest(catalog="catalog1", target="users", columns=requested_columns),
                AUTHORIZATION_HEADER,
            )


def test_plan_access_rejects_explicit_denied_column_request():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("secret", pa.string()),
        ]
    )
    planned_columns: list[list[str]] = []
    table_format = TrackingTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        planned_columns=planned_columns,
    )
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(
                decision=AccessDecision(
                    allowed_columns=["id"], masks={}, row_filter=None, policy_version=100
                )
            ),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
        ticket_codec=FakeTicketCodec(ticket_payload(columns=["id"], scan=scan_payload())),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError, match="not authorized"):
        use_case.plan(
            PlanRequest(catalog="catalog1", target="users", columns=["id", "secret"]),
            AUTHORIZATION_HEADER,
        )

    assert planned_columns == []


def test_plan_access_prunes_wildcard_to_authorized_columns():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("secret", pa.string())])
    planned_columns: list[list[str]] = []
    table_format = TrackingTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        planned_columns=planned_columns,
    )
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(
                decision=AccessDecision(
                    allowed_columns=["id"], masks={}, row_filter=None, policy_version=100
                )
            ),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    result = use_case.plan(
        PlanRequest(catalog="catalog1", target="users", columns=["*"]), AUTHORIZATION_HEADER
    )

    assert result.columns == ["id"]
    assert planned_columns == [["id"]]


@pytest.mark.parametrize("drift", ["type", "metadata"])
def test_backend_schema_drift_is_rejected_before_ticket_creation(monkeypatch, drift):
    from dataclasses import replace

    _, decision, table = _build_use_case_dependencies()
    original = StubTableFormat.plan
    monkeypatch.setattr(
        StubTableFormat,
        "plan",
        lambda self, request, max_tickets: replace(
            original(self, request, max_tickets),
            schema=(
                pa.schema([pa.field("id", pa.string())])
                if drift == "type"
                else table.schema.with_metadata({b"revision": b"changed"})
            ),
        ),
    )
    planner, _ = _build_end_to_end_access_flow(table, decision)
    with pytest.raises(ValueError, match="schema changed"):
        planner.plan(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
        )
