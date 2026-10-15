from typing import Any, cast

import pyarrow as pa
import pytest

from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.request import PlanRequest
from dal_obscura.kernel.policy.models import AccessDecision, MaskRule
from tests.support.read.read_scenarios import (
    AUTHORIZATION_HEADER,
    _build_use_case_dependencies,
    governed_read_service,
)
from tests.support.read.reads import StaticAccessContext, make_read_service
from tests.support.read.tickets import ticket_payload
from tests.support.read.use_cases import (
    FakeAuthorizer,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeTicketCodec,
    FakeTicketStore,
    StubTableFormat,
    TrackingTableFormat,
    scan_payload,
)


def test_schema_plan_and_fetch_use_the_injected_compiled_projection():
    from dal_obscura.kernel.plans.projection import CompiledProjection

    output_schema = pa.schema([pa.field("id", pa.int32())])

    class LiteralCompiler:
        def compile(self, schema, columns, masks):
            return CompiledProjection(('CAST(42 AS INTEGER) AS "id"',), output_schema)

    _, decision, table = _build_use_case_dependencies()
    service = governed_read_service(table, decision, projection_compiler=LiteralCompiler())
    request = PlanRequest(catalog="catalog1", target="users", columns=["id"])

    declared = service.schema(request, AUTHORIZATION_HEADER)
    planned = service.plan(request, AUTHORIZATION_HEADER)
    fetched = service.fetch(planned.ticket_tokens[0], AUTHORIZATION_HEADER)
    result = pa.Table.from_batches(fetched.result_batches, schema=fetched.output_schema)

    assert declared.output_schema.equals(output_schema, check_metadata=True)
    assert planned.output_schema.equals(output_schema, check_metadata=True)
    assert result.schema.equals(output_schema, check_metadata=True)
    assert result.to_pylist() == [{"id": 42}]


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
    planner = governed_read_service(table, decision)
    with pytest.raises(ValueError, match="schema changed"):
        planner.plan(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
        )


@pytest.mark.parametrize(
    "name",
    ["x" * 257, "界" * 1024, ' long."field ' + "x" * 16384],
    ids=["ascii-257", "unicode-1024", "ascii-16384"],
)
def test_long_field_identity_survives_signed_planning_filtering_and_masking(name):
    from dal_obscura_plugin_api.paths import FieldPath, FieldSegment

    path = FieldPath((FieldSegment(name),)).to_human()
    schema = pa.schema([pa.field(name, pa.string()), pa.field("region", pa.string())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array(["private-us", "private-eu", None]), pa.array(["us", "eu", "us"])],
        schema=schema,
    )
    source = StubTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        batches=(batch,),
    )
    reader = governed_read_service(
        source,
        AccessDecision(
            allowed_columns=[path, "region"],
            masks={path: MaskRule(type="redact", value="[hidden]")},
            row_filter="region = 'us'",
            policy_version=100,
        ),
    )
    request = PlanRequest(catalog="catalog1", target="users", columns=[path])
    advertised = reader.schema(request, AUTHORIZATION_HEADER)
    planned = reader.plan(request, AUTHORIZATION_HEADER)
    fetched = reader.fetch(planned.ticket_tokens[0], AUTHORIZATION_HEADER)
    table = pa.Table.from_batches(list(fetched.result_batches), schema=fetched.output_schema)
    expected_schema = pa.schema([pa.field(name, pa.string())])
    assert advertised.output_schema.equals(expected_schema, check_metadata=True)
    assert table.schema.equals(expected_schema, check_metadata=True)
    assert table.to_pylist() == [{name: "[hidden]"}, {name: None}]


@pytest.mark.parametrize("operation", ["schema", "plan"])
def test_read_rejects_native_incompatible_output_before_planning_or_tickets(operation, monkeypatch):
    from dal_obscura_plugin_conformance.schemas import nested_struct_schema

    schema = nested_struct_schema(63)
    source = StubTableFormat(
        catalog_name="catalog1", table_name="users", format="fake_format", schema=schema, batches=()
    )
    monkeypatch.setattr(
        type(source), "plan", lambda *args: pytest.fail("Unsupported output reached scan planning")
    )
    tickets = FakeTicketCodec()
    service = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(
                decision=AccessDecision(
                    allowed_columns=[
                        "root." + ".".join(f"level_{index}" for index in reversed(range(63)))
                    ],
                    masks={},
                    row_filter=None,
                    policy_version=1,
                )
            ),
            catalog_registry=cast(Any, FakeCatalogRegistry(source)),
        ),
        ticket_codec=tickets,
        ticket_store=FakeTicketStore(),
    )
    request = PlanRequest(catalog="catalog1", target="users", columns=["root"])
    with pytest.raises(ValueError, match="native interchange"):
        getattr(service, operation)(request, AUTHORIZATION_HEADER)
    assert tickets.signed_payloads == []


def test_read_schema_allows_native_projection_of_deep_unselected_source():
    from dal_obscura_plugin_conformance.schemas import nested_struct_schema

    schema = nested_struct_schema(63).append(pa.field("id", pa.int64()))
    source = StubTableFormat(
        catalog_name="catalog1", table_name="users", format="fake_format", schema=schema, batches=()
    )
    service = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(
                decision=AccessDecision(
                    allowed_columns=["id"], masks={}, row_filter=None, policy_version=1
                )
            ),
            catalog_registry=cast(Any, FakeCatalogRegistry(source)),
        ),
    )
    result = service.schema(
        PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
    )
    assert result.output_schema.equals(pa.schema([pa.field("id", pa.int64())]), check_metadata=True)
