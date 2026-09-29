from typing import Any, cast

import pyarrow as pa
import pytest

from dal_obscura.common.access_control.filters import deserialize_row_filter, row_filter_to_sql
from dal_obscura.common.access_control.models import AccessDecision, MaskRule, Principal
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.ticket_delivery.models import TicketPayload
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest
from dal_obscura.data_plane.application.use_cases.plan_access import PlanAccessUseCase
from tests.application.access_flow.helpers import (
    AUTHORIZATION_HEADER,
    _build_end_to_end_access_flow,
    _build_use_case_dependencies,
)
from tests.support.use_cases import (
    FakeAuthorizer,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeMasking,
    FakeTicketCodec,
    FakeTicketStore,
    PretendPushdownTableFormat,
    StubTableFormat,
    TrackingTableFormat,
    scan_payload,
)


def test_plan_access_resolves_catalog_with_tenant_from_principal_attributes():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    table_format = TrackingTableFormat(
        catalog_name="analytics",
        table_name="default.users",
        format="fake_format",
        schema=schema,
        planned_columns=[],
    )
    catalog_registry = FakeCatalogRegistry(table_format)
    authorizer = FakeAuthorizer(
        decision=AccessDecision(
            allowed_columns=["id", "region"],
            masks={},
            row_filter=None,
            policy_version=100,
        ),
        current_version=100,
    )
    ticket_codec = FakeTicketCodec()
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": "tenant-a"})
        ),
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
        now=lambda: 1000,
        nonce_factory=lambda: "nonce",
    )

    use_case.execute(
        PlanRequest(catalog="analytics", target="default.users", columns=["id"]),
        AUTHORIZATION_HEADER,
    )

    assert catalog_registry.last_tenant_id == "tenant-a"
    assert ticket_codec.signed_payloads[0].tenant_id == "tenant-a"


def test_plan_access_rejects_authorization_without_asset_identity():
    _, decision, table_format = _build_use_case_dependencies()
    ticket_store = FakeTicketStore()
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision, asset_id=None),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(),
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError, match="governed asset identity"):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]),
            AUTHORIZATION_HEADER,
        )

    assert ticket_store.stored == []


def test_plan_access_auth_failure():
    _schema, decision, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            asset_id="00000000-0000-4000-8000-000000000001",
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan=scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=None),
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]),
            AuthenticationRequest(),
        )


def test_plan_access_authz_failure():
    _schema, _, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            asset_id="00000000-0000-4000-8000-000000000001",
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan=scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=None),
        catalog_registry=cast(Any, catalog_registry),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_expands_wildcard_columns():
    _schema, decision, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            asset_id="00000000-0000-4000-8000-000000000001",
            catalog="catalog1",
            target="users",
            columns=["id", "region"],
            scan=scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )
    use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["*"]),
        AUTHORIZATION_HEADER,
    )

    assert authorizer.last_requested_columns == ["id", "region"]
    assert ticket_codec.signed_payloads[0].scan["full_row_filter"] == "region = 'us'"
    assert ticket_codec.signed_payloads[0].scan["full_row_filter"] is not None


@pytest.mark.parametrize("columns", [[], ["*", "id"], ["id", "id"]])
def test_plan_access_rejects_ambiguous_or_empty_column_requests(columns):
    _schema, decision, table_format = _build_use_case_dependencies()
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(ValueError, match="columns"):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=columns),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_rejects_scan_payloads_above_configured_ticket_limit():
    _schema, decision, table_format = _build_use_case_dependencies()
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(decision=decision),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
        max_ticket_payload_bytes=1,
    )

    with pytest.raises(ValueError, match="ticket byte limit"):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_accepts_nested_requested_columns():
    schema = pa.schema(
        [
            pa.field(
                "user",
                pa.struct(
                    [
                        pa.field(
                            "address",
                            pa.struct([pa.field("zip", pa.int64())]),
                        )
                    ]
                ),
            )
        ]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        batches=(),
    )
    decision = AccessDecision(
        allowed_columns=["user.address.zip"],
        masks={"user.address.zip": MaskRule(type="hash")},
        row_filter=None,
        policy_version=100,
    )
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            asset_id="00000000-0000-4000-8000-000000000001",
            catalog="catalog1",
            target="users",
            columns=["user.address.zip"],
            scan=scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["user.address.zip"]),
        AUTHORIZATION_HEADER,
    )

    assert authorizer.last_requested_columns == ["user.address.zip"]


def test_plan_access_expands_parent_request_with_null_masked_siblings():
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct([pa.field("name", pa.string()), pa.field("ssn", pa.string())]),
            )
        ]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        batches=(),
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["profile.name", "profile.ssn"],
                masks={"profile.ssn": MaskRule(type="null")},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    result = use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["profile"]),
        AUTHORIZATION_HEADER,
    )

    assert result.columns == ["profile.name", "profile.ssn"]


def test_plan_access_requires_map_key_permission_for_map_value_projection():
    schema = pa.schema(
        [
            pa.field(
                "contacts",
                pa.map_(pa.string(), pa.struct([pa.field("name", pa.string())])),
            )
        ]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        batches=(),
    )
    authorizer = FakeAuthorizer(
        decision=AccessDecision(
            allowed_columns=["contacts.$value.name"],
            masks={},
            row_filter=None,
            policy_version=100,
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError, match="Requested columns are not authorized"):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["contacts.$value.name"]),
            AUTHORIZATION_HEADER,
        )

    assert authorizer.last_requested_columns == ["contacts.$value.name", "contacts.$key"]


def test_plan_access_includes_authorized_map_keys_with_value_projection():
    schema = pa.schema(
        [
            pa.field(
                "contacts",
                pa.map_(pa.string(), pa.struct([pa.field("name", pa.string())])),
            )
        ]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        batches=(),
    )
    authorizer = FakeAuthorizer(
        decision=AccessDecision(
            allowed_columns=["contacts.$value.name", "contacts.$key"],
            masks={},
            row_filter=None,
            policy_version=100,
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    result = use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["contacts.$value.name"]),
        AUTHORIZATION_HEADER,
    )

    assert result.columns == ["contacts.$value.name", "contacts.$key"]


def test_plan_access_rejects_unknown_requested_columns():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "user",
                pa.struct([pa.field("address", pa.struct([pa.field("zip", pa.int64())]))]),
            ),
        ]
    )
    table_format = StubTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        batches=(),
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id", "user.address.zip"],
                masks={},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
            TicketPayload(
                asset_id="00000000-0000-4000-8000-000000000001",
                catalog="catalog1",
                target="users",
                columns=["id"],
                scan=scan_payload(),
                policy_version=100,
                principal_id="user1",
                expires_at=9999999999,
                nonce="abc",
            )
        ),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    for requested_columns in (["missing"], ["id.value"], ["user.missing"]):
        with pytest.raises(ValueError, match="Unknown columns requested"):
            use_case.execute(
                PlanRequest(catalog="catalog1", target="users", columns=requested_columns),
                AUTHORIZATION_HEADER,
            )


def test_plan_access_includes_hidden_row_filter_dependency_columns_in_execution_plan():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    planned_columns: list[list[str]] = []
    table_format = TrackingTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        planned_columns=planned_columns,
    )
    decision = AccessDecision(
        allowed_columns=["id"],
        masks={},
        row_filter="region = 'us'",
        policy_version=100,
    )
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            asset_id="00000000-0000-4000-8000-000000000001",
            catalog="catalog1",
            target="users",
            columns=["id"],
            scan=scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=authorizer,
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    result = use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["id"]),
        AUTHORIZATION_HEADER,
    )

    assert authorizer.last_requested_columns == ["id"]
    assert result.columns == ["id"]
    assert planned_columns == [["id", "region"]]


def test_plan_access_rejects_requested_row_filter_for_masked_column():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id", "region"],
                masks={"region": MaskRule(type="redact", value="***")},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(
            Any,
            FakeCatalogRegistry(
                TrackingTableFormat(
                    catalog_name="catalog1",
                    table_name="users",
                    format="fake_format",
                    schema=schema,
                    planned_columns=[],
                )
            ),
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
            TicketPayload(
                asset_id="00000000-0000-4000-8000-000000000001",
                catalog="catalog1",
                target="users",
                columns=["id"],
                scan=scan_payload(),
                policy_version=100,
                principal_id="user1",
                expires_at=9999999999,
                nonce="abc",
            )
        ),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(
        PermissionError,
        match="Requested row filter may not reference masked columns: region",
    ):
        use_case.execute(
            PlanRequest(
                catalog="catalog1",
                target="users",
                columns=["id"],
                row_filter=deserialize_row_filter("region = 'us'"),
            ),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_rejects_requested_row_filter_below_masked_parent():
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct([pa.field("region", pa.string()), pa.field("name", pa.string())]),
            )
        ]
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["profile.region"],
                masks={"profile": MaskRule(type="null")},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(
            Any,
            FakeCatalogRegistry(
                TrackingTableFormat(
                    catalog_name="catalog1",
                    table_name="users",
                    format="fake_format",
                    schema=schema,
                    planned_columns=[],
                )
            ),
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError, match=r"masked columns: profile\.region"):
        use_case.execute(
            PlanRequest(
                catalog="catalog1",
                target="users",
                columns=["profile.region"],
                row_filter=deserialize_row_filter("profile.region = 'us'"),
            ),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_rejects_requested_row_filter_for_non_visible_column():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id"],
                masks={},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(
            Any,
            FakeCatalogRegistry(
                TrackingTableFormat(
                    catalog_name="catalog1",
                    table_name="users",
                    format="fake_format",
                    schema=schema,
                    planned_columns=[],
                )
            ),
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
            TicketPayload(
                asset_id="00000000-0000-4000-8000-000000000001",
                catalog="catalog1",
                target="users",
                columns=["id"],
                scan=scan_payload(),
                policy_version=100,
                principal_id="user1",
                expires_at=9999999999,
                nonce="abc",
            )
        ),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(
        PermissionError,
        match="Requested row filter may only reference visible unmasked columns: region",
    ):
        use_case.execute(
            PlanRequest(
                catalog="catalog1",
                target="users",
                columns=["id"],
                row_filter=deserialize_row_filter("region = 'us'"),
            ),
            AUTHORIZATION_HEADER,
        )


def test_plan_access_allows_requested_row_filter_on_visible_unmasked_hidden_dependency():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    planned_columns: list[list[str]] = []
    table_format = TrackingTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="fake_format",
        schema=schema,
        planned_columns=planned_columns,
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id", "region"],
                masks={},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
            TicketPayload(
                asset_id="00000000-0000-4000-8000-000000000001",
                catalog="catalog1",
                target="users",
                columns=["id"],
                scan=scan_payload(),
                policy_version=100,
                principal_id="user1",
                expires_at=9999999999,
                nonce="abc",
            )
        ),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    result = use_case.execute(
        PlanRequest(
            catalog="catalog1",
            target="users",
            columns=["id"],
            row_filter=deserialize_row_filter("region = 'us'"),
        ),
        AUTHORIZATION_HEADER,
    )

    assert result.columns == ["id"]
    assert planned_columns == [["id", "region"]]


def test_plan_access_combines_policy_and_requested_row_filters_before_ticketing():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("active", pa.bool_()),
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
    ticket_codec = FakeTicketCodec(
        TicketPayload(
            asset_id="00000000-0000-4000-8000-000000000001",
            catalog="catalog1",
            target="users",
            columns=["id"],
            scan=scan_payload(),
            policy_version=100,
            principal_id="user1",
            expires_at=9999999999,
            nonce="abc",
        )
    )
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id", "region", "active"],
                masks={},
                row_filter="active = true",
                policy_version=100,
            )
        ),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    use_case.execute(
        PlanRequest(
            catalog="catalog1",
            target="users",
            columns=["id"],
            row_filter=deserialize_row_filter("region = 'us'"),
        ),
        AUTHORIZATION_HEADER,
    )

    payload_filter = ticket_codec.signed_payloads[0].scan["full_row_filter"]
    assert payload_filter is not None
    assert (
        row_filter_to_sql(deserialize_row_filter(payload_filter))
        == "active = TRUE AND region = 'us'"
    )
    assert planned_columns == [["id", "active", "region"]]


def test_plan_access_reports_non_sensitive_filter_and_projection_metrics():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("status", pa.string()),
        ]
    )
    table_format = PretendPushdownTableFormat(
        catalog_name="catalog1",
        table_name="users",
        format="pretend_pushdown",
        schema=schema,
        batches=(),
        backend_pushdown_sql="region = 'us'",
        residual_sql="LOWER(status) = 'active'",
    )
    decision = AccessDecision(
        allowed_columns=["id", "region", "status"],
        masks={},
        row_filter="LOWER(status) = 'active'",
        policy_version=100,
    )
    plan_access, _fetch_stream = _build_end_to_end_access_flow(table_format, decision)

    result = plan_access.execute(
        PlanRequest(
            catalog="catalog1",
            target="users",
            columns=["id"],
            row_filter=deserialize_row_filter("region = 'us'"),
        ),
        AUTHORIZATION_HEADER,
    )

    assert result.full_row_filter_present is True
    assert result.backend_pushdown_row_filter_present is True
    assert result.residual_row_filter_present is True
    assert result.visible_column_count == 1
    assert result.execution_column_count == 3


def test_plan_access_revalidates_requested_row_filter_against_base_schema():
    schema = pa.schema([pa.field("id", pa.int64())])
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id"],
                masks={},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(
            Any,
            FakeCatalogRegistry(
                TrackingTableFormat(
                    catalog_name="catalog1",
                    table_name="users",
                    format="fake_format",
                    schema=schema,
                    planned_columns=[],
                )
            ),
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
            TicketPayload(
                asset_id="00000000-0000-4000-8000-000000000001",
                catalog="catalog1",
                target="users",
                columns=["id"],
                scan=scan_payload(),
                policy_version=100,
                principal_id="user1",
                expires_at=9999999999,
                nonce="abc",
            )
        ),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(ValueError, match="Unknown column in row filter: missing"):
        use_case.execute(
            PlanRequest(
                catalog="catalog1",
                target="users",
                columns=["id"],
                row_filter=deserialize_row_filter("missing = 1"),
            ),
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
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id"],
                masks={},
                row_filter=None,
                policy_version=100,
            )
        ),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(
            TicketPayload(
                asset_id="00000000-0000-4000-8000-000000000001",
                catalog="catalog1",
                target="users",
                columns=["id"],
                scan=scan_payload(),
                policy_version=100,
                principal_id="user1",
                expires_at=9999999999,
                nonce="abc",
            )
        ),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError, match="not authorized"):
        use_case.execute(
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
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        authorizer=FakeAuthorizer(
            decision=AccessDecision(
                allowed_columns=["id"], masks={}, row_filter=None, policy_version=100
            )
        ),
        catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    result = use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["*"]), AUTHORIZATION_HEADER
    )

    assert result.columns == ["id"]
    assert planned_columns == [["id"]]


def test_backend_cannot_remove_core_row_filter(monkeypatch):
    from dataclasses import replace

    from tests.support.arrow import id_region_batch

    _, decision, table = _build_use_case_dependencies()
    table = replace(table, batches=(id_region_batch([1, 2], ["us", "eu"]),))
    original = StubTableFormat.plan
    monkeypatch.setattr(
        StubTableFormat,
        "plan",
        lambda self, request, max_tickets: replace(
            original(self, request, max_tickets), full_row_filter=None
        ),
    )
    planner, fetch = _build_end_to_end_access_flow(table, decision)
    plan = planner.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["id", "region"]),
        AUTHORIZATION_HEADER,
    )
    result = fetch.execute(plan.ticket_tokens[0], AUTHORIZATION_HEADER)
    assert pa.Table.from_batches(result.result_batches).column("id").to_pylist() == [1]


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
        planner.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
        )
