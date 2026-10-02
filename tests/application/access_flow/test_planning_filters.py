from typing import Any, cast

import pyarrow as pa
import pytest

from dal_obscura.common.access_control.filters import deserialize_row_filter, row_filter_to_sql
from dal_obscura.common.access_control.models import AccessDecision, MaskRule, Principal
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.data_plane.application.ports.access_context import StaticAccessContext
from dal_obscura.data_plane.application.use_cases.plan_access import PlanAccessUseCase
from tests.application.access_flow.helpers import (
    AUTHORIZATION_HEADER,
    _build_end_to_end_access_flow,
    _build_use_case_dependencies,
)
from tests.support.tickets import ticket_payload
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
        allowed_columns=["id"], masks={}, row_filter="region = 'us'", policy_version=100
    )
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(ticket_payload(columns=["id"], scan=scan_payload()))
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=cast(Any, FakeCatalogRegistry(table_format))
        ),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    result = use_case.execute(
        PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
    )

    assert authorizer.last_requested_columns == ["id"]
    assert result.columns == ["id"]
    assert planned_columns == [["id", "region"]]


def test_plan_access_rejects_requested_row_filter_for_masked_column():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
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
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(ticket_payload(columns=["id"], scan=scan_payload())),
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(
        PermissionError, match="Requested row filter may not reference masked columns: region"
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
        access_context=StaticAccessContext(
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
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(
                decision=AccessDecision(
                    allowed_columns=["id"], masks={}, row_filter=None, policy_version=100
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
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(ticket_payload(columns=["id"], scan=scan_payload())),
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
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(
                decision=AccessDecision(
                    allowed_columns=["id", "region"], masks={}, row_filter=None, policy_version=100
                )
            ),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(ticket_payload(columns=["id"], scan=scan_payload())),
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
    ticket_codec = FakeTicketCodec(ticket_payload(columns=["id"], scan=scan_payload()))
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(
                decision=AccessDecision(
                    allowed_columns=["id", "region", "active"],
                    masks={},
                    row_filter="active = true",
                    policy_version=100,
                )
            ),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
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
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(
                decision=AccessDecision(
                    allowed_columns=["id"], masks={}, row_filter=None, policy_version=100
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
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(ticket_payload(columns=["id"], scan=scan_payload())),
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
