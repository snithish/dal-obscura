from typing import Any, cast

import pytest

from dal_obscura.kernel.identity.authentication import AuthenticationRequest
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.request import PlanRequest
from tests.support.read.fakes import (
    AUTHORIZATION_HEADER,
    FakeAuthorizer,
    FakeCatalogRegistry,
    FakeIdentity,
    FakeTicketCodec,
    FakeTicketStore,
    StaticAccessContext,
    StubTableFormat,
    _build_use_case_dependencies,
    make_read_service,
    scan_payload,
    ticket_payload,
)


def test_cancellation_during_planning_does_not_issue_tickets():
    from threading import Event

    cancelled = Event()
    schema, decision, original = _build_use_case_dependencies()

    class CancellingSource(StubTableFormat):
        def plan(self, request, max_tickets):
            result = super().plan(request, max_tickets)
            cancelled.set()
            return result

    source = CancellingSource(
        catalog_name=original.catalog_name,
        table_name=original.table_name,
        format=original.format,
        schema=schema,
        batches=original.batches,
    )
    store = FakeTicketStore()
    reader = make_read_service(
        identity=FakeIdentity(Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(FakeAuthorizer(decision), FakeCatalogRegistry(source)),
        ticket_store=store,
    )
    with pytest.raises(InterruptedError, match="cancelled"):
        reader.plan(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]),
            AUTHORIZATION_HEADER,
            cancel_check=cancelled.is_set,
        )
    assert store.stored == []


def test_plan_access_rejects_authorization_without_asset_identity():
    _, decision, table_format = _build_use_case_dependencies()
    ticket_store = FakeTicketStore()
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(decision=decision, asset_id=None),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
        ticket_codec=FakeTicketCodec(),
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError, match="governed asset identity"):
        use_case.plan(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
        )

    assert ticket_store.stored == []


def test_plan_access_auth_failure():
    _schema, decision, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(ticket_payload(columns=["id", "region"], scan=scan_payload()))
    use_case = make_read_service(
        identity=FakeIdentity(principal=None),
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=cast(Any, catalog_registry)
        ),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError):
        use_case.plan(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AuthenticationRequest()
        )


def test_plan_access_authz_failure():
    _schema, _, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
    ticket_codec = FakeTicketCodec(ticket_payload(columns=["id", "region"], scan=scan_payload()))
    use_case = make_read_service(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(decision=None), catalog_registry=cast(Any, catalog_registry)
        ),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError):
        use_case.plan(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
        )


def test_plan_access_rejects_scan_payloads_above_configured_ticket_limit():
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
        max_ticket_payload_bytes=1,
    )

    with pytest.raises(ValueError, match="ticket byte limit"):
        use_case.plan(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
        )
