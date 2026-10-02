from typing import Any, cast

import pytest

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.data_plane.application.ports.access_context import StaticAccessContext
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest
from dal_obscura.data_plane.application.use_cases.plan_access import PlanAccessUseCase
from tests.application.access_flow.helpers import (
    AUTHORIZATION_HEADER,
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
    scan_payload,
)


def test_plan_access_rejects_authorization_without_asset_identity():
    _, decision, table_format = _build_use_case_dependencies()
    ticket_store = FakeTicketStore()
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(decision=decision, asset_id=None),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
        masking=FakeMasking(),
        ticket_codec=FakeTicketCodec(),
        ticket_store=ticket_store,
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError, match="governed asset identity"):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
        )

    assert ticket_store.stored == []


def test_plan_access_auth_failure():
    _schema, decision, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
    authorizer = FakeAuthorizer(decision=decision)
    ticket_codec = FakeTicketCodec(ticket_payload(columns=["id", "region"], scan=scan_payload()))
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=None),
        access_context=StaticAccessContext(
            authorizer=authorizer, catalog_registry=cast(Any, catalog_registry)
        ),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AuthenticationRequest()
        )


def test_plan_access_authz_failure():
    _schema, _, table_format = _build_use_case_dependencies()
    catalog_registry = FakeCatalogRegistry(table_format)
    ticket_codec = FakeTicketCodec(ticket_payload(columns=["id", "region"], scan=scan_payload()))
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(decision=None), catalog_registry=cast(Any, catalog_registry)
        ),
        masking=FakeMasking(),
        ticket_codec=ticket_codec,
        ticket_store=FakeTicketStore(),
        ticket_ttl_seconds=300,
        max_tickets=1,
        max_ticket_exchanges=1,
    )

    with pytest.raises(PermissionError):
        use_case.execute(
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
        )


def test_plan_access_rejects_scan_payloads_above_configured_ticket_limit():
    _schema, decision, table_format = _build_use_case_dependencies()
    use_case = PlanAccessUseCase(
        identity=FakeIdentity(principal=Principal(id="user1", groups=[], attributes={})),
        access_context=StaticAccessContext(
            authorizer=FakeAuthorizer(decision=decision),
            catalog_registry=cast(Any, FakeCatalogRegistry(table_format)),
        ),
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
            PlanRequest(catalog="catalog1", target="users", columns=["id"]), AUTHORIZATION_HEADER
        )
