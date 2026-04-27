from dataclasses import dataclass
from typing import Any, cast

import pyarrow as pa

from dal_obscura.application.ports.identity import AuthenticationRequest
from dal_obscura.application.use_cases.get_schema import GetSchemaResult
from dal_obscura.interfaces.flight.contracts import authentication_request_from_context
from dal_obscura.interfaces.flight.server import DataAccessFlightService
from tests.support.flight import command_descriptor


class DummyContext:
    def __init__(self, headers):
        self.headers = headers

    def get_middleware(self, key):
        del key

    def peer_identity(self):
        return "spiffe://cluster/ns/default/sa/spark"

    def peer(self):
        return "ipv4:127.0.0.1:50000"


@dataclass
class RecordingGetSchemaUseCase:
    auth_request: AuthenticationRequest | None = None

    def execute(self, request: object, auth_request: AuthenticationRequest) -> GetSchemaResult:
        del request
        self.auth_request = auth_request
        return GetSchemaResult(
            output_schema=pa.schema([pa.field("id", pa.int64())]),
            target="default.users",
            columns=["id"],
            principal_id="user1",
            policy_version=1,
            catalog="analytics",
        )


def test_authentication_request_from_context_includes_headers_and_peer_metadata():
    context = DummyContext(headers=[(b"authorization", b"Bearer token-1")])

    request = authentication_request_from_context(context, method="get_flight_info")

    assert request.get("authorization") == "Bearer token-1"
    assert request.peer_identity == "spiffe://cluster/ns/default/sa/spark"
    assert request.peer == "ipv4:127.0.0.1:50000"
    assert request.method == "get_flight_info"


def test_flight_service_passes_authentication_request_to_use_case():
    get_schema = RecordingGetSchemaUseCase()
    service = DataAccessFlightService(
        location="grpc+tcp://0.0.0.0:0",
        get_schema_use_case=cast(Any, get_schema),
        plan_access_use_case=cast(Any, object()),
        fetch_stream_use_case=cast(Any, object()),
    )
    try:
        service.get_schema(
            DummyContext(headers=[(b"authorization", b"Bearer token-1")]),
            command_descriptor(
                {"catalog": "analytics", "target": "default.users", "columns": ["id"]}
            ),
        )
    finally:
        service.shutdown()

    auth_request = get_schema.auth_request
    assert auth_request is not None
    assert auth_request.get("authorization") == "Bearer token-1"
    assert auth_request.peer_identity == "spiffe://cluster/ns/default/sa/spark"
    assert auth_request.method == "get_schema"
