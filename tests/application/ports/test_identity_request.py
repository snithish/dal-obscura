from collections.abc import Mapping

from dal_obscura.application.ports.identity import AuthenticationRequest


def test_authentication_request_normalizes_headers_and_exposes_explicit_header_lookup():
    request = AuthenticationRequest(
        headers={"Authorization": "Bearer token-1", "X-Api-Key": "secret-1"},
        peer_identity="spiffe://cluster/ns/default/sa/spark",
        peer="ipv4:127.0.0.1:50000",
        method="get_flight_info",
    )

    assert not isinstance(request, Mapping)
    assert request.header("authorization") == "Bearer token-1"
    assert request.header("x-api-key") == "secret-1"
    assert request.headers == {
        "authorization": "Bearer token-1",
        "x-api-key": "secret-1",
    }
    assert request.peer_identity == "spiffe://cluster/ns/default/sa/spark"
    assert request.peer == "ipv4:127.0.0.1:50000"
    assert request.method == "get_flight_info"


def test_authentication_request_returns_none_for_missing_header():
    request = AuthenticationRequest()

    assert request.header("authorization") is None
