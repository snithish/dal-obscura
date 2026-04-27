from dal_obscura.application.ports.identity import (
    AuthenticationRequest,
    coerce_authentication_request,
)


def test_authentication_request_behaves_like_headers_mapping():
    request = AuthenticationRequest(
        headers={"Authorization": "Bearer token-1", "X-Api-Key": "secret-1"},
        peer_identity="spiffe://cluster/ns/default/sa/spark",
        peer="ipv4:127.0.0.1:50000",
        method="get_flight_info",
    )

    assert request["authorization"] == "Bearer token-1"
    assert request.get("x-api-key") == "secret-1"
    assert dict(request.items()) == {
        "authorization": "Bearer token-1",
        "x-api-key": "secret-1",
    }
    assert request.peer_identity == "spiffe://cluster/ns/default/sa/spark"
    assert request.peer == "ipv4:127.0.0.1:50000"
    assert request.method == "get_flight_info"


def test_coerce_authentication_request_preserves_existing_context_when_not_overridden():
    original = AuthenticationRequest(
        headers={"authorization": "Bearer token-1"},
        peer_identity="client-cert-subject",
        peer="ipv4:127.0.0.1:50000",
        method="do_get",
    )

    coerced = coerce_authentication_request(original)

    assert coerced is original
    assert coerced.peer_identity == "client-cert-subject"
    assert coerced.method == "do_get"


def test_coerce_authentication_request_wraps_plain_headers():
    request = coerce_authentication_request(
        {"authorization": "Bearer token-1"},
        peer_identity="peer-subject",
        peer="peer-address",
        method="get_schema",
    )

    assert request.get("authorization") == "Bearer token-1"
    assert request.peer_identity == "peer-subject"
    assert request.peer == "peer-address"
    assert request.method == "get_schema"
