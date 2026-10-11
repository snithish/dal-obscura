"""Control transport bounds and fail-closed JSON through the operator boundary."""

from io import BytesIO
from types import SimpleNamespace

import pytest

from dal_obscura.interfaces.cli import operator


@pytest.fixture
def control_response(monkeypatch):
    responses = []
    monkeypatch.setenv("DAL_OBSCURA_DEPLOY_ADMIN_TOKEN", "sentinel-credential")

    class Response(BytesIO):
        consumed = 0

        def read(self, size=-1):
            result = super().read(size)
            self.consumed += len(result)
            return result

    def install(payload):
        response = Response(payload)
        responses.append(response)
        monkeypatch.setattr(
            operator, "build_opener", lambda *args: SimpleNamespace(open=lambda *a, **kw: response)
        )
        return response

    yield install
    for response in responses:
        response.close()


def request():
    return operator.api_request({"control_url": "https://control.example"}, "GET", "/v1/plugins")


def test_control_response_accepts_complete_json_at_byte_boundary(control_response):
    limit = 8 * 1024 * 1024
    response = control_response(b'{"plugins":[]}' + b" " * (limit - 14))
    assert request() == {"plugins": []}
    assert response.closed


def test_control_response_stops_reading_oversized_body_and_closes(control_response):
    limit = 8 * 1024 * 1024
    response = control_response(b"sentinel-secret" + b" " * (limit * 2))
    with pytest.raises(ValueError, match="Control API response exceeds") as error:
        request()
    assert response.consumed <= limit + 1
    assert response.closed
    assert "sentinel-secret" not in str(error.value)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"value":NaN}',
        b'{"value":Infinity}',
        b'{"value":-Infinity}',
        b'{"value":1e999}',
        b'{"secret":"sentinel-secret"',
        b'"\xff"',
        b"[" * 2000 + b"0" + b"]" * 2000,
    ],
    ids=[
        "nan",
        "infinity",
        "negative-infinity",
        "overflow",
        "malformed",
        "invalid-utf8",
        "deep-json",
    ],
)
def test_control_response_rejects_invalid_json_without_body_details(control_response, payload):
    response = control_response(payload)
    with pytest.raises(ValueError, match="Control API returned invalid JSON") as error:
        request()
    assert response.closed
    assert "sentinel-secret" not in str(error.value)
