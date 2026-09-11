from __future__ import annotations

import http.cookiejar
import json
import sys
import urllib.error
import urllib.request
from typing import cast

BASE_URL = "http://127.0.0.1:8821"


class SmokeFailure(RuntimeError):
    """Raised when the local governance application fails a required check."""


def request(
    opener: urllib.request.OpenerDirector,
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, object] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, bytes, dict[str, str]]:
    body = json.dumps(payload).encode() if payload is not None else None
    request_headers = {"accept": "application/json", **(headers or {})}
    if body is not None:
        request_headers["content-type"] = "application/json"
    request = urllib.request.Request(
        f"{BASE_URL}{path}", data=body, headers=request_headers, method=method
    )
    try:
        with opener.open(request, timeout=10) as response:
            return response.status, response.read(), dict(response.headers.items())
    except urllib.error.HTTPError as error:
        return error.code, error.read(), dict(error.headers.items())
    except urllib.error.URLError as error:
        raise SmokeFailure(f"request to {path} failed: {error.reason}") from error


def cookie_value(cookies: http.cookiejar.CookieJar, name: str) -> str:
    for cookie in cookies:
        if cookie.name == name and cookie.value:
            return cookie.value
    raise SmokeFailure(f"missing {name} cookie")


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeFailure(message)


def json_object(raw: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise SmokeFailure(f"{label} returned invalid JSON") from error
    if not isinstance(value, dict):
        raise SmokeFailure(f"{label} returned a JSON value other than an object")
    return value


def main() -> None:
    cookies = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))

    status, html, headers = request(opener, "/", headers={"accept": "text/html"})
    expect(status == 200, f"UI root returned {status}, expected 200")
    expect(b'<div id="root">' in html, "UI root did not contain the application root")
    expect("Content-Security-Policy" in headers, "UI root omitted Content-Security-Policy")

    status, raw, _ = request(opener, "/v1/ui-auth-config")
    expect(status == 200, f"UI auth configuration returned {status}, expected 200")
    shortcuts = json_object(raw, "UI auth configuration").get("login_shortcuts", [])
    if not isinstance(shortcuts, list):
        raise SmokeFailure("UI auth configuration has invalid login shortcuts")
    owner = next(
        (
            item
            for item in shortcuts
            if isinstance(item, dict)
            and cast(dict[str, object], item).get("login_hint") == "asset-owner"
        ),
        None,
    )
    if owner is None:
        raise SmokeFailure("UI auth configuration omitted the asset-owner login shortcut")

    status, raw, _ = request(
        opener, "/v1/demo-login", method="POST", payload={"login_hint": "asset-owner"}
    )
    expect(
        status == 200 and json_object(raw, "demo login") == {"authenticated": True},
        "demo login did not create a browser session",
    )
    csrf = cookie_value(cookies, "dal_obscura_csrf")

    status, raw, _ = request(opener, "/v1/session")
    expect(
        status == 200 and json_object(raw, "session").get("principal") == "asset-owner",
        "asset-owner browser session was not accepted",
    )
    status, _, _ = request(opener, "/v1/assets")
    expect(status == 200, f"asset-owner asset inventory returned {status}, expected 200")

    status, raw, _ = request(opener, "/v1/logout", method="POST", headers={"x-csrf-token": csrf})
    expect(
        status == 200 and json_object(raw, "logout") == {"authenticated": False},
        "logout did not expire the browser session",
    )
    status, _, _ = request(opener, "/v1/session")
    expect(status == 401, f"session remained authenticated after logout: {status}")
    print("ui-smoke: browser session, authorization, and logout passed")


if __name__ == "__main__":
    try:
        main()
    except (SmokeFailure, TimeoutError) as error:
        print(f"ui-smoke failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
