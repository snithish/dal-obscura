from __future__ import annotations

import http.cookiejar
import json
import sys
import urllib.error
import urllib.request

BASE_URL = "http://127.0.0.1:8821"


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


def cookie_value(cookies: http.cookiejar.CookieJar, name: str) -> str:
    for cookie in cookies:
        if cookie.name == name and cookie.value:
            return cookie.value
    raise AssertionError(f"missing {name} cookie")


def main() -> None:
    cookies = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))

    status, html, headers = request(opener, "/", headers={"accept": "text/html"})
    assert status == 200
    assert b'<div id="root">' in html
    assert "Content-Security-Policy" in headers

    status, raw, _ = request(opener, "/v1/ui-auth-config")
    assert status == 200
    shortcuts = json.loads(raw).get("login_shortcuts", [])
    owner = next(item for item in shortcuts if item["login_hint"] == "asset-owner")

    status, raw, _ = request(
        opener, "/v1/demo-login", method="POST", payload={"login_hint": owner["login_hint"]}
    )
    assert status == 200 and json.loads(raw) == {"authenticated": True}
    csrf = cookie_value(cookies, "dal_obscura_csrf")

    status, raw, _ = request(opener, "/v1/session")
    assert status == 200 and json.loads(raw)["principal"] == "asset-owner"
    status, _, _ = request(opener, "/v1/assets")
    assert status == 200

    status, raw, _ = request(opener, "/v1/logout", method="POST", headers={"x-csrf-token": csrf})
    assert status == 200 and json.loads(raw) == {"authenticated": False}
    status, _, _ = request(opener, "/v1/session")
    assert status == 401
    print("ui-smoke: browser session, authorization, and logout passed")


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, KeyError, StopIteration, TimeoutError) as error:
        print(f"ui-smoke failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
