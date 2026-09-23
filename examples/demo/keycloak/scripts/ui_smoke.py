from __future__ import annotations

import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

DEMO_DIR = Path(__file__).resolve().parents[1]


def _base_url() -> str:
    port = os.environ.get("DAL_OBSCURA_DEMO_UI_PORT")
    if port is None:
        env_path = DEMO_DIR / ".runtime" / "control-plane.env"
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                key, separator, value = line.partition("=")
                if separator and key == "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ORIGIN":
                    return value
        port = "28821"
    return f"http://127.0.0.1:{port}"


BASE_URL = _base_url()


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


def control_plane_admin_token() -> str:
    """Read local bootstrap secret without exposing it in smoke output."""
    path = DEMO_DIR / ".runtime" / "control-plane.env"
    if not path.exists():
        raise SmokeFailure(f"missing local control-plane environment: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() == "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN" and value.strip():
            return value.strip()
    raise SmokeFailure("control-plane environment omitted admin token")


def main() -> None:
    cookies = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))

    status, html, headers = request(opener, "/", headers={"accept": "text/html"})
    expect(status == 200, f"UI root returned {status}, expected 200")
    expect(b'<div id="root">' in html, "UI root did not contain the application root")
    expect("Content-Security-Policy" in headers, "UI root omitted Content-Security-Policy")

    status, raw, _ = request(opener, "/v1/ui-auth-config")
    expect(status == 200, f"UI auth configuration returned {status}, expected 200")
    auth_config = json_object(raw, "UI auth configuration")
    expect(bool(auth_config.get("authority")), "UI auth configuration omitted OIDC authority")
    expect(bool(auth_config.get("client_id")), "UI auth configuration omitted OIDC client ID")

    status, raw, _ = request(
        opener,
        "/v1/session/bootstrap",
        method="POST",
        headers={"authorization": f"Bearer {control_plane_admin_token()}"},
    )
    expect(
        status == 200 and json_object(raw, "bootstrap") == {"authenticated": True},
        "local bootstrap did not create a browser session",
    )
    csrf = cookie_value(cookies, "dal_obscura_csrf")

    status, raw, _ = request(opener, "/v1/session")
    expect(
        status == 200 and json_object(raw, "session").get("principal") == "platform:admin",
        "local admin browser session was not accepted",
    )
    status, _, _ = request(opener, "/v1/assets")
    expect(status == 200, f"admin asset inventory returned {status}, expected 200")

    status, raw, _ = request(opener, "/v1/logout", method="POST", headers={"x-csrf-token": csrf})
    expect(
        status == 200 and json_object(raw, "logout") == {"authenticated": False},
        f"logout failed with status {status}: {raw.decode('utf-8', errors='replace')[:200]}",
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
