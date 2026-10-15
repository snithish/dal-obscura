"""Authenticated control API transport with bounded strict JSON responses."""

from __future__ import annotations

import json
import math
import os
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from dal_obscura_plugin_api.execution import close_preserving_error
from pydantic_core import from_json

MAX_CONTROL_RESPONSE_BYTES = 8 * 1024 * 1024


class NoRedirect(HTTPRedirectHandler):
    """Never forward deployment credentials to a redirect destination."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        try:
            raise ValueError("Control API redirects are not allowed; configure the final HTTPS URL")
        finally:
            close_preserving_error(fp)


def api_request(
    target: dict[str, Any], method: str, path: str, body: dict[str, Any] | None = None
) -> Any:
    base = validate_control_url(str(target["control_url"]))
    token = os.environ[target.get("admin_token_env", "DAL_OBSCURA_DEPLOY_ADMIN_TOKEN")]
    request = Request(
        base + path,
        method=method,
        data=None if body is None else json.dumps(body, allow_nan=False).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with build_opener(NoRedirect()).open(request, timeout=30) as response:
            payload = response.read(MAX_CONTROL_RESPONSE_BYTES + 1)
            if len(payload) > MAX_CONTROL_RESPONSE_BYTES:
                raise ValueError("Control API response exceeds the 8 MiB limit")
            try:
                # The maintained parser also rejects excessive nesting without Python recursion.
                result = from_json(payload, allow_inf_nan=False, cache_strings=False)
                pending = [result]
                while pending:
                    value = pending.pop()
                    if isinstance(value, float) and not math.isfinite(value):
                        raise ValueError("Non-finite number")
                    if isinstance(value, dict):
                        pending.extend(value.values())
                    elif isinstance(value, list):
                        pending.extend(value)
                return result
            except ValueError:
                raise ValueError("Control API returned invalid JSON") from None
    except HTTPError as error:
        close_preserving_error(error)
        raise


def validate_control_url(value: str) -> str:
    base = value.rstrip("/")
    parsed = urlsplit(base)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("control_url must not contain credentials, query or fragment")
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    ):
        raise ValueError("control_url requires HTTPS except on loopback")
    if not parsed.hostname:
        raise ValueError("control_url requires a hostname")
    return base
