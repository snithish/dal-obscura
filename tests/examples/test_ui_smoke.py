from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


def _module() -> ModuleType:
    path = Path(__file__).parents[2] / "examples/demo/keycloak/scripts/ui_smoke.py"
    spec = importlib.util.spec_from_file_location("ui_smoke", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ui_smoke_runs_full_authenticated_browser_contract(monkeypatch, capsys) -> None:
    module = _module()
    responses = iter(
        [
            (200, b'<div id="root">', {"Content-Security-Policy": "default-src 'self'"}),
            (200, b'{"authority":"http://keycloak","client_id":"dal-obscura-ui"}', {}),
            (200, b'{"authenticated":true}', {}),
            (200, b'{"principal":"platform:admin"}', {}),
            (200, b"[]", {}),
            (200, b'{"authenticated":false}', {}),
            (401, b'{"detail":"Unauthorized"}', {}),
        ]
    )
    monkeypatch.setattr(module, "request", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr(module, "cookie_value", lambda *args: "csrf-token")
    monkeypatch.setattr(module, "control_plane_admin_token", lambda: "test-admin")

    module.main()

    assert "ui-smoke: browser session, authorization, and logout passed" in capsys.readouterr().out


def test_ui_smoke_reports_actionable_root_failure(monkeypatch) -> None:
    module = _module()
    monkeypatch.setattr(module, "request", lambda *args, **kwargs: (503, b"", {}))

    with pytest.raises(module.SmokeFailure, match="UI root returned 503"):
        module.main()
