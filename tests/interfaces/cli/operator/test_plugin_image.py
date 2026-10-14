"""Release verification must reject a replaced lock before probing readiness."""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest

from dal_obscura.interfaces.cli import plugin_image


@pytest.mark.parametrize("case", ["replaced_lock", "missing_plugin"])
def test_worker_verification_rejects_admission_drift_before_readiness(tmp_path, monkeypatch, case):
    path = tmp_path / "plugin-lock.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "plugins": [
                    {
                        "kind": "catalog",
                        "plugin_id": "acme.delta",
                        "lock": ["acme-delta-plugin", "1.0", "2", "a" * 64, "b" * 64],
                    }
                ],
            }
        )
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setenv("DAL_OBSCURA_PLUGIN_LOCK_FILE", str(path))
    monkeypatch.setattr(
        plugin_image.sys,
        "argv",
        ["image-tool", "verify", "data-plane", "c" * 64 if case == "replaced_lock" else digest],
    )
    monkeypatch.setattr(
        plugin_image,
        "create_plugin_registry",
        lambda **kwargs: SimpleNamespace(admitted=dict),
    )
    monkeypatch.setattr(
        plugin_image, "urlopen", lambda *a, **kw: pytest.fail("Must not query readiness")
    )
    with pytest.raises(
        RuntimeError, match="lock differs" if case == "replaced_lock" else "missing a selected"
    ):
        plugin_image.main()
