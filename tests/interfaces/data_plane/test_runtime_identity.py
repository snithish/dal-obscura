from __future__ import annotations

import pytest

from dal_obscura.data_plane.infrastructure.adapters.secret_providers import EnvSecretProvider
from dal_obscura.data_plane.interfaces.cli.main import _load_identity_provider


def test_runtime_rejects_dynamic_identity_provider_module():
    with pytest.raises(ValueError, match="only built-in OIDC"):
        _load_identity_provider(
            {"module": "untrusted.module.Provider", "args": {}},
            secret_provider=EnvSecretProvider(),
        )
