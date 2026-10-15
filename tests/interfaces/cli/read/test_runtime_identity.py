from __future__ import annotations

import pytest

from dal_obscura.application.reads.sources import LiveRuntime
from dal_obscura.interfaces.cli.read import (
    _identity_from_runtime,
    _load_identity_provider,
)
from dal_obscura.runtime.secrets import SecretProviderConfig, load_secret_provider


def test_runtime_rejects_dynamic_identity_provider_module():
    with pytest.raises(ValueError, match="only built-in OIDC"):
        _load_identity_provider(
            {"module": "untrusted.module.Provider", "args": {}},
            secret_provider=load_secret_provider(SecretProviderConfig()),
        )


def test_runtime_rejects_multiple_enabled_identity_providers():
    runtime = LiveRuntime(
        auth_chain={"providers": [{"enabled": True}, {"enabled": True}]},
        ticket={},
    )

    with pytest.raises(ValueError, match="exactly one enabled OIDC"):
        _identity_from_runtime(
            runtime, secret_provider=load_secret_provider(SecretProviderConfig())
        )
