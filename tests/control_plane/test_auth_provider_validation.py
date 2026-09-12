from dal_obscura.control_plane.application.auth_provider_validation import (
    redact_auth_provider,
)


def test_auth_provider_redaction_hides_legacy_sensitive_values_and_scope_fields():
    result = redact_auth_provider(
        {
            "id": "provider-1",
            "cell_id": "private-cell",
            "ordinal": 1,
            "module": "provider",
            "args": {
                "issuer": "https://issuer.example",
                "client_secret": "legacy-secret",
                "nested": {"token": "legacy-token"},
                "jwks": {"keys": [{"kty": "RSA"}]},
            },
            "enabled": True,
        }
    )

    assert "cell_id" not in result
    assert result["args"] == {
        "issuer": "https://issuer.example",
        "client_secret": "[redacted]",
        "nested": {"token": "[redacted]"},
        "jwks": "[redacted]",
    }
