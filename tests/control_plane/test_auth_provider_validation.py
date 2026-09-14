import pytest

from dal_obscura.control_plane.application.auth_provider_validation import (
    OIDC_IDENTITY_MODULE,
    redact_auth_provider,
    validate_auth_provider_payloads,
)
from dal_obscura.control_plane.application.errors import ValidationFailure


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


@pytest.mark.parametrize("field", ["issuer", "jwks_url"])
def test_auth_provider_endpoints_reject_query_data(field: str):
    args = {
        "issuer": "https://issuer.example/realm",
        "jwks_url": "https://issuer.example/realm/certs",
    }
    args[field] += "?redirect=https://attacker.example"

    with pytest.raises(ValidationFailure, match="query data"):
        validate_auth_provider_payloads(
            [{"ordinal": 1, "module": OIDC_IDENTITY_MODULE, "args": args, "enabled": True}]
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("subject_claim", "claims..sub"),
        ("group_claims", "groups"),
        ("attribute_claims", {"tenant": ["tenant"]}),
        ("leeway_seconds", -1),
        ("jwks_refresh_interval_seconds", 0),
        ("max_jwks_keys", 0),
    ],
)
def test_auth_provider_claim_and_cache_options_have_strict_types(field: str, value: object):
    args: dict[str, object] = {"issuer": "https://issuer.example/realm", field: value}

    with pytest.raises(ValidationFailure):
        validate_auth_provider_payloads(
            [{"ordinal": 1, "module": OIDC_IDENTITY_MODULE, "args": args, "enabled": True}]
        )


def test_auth_provider_accepts_full_claim_and_cache_configuration():
    validate_auth_provider_payloads(
        [
            {
                "ordinal": 1,
                "module": OIDC_IDENTITY_MODULE,
                "args": {
                    "issuer": "https://issuer.example/realm",
                    "audience": ["gateway", "analytics"],
                    "algorithms": ["RS256"],
                    "subject_claim": "sub",
                    "group_claims": ["groups", "realm_access.roles"],
                    "attribute_claims": {"tenant": "tenant.id"},
                    "leeway_seconds": 30,
                    "jwks_refresh_interval_seconds": 60,
                    "max_jwks_keys": 512,
                },
                "enabled": True,
            }
        ]
    )
