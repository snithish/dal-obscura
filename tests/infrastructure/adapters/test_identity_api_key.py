import pytest

from dal_obscura.data_plane.application.ports.identity import (
    AuthenticationRequest,
    InvalidCredentialsError,
    MissingCredentialsError,
)
from dal_obscura.data_plane.infrastructure.adapters.identity_api_key import ApiKeyIdentityProvider


def test_api_key_provider_authenticates_x_api_key_header():
    provider = ApiKeyIdentityProvider(
        keys=[
            {
                "id": "spark-prod",
                "secret": "api-secret-1",
                "groups": ["spark", "analytics"],
                "attributes": {"tenant": "acme"},
            }
        ]
    )

    principal = provider.authenticate(AuthenticationRequest(headers={"x-api-key": "api-secret-1"}))

    assert principal.id == "spark-prod"
    assert principal.groups == ["spark", "analytics"]
    assert principal.attributes == {"tenant": "acme"}


def test_api_key_provider_can_read_bearer_authorization_header():
    provider = ApiKeyIdentityProvider(
        header="authorization",
        scheme="bearer",
        keys=[{"id": "batch-job", "secret": "batch-secret"}],
    )

    principal = provider.authenticate(
        AuthenticationRequest(headers={"authorization": "Bearer batch-secret"})
    )

    assert principal.id == "batch-job"


def test_api_key_provider_raises_missing_when_header_absent():
    provider = ApiKeyIdentityProvider(keys=[{"id": "spark-prod", "secret": "api-secret-1"}])

    with pytest.raises(MissingCredentialsError, match="Missing API key"):
        provider.authenticate(AuthenticationRequest(headers={}))


def test_api_key_provider_raises_invalid_when_header_value_does_not_match():
    provider = ApiKeyIdentityProvider(keys=[{"id": "spark-prod", "secret": "api-secret-1"}])

    with pytest.raises(InvalidCredentialsError, match="Invalid API key"):
        provider.authenticate(AuthenticationRequest(headers={"x-api-key": "wrong"}))
