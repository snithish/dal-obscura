import pytest

from dal_obscura.data_plane.application.ports.identity import (
    AuthenticationRequest,
    InvalidCredentialsError,
    MissingCredentialsError,
)
from dal_obscura.data_plane.infrastructure.adapters.identity_trusted_headers import (
    TrustedHeaderIdentityProvider,
)


def test_trusted_header_provider_maps_gateway_headers_to_principal():
    provider = TrustedHeaderIdentityProvider(
        shared_secret="proxy-secret",
        subject_header="x-auth-request-user",
        groups_header="x-auth-request-groups",
        attribute_header_prefix="x-auth-request-attr-",
    )

    principal = provider.authenticate(
        AuthenticationRequest(
            headers={
                "x-dal-obscura-proxy-secret": "proxy-secret",
                "x-auth-request-user": "user1",
                "x-auth-request-groups": "analyst,finance",
                "x-auth-request-attr-tenant": "acme",
            }
        )
    )

    assert principal.id == "user1"
    assert principal.groups == ["analyst", "finance"]
    assert principal.attributes == {"tenant": "acme"}


def test_trusted_header_provider_raises_missing_when_proxy_secret_absent():
    provider = TrustedHeaderIdentityProvider(shared_secret="proxy-secret")

    with pytest.raises(MissingCredentialsError, match="Missing trusted proxy secret"):
        provider.authenticate(AuthenticationRequest(headers={}))


def test_trusted_header_provider_raises_invalid_when_proxy_secret_mismatches():
    provider = TrustedHeaderIdentityProvider(shared_secret="proxy-secret")

    with pytest.raises(InvalidCredentialsError, match="Invalid trusted proxy secret"):
        provider.authenticate(
            AuthenticationRequest(headers={"x-dal-obscura-proxy-secret": "wrong"})
        )


def test_trusted_header_provider_requires_subject_after_secret_validation():
    provider = TrustedHeaderIdentityProvider(shared_secret="proxy-secret")

    with pytest.raises(InvalidCredentialsError, match="Missing trusted subject"):
        provider.authenticate(
            AuthenticationRequest(headers={"x-dal-obscura-proxy-secret": "proxy-secret"})
        )
