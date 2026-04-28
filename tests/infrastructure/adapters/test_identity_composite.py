import pytest

from dal_obscura.application.ports.identity import (
    AuthenticationRequest,
    InvalidCredentialsError,
    MissingCredentialsError,
)
from dal_obscura.domain.access_control.models import Principal
from dal_obscura.infrastructure.adapters.identity_composite import CompositeIdentityProvider


class MissingProvider:
    def authenticate(self, request: AuthenticationRequest) -> Principal:
        del request
        raise MissingCredentialsError("Missing token")


class InvalidProvider:
    def authenticate(self, request: AuthenticationRequest) -> Principal:
        del request
        raise InvalidCredentialsError("Invalid token")


class AcceptingProvider:
    def authenticate(self, request: AuthenticationRequest) -> Principal:
        assert isinstance(request, AuthenticationRequest)
        return Principal(id="user1", groups=["analyst"], attributes={"tenant": "acme"})


def test_composite_tries_next_provider_only_when_credentials_are_missing():
    provider = CompositeIdentityProvider([MissingProvider(), AcceptingProvider()])

    principal = provider.authenticate(AuthenticationRequest(headers={"x-api-key": "secret"}))

    assert principal.id == "user1"
    assert principal.groups == ["analyst"]
    assert principal.attributes == {"tenant": "acme"}


def test_composite_does_not_fall_back_after_invalid_credentials():
    provider = CompositeIdentityProvider([InvalidProvider(), AcceptingProvider()])

    with pytest.raises(InvalidCredentialsError, match="Invalid token"):
        provider.authenticate(AuthenticationRequest(headers={"authorization": "Bearer bad"}))


def test_composite_requires_at_least_one_provider():
    with pytest.raises(ValueError, match="at least one"):
        CompositeIdentityProvider([])
