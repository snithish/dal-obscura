from __future__ import annotations

from dal_obscura.kernel.identity.authentication import AuthenticationRequest
from dal_obscura.kernel.identity.principal import Principal


class RecordingIdentityProvider:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs

    def authenticate(self, request: AuthenticationRequest) -> Principal:
        del request
        return Principal(id="provider-user", groups=[], attributes={})


class MissingAuthenticateProvider:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


class FailingIdentityProvider:
    def __init__(self, **kwargs: object) -> None:
        raise RuntimeError("identity provider boom")
