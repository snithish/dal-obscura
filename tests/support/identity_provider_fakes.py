from __future__ import annotations

from dal_obscura.common.access_control.models import Principal
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest


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
