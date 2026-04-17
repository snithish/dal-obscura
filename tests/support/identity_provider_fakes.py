from __future__ import annotations

from collections.abc import Mapping

from dal_obscura.domain.access_control.models import Principal


class RecordingIdentityProvider:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs

    def authenticate(self, headers: Mapping[str, str]) -> Principal:
        return Principal(id="provider-user", groups=[], attributes={})


class MissingAuthenticateProvider:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


class FailingIdentityProvider:
    def __init__(self, **kwargs: object) -> None:
        raise RuntimeError("identity provider boom")
