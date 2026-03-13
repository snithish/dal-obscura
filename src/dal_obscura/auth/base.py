from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class AuthResult:
    principal_id: str
    groups: list[str]
    attributes: dict[str, str]


class Authenticator(ABC):
    @abstractmethod
    def authenticate(self, headers: Mapping[str, str]) -> AuthResult: ...
