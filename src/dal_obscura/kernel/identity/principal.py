from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True)
class Principal:
    """Authenticated caller plus any groups and free-form identity attributes."""

    id: str
    groups: Sequence[str]
    attributes: Mapping[str, str]
    issuer: str = ""
    expires_at: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "groups", tuple(self.groups))
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))

    def tokens(self) -> list[str]:
        """Returns tokens used by policy matching, including `group:` prefixes."""
        return [self.id, *[f"group:{group}" for group in self.groups]]
