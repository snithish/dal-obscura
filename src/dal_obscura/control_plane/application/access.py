from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ControlPlaneActor:
    principal: str
    groups: tuple[str, ...]
    platform_admin: bool = False

    @classmethod
    def for_platform_admin(cls, principal: str) -> ControlPlaneActor:
        return cls(principal=principal, groups=(), platform_admin=True)

    def owner_principals(self) -> set[str]:
        principals = {self.principal}
        principals.update(f"group:{group}" for group in self.groups)
        return principals
