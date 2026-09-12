from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ControlPlaneActor:
    """Authenticated control-plane caller.

    Example:
        ```python
        actor = ControlPlaneActor.for_platform_admin("admin")
        assert actor.platform_admin
        ```
    """

    principal: str
    groups: tuple[str, ...]
    platform_admin: bool = False
    issuer: str = ""

    @classmethod
    def for_platform_admin(cls, principal: str) -> ControlPlaneActor:
        return cls(principal=principal, groups=(), platform_admin=True)

    def owner_principals(self) -> set[str]:
        # Federated identities are scoped by issuer so two providers cannot
        # collide on the same subject or group display name. Local/demo actors
        # retain the historical unscoped form for compatibility.
        prefix = f"{self.issuer.rstrip('/')}|" if self.issuer else ""
        principals = {f"{prefix}{self.principal}"}
        principals.update(f"{prefix}group:{group}" for group in self.groups)
        return principals

    def identity_key(self) -> str:
        """Returns the stable storage key for this authenticated identity."""

        if not self.issuer:
            return self.principal
        return f"{self.issuer.rstrip('/')}|{self.principal}"
