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
        # Federated identities are scoped by the exact issuer. Escape the
        # delimiter in each component so a subject/group containing ``|``
        # cannot collide with a different pair. Local/demo actors retain the
        # historical unscoped form.
        prefix = f"{_identity_component(self.issuer)}|" if self.issuer else ""
        principals = {
            f"{prefix}{_identity_component(self.principal) if self.issuer else self.principal}"
        }
        principals.update(
            f"{prefix}group:{_identity_component(group) if self.issuer else group}"
            for group in self.groups
        )
        return principals

    def identity_key(self) -> str:
        """Returns the stable storage key for this authenticated identity."""

        if not self.issuer:
            return self.principal
        return f"{_identity_component(self.issuer)}|{_identity_component(self.principal)}"


def _identity_component(value: str) -> str:
    """Escapes identity delimiters while preserving ordinary display values."""

    return value.replace("%", "%25").replace("|", "%7C")
