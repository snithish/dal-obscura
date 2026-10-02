from __future__ import annotations

from dataclasses import dataclass

from dal_obscura.identity.names import encode_federated_group, encode_federated_identity

_MAX_IDENTITY_TEXT_LENGTH = 1_024
_MAX_GROUPS = 256


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

    def __post_init__(self) -> None:
        if not _valid_identity_text(self.principal, allow_empty=False):
            raise ValueError("Actor principal must be a bounded printable string")
        if not isinstance(self.groups, tuple) or len(self.groups) > _MAX_GROUPS:
            raise ValueError("Actor groups must be a bounded tuple")
        if any(not _valid_identity_text(group, allow_empty=False) for group in self.groups):
            raise ValueError("Actor groups must be bounded printable strings")
        if not _valid_identity_text(self.issuer, allow_empty=True):
            raise ValueError("Actor issuer must be a bounded printable string")

    @classmethod
    def for_platform_admin(cls, principal: str) -> ControlPlaneActor:
        return cls(principal=principal, groups=(), platform_admin=True)

    def owner_principals(self) -> set[str]:
        # Federated identities are scoped by the exact issuer. Escape the
        # delimiter in each component so a subject/group containing ``|``
        # cannot collide with a different pair. Local/demo actors retain the
        # historical unscoped form.
        principals = {
            encode_federated_identity(self.issuer, self.principal)
            if self.issuer
            else self.principal
        }
        principals.update(
            encode_federated_group(self.issuer, group) if self.issuer else f"group:{group}"
            for group in self.groups
        )
        return principals

    def identity_key(self) -> str:
        """Returns the stable storage key for this authenticated identity."""

        if not self.issuer:
            return self.principal
        return encode_federated_identity(self.issuer, self.principal)


def _valid_identity_text(value: object, *, allow_empty: bool) -> bool:
    return (
        isinstance(value, str)
        and (allow_empty or bool(value))
        and len(value) <= _MAX_IDENTITY_TEXT_LENGTH
        and all(ord(char) >= 0x20 and ord(char) != 0x7F for char in value)
    )
