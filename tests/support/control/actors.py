"""Synthetic identities for authorization contracts, not live SSO evidence."""

from dataclasses import dataclass

from fastapi.testclient import TestClient


@dataclass(frozen=True)
class DemoToken:
    principal: str
    groups: tuple[str, ...] = ()
    issuer: str = ""


def _actor_for_token(token: str) -> DemoToken:
    if token == "owner-token":
        return DemoToken("asset-owner", ("asset-owners",))
    if token == "grant-manager-token":
        return DemoToken("grant-manager", ())
    if token == "outsider-token":
        return DemoToken("outsider", ("analysts",))
    if token == "admin-oidc-token":
        return DemoToken("demo-admin", ("platform-admins",))
    if token == "editor-token":
        return DemoToken("editor")
    if token == "issuer-a-owner-token":
        return DemoToken("alice", issuer="https://issuer-a.example/")
    if token == "issuer-b-owner-token":
        return DemoToken("alice", issuer="https://issuer-b.example")
    raise PermissionError("bad token")


def _client(client_factory, *, secure: bool = False) -> TestClient:
    return client_factory(
        oidc_actor_resolver=_actor_for_token,
        oidc_admin_group="platform-admins",
        base_url="https://testserver" if secure else "http://testserver",
    )


def _bearer(token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"}
