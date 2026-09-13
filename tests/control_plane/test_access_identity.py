from dal_obscura.control_plane.application.access import ControlPlaneActor


def test_federated_identity_preserves_exact_issuer_and_escapes_delimiters() -> None:
    actor = ControlPlaneActor(
        principal="subject|one%two",
        groups=("group|one%two",),
        issuer="https://issuer.example/realm/",
    )

    assert actor.identity_key() == "https://issuer.example/realm/|subject%7Cone%25two"
    assert actor.owner_principals() == {
        "https://issuer.example/realm/|subject%7Cone%25two",
        "https://issuer.example/realm/|group:group%7Cone%25two",
    }


def test_local_identity_keeps_existing_unscoped_representation() -> None:
    actor = ControlPlaneActor(principal="local|operator", groups=("local|group",))

    assert actor.identity_key() == "local|operator"
    assert actor.owner_principals() == {"local|operator", "group:local|group"}
