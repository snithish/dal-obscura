from dal_obscura.control_plane.application.access import ControlPlaneActor


def test_federated_identity_preserves_exact_issuer_and_escapes_delimiters() -> None:
    actor = ControlPlaneActor(
        principal="subject|one%two",
        groups=("group|one%two",),
        issuer="https://issuer.example/realm/",
    )

    assert actor.identity_key() == "https://issuer.example/realm/|u|subject%7Cone%25two"
    assert actor.owner_principals() == {
        "https://issuer.example/realm/|u|subject%7Cone%25two",
        "https://issuer.example/realm/|g|group%7Cone%25two",
    }


def test_federated_subject_and_group_names_cannot_collide() -> None:
    subject = ControlPlaneActor(
        principal="group:analyst", groups=(), issuer="https://issuer.example"
    )
    group = ControlPlaneActor(
        groups=("analyst",), issuer="https://issuer.example", principal="other"
    )

    assert subject.identity_key() != next(iter(group.owner_principals() - {group.identity_key()}))


def test_local_identity_keeps_existing_unscoped_representation() -> None:
    actor = ControlPlaneActor(principal="local|operator", groups=("local|group",))

    assert actor.identity_key() == "local|operator"
    assert actor.owner_principals() == {"local|operator", "group:local|group"}
