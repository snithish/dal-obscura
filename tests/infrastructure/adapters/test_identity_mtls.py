import pytest

from dal_obscura.data_plane.application.ports.identity import (
    AuthenticationRequest,
    InvalidCredentialsError,
    MissingCredentialsError,
)
from dal_obscura.data_plane.infrastructure.adapters.identity_mtls import MtlsIdentityProvider


def test_mtls_provider_maps_peer_identity_to_configured_principal():
    provider = MtlsIdentityProvider(
        identities=[
            {
                "peer_identity": "spiffe://cluster/ns/default/sa/spark",
                "id": "spark-service",
                "groups": ["spark"],
                "attributes": {"tenant": "acme"},
            }
        ]
    )

    principal = provider.authenticate(
        AuthenticationRequest(peer_identity="spiffe://cluster/ns/default/sa/spark")
    )

    assert principal.id == "spark-service"
    assert principal.groups == ["spark"]
    assert principal.attributes == {"tenant": "acme"}


def test_mtls_provider_can_use_peer_identity_as_principal_id_when_no_mapping_is_configured():
    provider = MtlsIdentityProvider()

    principal = provider.authenticate(AuthenticationRequest(peer_identity="client-cert-subject"))

    assert principal.id == "client-cert-subject"
    assert principal.groups == []
    assert principal.attributes == {"peer_identity": "client-cert-subject"}


def test_mtls_provider_raises_missing_without_peer_identity():
    provider = MtlsIdentityProvider()

    with pytest.raises(MissingCredentialsError, match="Missing peer identity"):
        provider.authenticate(AuthenticationRequest())


def test_mtls_provider_raises_invalid_for_unmapped_peer_identity():
    provider = MtlsIdentityProvider(identities=[{"peer_identity": "client-a", "id": "service-a"}])

    with pytest.raises(InvalidCredentialsError, match="Unrecognized peer identity"):
        provider.authenticate(AuthenticationRequest(peer_identity="client-b"))
