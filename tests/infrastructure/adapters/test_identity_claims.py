import pytest

from dal_obscura.infrastructure.adapters.identity_claims import PrincipalClaimMapper


def test_principal_claim_mapper_extracts_subject_groups_and_attributes():
    mapper = PrincipalClaimMapper(
        subject_claim="sub",
        group_claims=["groups", "realm_access.roles", "resource_access.dal-obscura.roles"],
        attribute_claims={"tenant": "tenant", "clearance": "custom.clearance"},
    )

    principal = mapper.map_claims(
        {
            "sub": "user-123",
            "groups": ["/analytics", "finance"],
            "realm_access": {"roles": ["analyst"]},
            "resource_access": {"dal-obscura": {"roles": ["reader"]}},
            "tenant": "acme",
            "custom": {"clearance": "high"},
        }
    )

    assert principal.id == "user-123"
    assert principal.groups == ["/analytics", "finance", "analyst", "reader"]
    assert principal.attributes == {"tenant": "acme", "clearance": "high"}


def test_principal_claim_mapper_rejects_missing_subject():
    mapper = PrincipalClaimMapper(subject_claim="sub")

    with pytest.raises(PermissionError, match="Missing subject"):
        mapper.map_claims({"groups": ["analyst"]})


def test_principal_claim_mapper_rejects_non_scalar_attributes():
    mapper = PrincipalClaimMapper(
        subject_claim="sub",
        attribute_claims={"tenant": "tenant"},
    )

    with pytest.raises(PermissionError, match="Invalid attribute claim"):
        mapper.map_claims({"sub": "user-123", "tenant": ["acme"]})
