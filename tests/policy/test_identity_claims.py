import pytest

from dal_obscura.identity.claims import PrincipalClaimMapper


def test_principal_claim_mapper_extracts_subject_groups_and_attributes():
    mapper = PrincipalClaimMapper(
        subject_claim="sub",
        group_claims=["groups", "realm_access.roles", "resource_access.dal-obscura.roles"],
        attribute_claims={"department": "department", "clearance": "custom.clearance"},
    )

    principal = mapper.map_claims(
        {
            "sub": "user-123",
            "groups": ["/analytics", "finance"],
            "realm_access": {"roles": ["analyst"]},
            "resource_access": {"dal-obscura": {"roles": ["reader"]}},
            "department": "acme",
            "custom": {"clearance": "high"},
        }
    )

    assert principal.id == "user-123"
    assert principal.groups == ("/analytics", "finance", "analyst", "reader")
    assert principal.attributes == {"department": "acme", "clearance": "high"}


def test_principal_claim_mapper_rejects_missing_subject():
    mapper = PrincipalClaimMapper(subject_claim="sub")

    with pytest.raises(PermissionError, match="Missing subject"):
        mapper.map_claims({"groups": ["analyst"]})


def test_principal_claim_mapper_rejects_non_scalar_attributes():
    mapper = PrincipalClaimMapper(
        subject_claim="sub", attribute_claims={"department": "department"}
    )

    with pytest.raises(PermissionError, match="Invalid attribute claim"):
        mapper.map_claims({"sub": "user-123", "department": ["acme"]})


@pytest.fixture
def department_mapper():
    return PrincipalClaimMapper(
        attribute_claims={"department": "employee.dept"},
        attribute_definitions={"department": {"allowed_values": ["Engineering", "Finance"]}},
    )


@pytest.mark.parametrize(
    "claims,expected",
    [
        pytest.param(
            {"sub": "alice", "employee": {"dept": " Finance "}},
            {"department": "Finance"},
            id="trimmed-allowed-value",
        ),
        pytest.param({"sub": "alice"}, {}, id="missing-claim"),
    ],
)
def test_attribute_mapping_normalizes_allowed_and_missing_claims(
    department_mapper, claims, expected
):
    assert department_mapper.map_claims(claims).attributes == expected


def test_attribute_mapping_rejects_values_outside_its_domain(department_mapper):
    with pytest.raises(PermissionError, match="outside its allowed values"):
        department_mapper.map_claims({"sub": "alice", "employee": {"dept": "Admin"}})


def test_attribute_mapping_preview_uses_runtime_mapper_without_subject():
    mapper = PrincipalClaimMapper(attribute_claims={"department": "employee.dept"})
    assert mapper.map_attributes({"employee": {"dept": "Finance"}}) == {"department": "Finance"}
