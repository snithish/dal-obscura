from dal_obscura.common.access_control.models import AccessDecision, MaskRule, Principal


def test_principal_and_decision_copy_caller_collections() -> None:
    groups = ["analyst"]
    attributes = {"tenant": "one"}
    columns = ["id"]
    masks = {"email": MaskRule(type="email")}
    principal = Principal(id="alice", groups=groups, attributes=attributes)
    decision = AccessDecision(
        allowed_columns=columns,
        masks=masks,
        row_filter=None,
        policy_version=1,
    )

    groups.append("admin")
    attributes["tenant"] = "two"
    columns.append("email")
    masks.clear()

    assert principal.groups == ["analyst"]
    assert principal.attributes == {"tenant": "one"}
    assert decision.allowed_columns == ["id"]
    assert decision.masks == {"email": MaskRule(type="email")}
