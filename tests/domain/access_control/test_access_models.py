from dal_obscura.common.access_control.models import (
    AccessDecision,
    AccessRule,
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
    PrincipalConditionValue,
)


def test_principal_and_decision_copy_caller_collections() -> None:
    groups = ["analyst"]
    attributes = {"department": "one"}
    columns = ["id"]
    masks = {"email": MaskRule(type="email")}
    principal = Principal(id="alice", groups=groups, attributes=attributes)
    decision = AccessDecision(
        allowed_columns=columns, masks=masks, row_filter=None, policy_version=1
    )

    groups.append("admin")
    attributes["department"] = "two"
    columns.append("email")
    masks.clear()

    assert principal.groups == ["analyst"]
    assert principal.attributes == {"department": "one"}
    assert decision.allowed_columns == ["id"]
    assert decision.masks == {"email": MaskRule(type="email")}


def test_policy_models_copy_nested_caller_collections() -> None:
    principals = ["group:analyst"]
    columns = ["id"]
    masks = {"email": MaskRule(type="email")}
    when: dict[str, PrincipalConditionValue] = {"department": "one"}
    rule = AccessRule(
        principals=principals, columns=columns, masks=masks, row_filter=None, when=when
    )
    rules = [rule]
    dataset = DatasetPolicy(target="users", catalog="analytics", rules=rules)
    datasets = [dataset]
    policy = Policy(version=1, datasets=datasets)

    principals.append("group:admin")
    columns.append("email")
    masks.clear()
    when["department"] = "two"
    rules.clear()
    datasets.clear()

    assert policy.datasets[0].rules[0].principals == ["group:analyst"]
    assert policy.datasets[0].rules[0].columns == ["id"]
    assert policy.datasets[0].rules[0].masks == {"email": MaskRule(type="email")}
    assert policy.datasets[0].rules[0].when == {"department": "one"}
