from dal_obscura.common.access_control.models import (
    AccessRule,
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
)
from dal_obscura.common.access_control.policy_resolution import resolve_access


def resolve(rules, principal=None):
    return resolve_access(
        Policy(version=1, datasets=[DatasetPolicy(target="orders", catalog="demo", rules=rules)]),
        principal or Principal(id="alice", groups=["analysts"], attributes={}),
        "orders",
        "demo",
        ["email", "country"],
    )


def test_unmatched_columns_are_returned_as_null():
    columns, masks, row_filter = resolve([])
    assert columns == ["email", "country"]
    assert masks == {"email": MaskRule(type="null"), "country": MaskRule(type="null")}
    assert row_filter is None


def test_grants_override_only_the_default_null_mask():
    rule = AccessRule(principals=["group:analysts"], columns=["email"], masks={}, row_filter=None)
    assert resolve([rule])[1] == {"country": MaskRule(type="null")}
    masked = AccessRule(
        principals=["group:analysts"],
        columns=["email"],
        masks={"email": MaskRule(type="hash")},
        row_filter=None,
    )
    assert resolve([rule, masked])[1]["email"].type == "hash"
    assert resolve([masked, rule])[1]["email"].type == "hash"


def test_row_only_rules_do_not_unmask_columns():
    rule = AccessRule(
        principals=["group:analysts"], columns=[], masks={}, row_filter="country = 'US'"
    )
    columns, masks, row_filter = resolve([rule])
    assert columns == ["email", "country"]
    assert len(masks) == 2
    assert row_filter == "(country = 'US')"


def test_allow_all_short_circuits_masks_and_row_filters():
    restricted = AccessRule(
        principals=["group:analysts"],
        columns=["email"],
        masks={"email": MaskRule(type="null")},
        row_filter="country = 'US'",
    )
    bypass = AccessRule(
        principals=["*"], columns=["*"], masks={}, row_filter=None, effect="allow_all"
    )
    assert resolve([restricted, bypass]) == (["email", "country"], {}, None)
    assert resolve([bypass, restricted], Principal(id="someone", groups=[], attributes={})) == (
        ["email", "country"],
        {},
        None,
    )


def test_explicit_null_wins_over_incompatible_masks_in_every_order():
    from itertools import permutations

    rules = [
        AccessRule(
            principals=["*"],
            columns=["email"],
            masks={"email": MaskRule(type=kind)},
            row_filter=None,
        )
        for kind in ("hash", "email", "null")
    ]
    for ordered in permutations(rules):
        assert resolve(list(ordered))[1]["email"] == MaskRule(type="null")


def test_policy_compiler_rejects_non_null_map_key_masks():
    import pytest

    from dal_obscura.control_plane.application.errors import ValidationFailure
    from dal_obscura.control_plane.application.policy_compiler import compile_policy_rule_payloads

    with pytest.raises(ValidationFailure, match="Map keys support only the null mask"):
        compile_policy_rule_payloads(
            [
                {
                    "principals": ["*"],
                    "columns": ["contacts.$key"],
                    "masks": {"contacts.$key": {"type": "hash"}},
                }
            ],
            [],
        )


def test_compiler_rejects_display_aliases_and_requires_canonical_paths():
    import pytest

    from dal_obscura.control_plane.application.errors import ValidationFailure
    from dal_obscura.control_plane.application.policy_compiler import compile_policy_rule_payloads

    fields: list[dict[str, object]] = [
        {"name": "contacts.email", "path": ["contacts", "$element", "email"], "type": "string"}
    ]
    with pytest.raises(ValidationFailure, match="Unknown column path"):
        compile_policy_rule_payloads([{"principals": ["*"], "columns": ["contacts.email"]}], fields)
    assert compile_policy_rule_payloads(
        [{"principals": ["*"], "columns": ["contacts.$element.email"]}], fields
    )[0]["columns"] == ["contacts.$element.email"]


def test_overlapping_masks_cannot_silently_replace_each_other():
    from itertools import permutations

    import pytest

    from dal_obscura.control_plane.application.errors import ValidationFailure
    from dal_obscura.control_plane.application.policy_compiler import compile_policy_rule_payloads

    fields: list[dict[str, object]] = [
        {"name": "profile.email", "path": ["profile", "email"], "type": "string"}
    ]
    masks = [("profile", {"type": "null"}), ("profile.email", {"type": "hash"})]
    for ordered in permutations(masks):
        with pytest.raises(ValidationFailure, match="Overlapping masks"):
            compile_policy_rule_payloads(
                [{"principals": ["*"], "columns": ["profile"], "masks": dict(ordered)}], fields
            )


def test_unused_mask_values_are_rejected_instead_of_affecting_conflict_resolution():
    import pytest

    from dal_obscura.control_plane.application.errors import ValidationFailure
    from dal_obscura.control_plane.application.policy_compiler import compile_policy_rule_payloads

    for kind in ("hash", "null", "email"):
        with pytest.raises(ValidationFailure, match="does not accept a value"):
            compile_policy_rule_payloads(
                [
                    {
                        "principals": ["*"],
                        "columns": ["email"],
                        "masks": {"email": {"type": kind, "value": "ignored"}},
                    }
                ],
                [],
            )
