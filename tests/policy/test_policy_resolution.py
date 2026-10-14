import pytest

from dal_obscura.policy.models import (
    AccessRule,
    AssetPolicy,
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
)
from dal_obscura.policy.policy_resolution import resolve_access


def test_resolve_access_allows_columns():
    policy = _policy(
        AccessRule(
            principals=["user1", "group:analyst"],
            columns=["id", "name"],
            masks={"name": MaskRule(type="redact", value="***")},
            row_filter="region = 'us'",
        )
    )
    principal = Principal(id="user1", groups=["analyst"], attributes={})

    allowed, masks, row_filter = resolve_access(
        policy,
        principal,
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["id", "name", "region"],
    )

    assert allowed == ["id", "name", "region"]
    assert masks["region"] == MaskRule(type="null")
    assert masks["name"].type == "redact"
    assert row_filter == "(region = 'us')"


def test_resolve_access_defaults_to_null_without_matching_grant():
    policy = _policy(
        AccessRule(principals=["group:analyst"], columns=["id"], masks={}, row_filter=None)
    )
    principal = Principal(id="user1", groups=["guest"], attributes={})

    allowed, masks, row_filter = resolve_access(
        policy, principal, target="catalog.db.table", catalog="analytics", requested_columns=["id"]
    )
    assert allowed == ["id"]
    assert masks == {"id": MaskRule(type="null")}
    assert row_filter is None


def test_resolve_access_unions_grants_filters_and_strictest_masks():
    policy = _policy(
        AccessRule(
            principals=["group:analyst"],
            columns=["id", "email"],
            masks={"email": MaskRule(type="hash")},
            row_filter="region = 'us'",
        ),
        AccessRule(
            principals=["user1"],
            columns=["email", "region"],
            masks={"email": MaskRule(type="null")},
            row_filter="active = true",
        ),
    )
    principal = Principal(id="user1", groups=["analyst"], attributes={})

    allowed, masks, row_filter = resolve_access(
        policy,
        principal,
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["id", "email", "region"],
    )

    assert allowed == ["id", "email", "region"]
    assert masks["email"].type == "null"
    assert row_filter == "(region = 'us') AND (active = true)"


@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
def test_resolve_access_rejects_incomparable_masks_regardless_of_rule_order(reverse):
    principal = Principal(id="user1", groups=[], attributes={})
    rules = (
        AccessRule(
            principals=["user1"],
            columns=["email"],
            masks={"email": MaskRule(type="email")},
            row_filter=None,
        ),
        AccessRule(
            principals=["user1"],
            columns=["email"],
            masks={"email": MaskRule(type="keep_last", value=4)},
            row_filter=None,
        ),
    )

    ordered_rules = tuple(reversed(rules)) if reverse else rules
    with pytest.raises(PermissionError, match="Conflicting masks"):
        resolve_access(
            _policy(*ordered_rules),
            principal,
            target="catalog.db.table",
            catalog="analytics",
            requested_columns=["email"],
        )


@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
def test_resolve_access_combines_keep_last_masks_to_smaller_value_regardless_of_order(reverse):
    principal = Principal(id="user1", groups=[], attributes={})
    rules = (
        AccessRule(
            principals=["user1"],
            columns=["account_id"],
            masks={"account_id": MaskRule(type="keep_last", value=4)},
            row_filter=None,
        ),
        AccessRule(
            principals=["user1"],
            columns=["account_id"],
            masks={"account_id": MaskRule(type="keep_last", value=2)},
            row_filter=None,
        ),
    )

    ordered_rules = tuple(reversed(rules)) if reverse else rules
    _allowed, masks, _row_filter = resolve_access(
        _policy(*ordered_rules),
        principal,
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["account_id"],
    )

    assert masks["account_id"] == MaskRule(type="keep_last", value=2)


def test_resolve_access_allows_by_role_and_principal_attributes():
    policy = _policy(
        AccessRule(
            principals=["group:analyst"],
            when={"department": "acme"},
            columns=["id", "region"],
            masks={},
            row_filter="region = 'us'",
        )
    )
    principal = Principal(id="user1", groups=["analyst"], attributes={"department": "acme"})

    allowed, _masks, row_filter = resolve_access(
        policy,
        principal,
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["id", "region"],
    )

    assert allowed == ["id", "region"]
    assert row_filter == "(region = 'us')"


def test_resolve_access_unions_columns_and_filters_for_matching_grants():
    policy = _policy(
        AccessRule(principals=["user1"], columns=["id"], masks={}, row_filter="region = 'us'"),
        AccessRule(
            principals=["user1"],
            columns=["name"],
            masks={"name": MaskRule(type="redact", value="***")},
            row_filter="active = true",
        ),
        catalog=None,
        target="/landing/*.parquet",
    )
    principal = Principal(id="user1", groups=[], attributes={})

    allowed, masks, row_filter = resolve_access(
        policy,
        principal,
        target="/landing/data.parquet",
        catalog=None,
        requested_columns=["id", "name", "region"],
    )

    assert allowed == ["id", "name", "region"]
    assert masks["region"] == MaskRule(type="null")
    assert "name" in masks
    assert row_filter == "(region = 'us') AND (active = true)"


def test_resolve_access_nulls_ungranted_nested_siblings():
    policy = _policy(
        AccessRule(principals=["user1"], columns=["profile.name"], masks={}, row_filter=None)
    )

    allowed, masks, _filter = resolve_access(
        policy,
        Principal(id="user1", groups=[], attributes={}),
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["profile.name", "profile.email"],
    )

    assert allowed == ["profile.name", "profile.email"]
    assert masks == {"profile.email": MaskRule(type="null")}


def test_resolve_access_parent_grant_authorizes_requested_nested_leaf():
    policy = _policy(
        AccessRule(principals=["user1"], columns=["profile"], masks={}, row_filter=None)
    )

    allowed, _masks, _filter = resolve_access(
        policy,
        Principal(id="user1", groups=[], attributes={}),
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["profile.name"],
    )

    assert allowed == ["profile.name"]


def test_mask_exemption_skips_only_that_rule_mask_and_preserves_grant_and_filter():
    policy = _policy(
        AccessRule(
            principals=["group:analyst"],
            columns=["email"],
            masks={"email": MaskRule(type="hash", exempt_principals=("group:privacy",))},
            row_filter="country = 'US'",
        ),
        AccessRule(
            principals=["group:analyst"],
            columns=["email"],
            masks={"email": MaskRule(type="hash")},
            row_filter=None,
        ),
    )
    for ordered in (policy.datasets[0].rules, list(reversed(policy.datasets[0].rules))):
        allowed, masks, row_filter = resolve_access(
            _policy(*ordered),
            Principal(id="alice", groups=["analyst", "privacy"], attributes={}),
            target="catalog.db.table",
            catalog="analytics",
            requested_columns=["email"],
        )
        assert allowed == ["email"]
        assert masks["email"] == MaskRule(type="hash")
        assert row_filter == "(country = 'US')"

    exempt_only = _policy(policy.datasets[0].rules[0])
    _allowed, masks, _filter = resolve_access(
        exempt_only,
        Principal(id="alice", groups=["analyst", "privacy"], attributes={}),
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["email"],
    )
    assert masks == {}


def test_exempt_non_reader_gets_no_grant_and_condition_mismatch_does_not_exempt():
    policy = _policy(
        AccessRule(
            principals=["group:analyst"],
            when={"department": "acme"},
            columns=["email"],
            masks={"email": MaskRule(type="hash", exempt_principals=("user:alice",))},
            row_filter="active = true",
        )
    )
    for principal in [
        Principal(id="alice", groups=[], attributes={"department": "acme"}),
        Principal(id="bob", groups=["analyst"], attributes={"department": "other"}),
    ]:
        allowed, masks, row_filter = resolve_access(
            policy, principal, "catalog.db.table", "analytics", ["email"]
        )
        assert allowed == ["email"]
        assert masks == {"email": MaskRule(type="null")}
        assert row_filter is None


def test_compiled_mask_exemptions_round_trip():
    rule = AccessRule(
        ordinal=0,
        effect="allow",
        principals=["group:analyst"],
        columns=["email"],
        masks={"email": MaskRule(type="hash", exempt_principals=("group:privacy", "user:alice"))},
    )
    policy = AssetPolicy(version=1, catalog="analytics", target="orders", rules=[rule])
    decoded = AssetPolicy.from_json(policy.to_json())
    assert decoded.rules[0].masks["email"].exempt_principals == ("group:privacy", "user:alice")


def _policy(
    *rules: AccessRule,
    catalog: str | None = "analytics",
    target: str = "catalog.db.table",
) -> Policy:
    return Policy(
        version=1, datasets=[DatasetPolicy(target=target, catalog=catalog, rules=list(rules))]
    )
