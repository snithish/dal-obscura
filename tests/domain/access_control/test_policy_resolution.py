import pytest

from dal_obscura.common.access_control.models import (
    AccessRule,
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
)
from dal_obscura.common.access_control.policy_resolution import dataset_version, resolve_access


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

    assert allowed == ["id", "name"]
    assert masks["name"].type == "redact"
    assert row_filter == "(region = 'us')"


def test_resolve_access_default_denies_without_matching_grant():
    policy = _policy(
        AccessRule(
            principals=["group:analyst"],
            columns=["id"],
            masks={},
            row_filter=None,
        )
    )
    principal = Principal(id="user1", groups=["guest"], attributes={})

    with pytest.raises(PermissionError, match="No allowed columns"):
        resolve_access(
            policy,
            principal,
            target="catalog.db.table",
            catalog="analytics",
            requested_columns=["id"],
        )


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


def test_resolve_access_rejects_incomparable_masks_regardless_of_rule_order():
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

    for ordered_rules in (rules, tuple(reversed(rules))):
        with pytest.raises(PermissionError, match="Conflicting masks"):
            resolve_access(
                _policy(*ordered_rules),
                principal,
                target="catalog.db.table",
                catalog="analytics",
                requested_columns=["email"],
            )


def test_resolve_access_allows_by_role_and_principal_attributes():
    policy = _policy(
        AccessRule(
            principals=["group:analyst"],
            when={"tenant": "acme"},
            columns=["id", "region"],
            masks={},
            row_filter="region = 'us'",
        )
    )
    principal = Principal(id="user1", groups=["analyst"], attributes={"tenant": "acme"})

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
        AccessRule(
            principals=["user1"],
            columns=["id"],
            masks={},
            row_filter="region = 'us'",
        ),
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

    assert allowed == ["id", "name"]
    assert "name" in masks
    assert row_filter == "(region = 'us') AND (active = true)"


def test_policy_version_changes_when_abac_clauses_change():
    first = _policy(
        AccessRule(
            principals=["group:analyst"],
            when={"tenant": "acme"},
            columns=["id"],
            masks={},
            row_filter=None,
        )
    ).datasets[0]
    second = _policy(
        AccessRule(
            principals=["group:analyst"],
            when={"tenant": "globex"},
            columns=["id"],
            masks={},
            row_filter=None,
        )
    ).datasets[0]

    assert dataset_version(first) != dataset_version(second)


def _policy(
    *rules: AccessRule,
    catalog: str | None = "analytics",
    target: str = "catalog.db.table",
) -> Policy:
    return Policy(
        version=1,
        datasets=[DatasetPolicy(target=target, catalog=catalog, rules=list(rules))],
    )
