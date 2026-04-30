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


def test_resolve_access_unions_columns_and_filters():
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


def test_resolve_access_denies_override_allows():
    policy = _policy(
        AccessRule(
            principals=["group:analyst"],
            columns=["id", "email"],
            masks={},
            row_filter=None,
        ),
        AccessRule(
            principals=["group:analyst"],
            when={"clearance": "low"},
            effect="deny",
            columns=["email"],
            masks={},
            row_filter=None,
        ),
    )
    principal = Principal(id="user1", groups=["analyst"], attributes={"clearance": "low"})

    allowed, _masks, row_filter = resolve_access(
        policy,
        principal,
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["id", "email"],
    )

    assert allowed == ["id"]
    assert row_filter is None


def test_resolve_access_applies_deny_precedence_across_multiple_rules():
    policy = _policy(
        AccessRule(
            principals=["group:analyst"],
            columns=["id"],
            masks={},
            row_filter=None,
        ),
        AccessRule(
            principals=["group:analyst"],
            when={"region_scope": ["eu", "global"]},
            columns=["email"],
            masks={},
            row_filter=None,
        ),
        AccessRule(
            principals=["group:analyst"],
            when={"region_scope": "eu"},
            effect="deny",
            columns=["email"],
            masks={},
            row_filter=None,
        ),
    )
    principal = Principal(id="user1", groups=["analyst"], attributes={"region_scope": "eu"})

    allowed, _masks, _row_filter = resolve_access(
        policy,
        principal,
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["id", "email"],
    )

    assert allowed == ["id"]


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
