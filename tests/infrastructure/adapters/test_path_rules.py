from __future__ import annotations

import pytest

from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer


def test_path_rule_enforcer_allows_configured_roots_and_descendants():
    enforcer = PathRuleEnforcer(
        [
            {"root": "/warehouse"},
            {"root": "s3://analytics-demo/delta"},
        ]
    )

    enforcer.check("/warehouse")
    enforcer.check("/warehouse/retail/customer_revenue/data.parquet")
    enforcer.check("s3://analytics-demo/delta/customer_revenue/part-0.parquet")

    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("/warehouse-private/customer_revenue/data.parquet")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://analytics-demo/delta-private/customer_revenue/part-0.parquet")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://other-bucket/delta/customer_revenue/part-0.parquet")


def test_path_rule_enforcer_rejects_glob_rules_and_wildcard_roots():
    with pytest.raises(ValueError, match="glob patterns are no longer supported"):
        PathRuleEnforcer([{"glob": "s3://warehouse/*", "allow": True}])

    with pytest.raises(ValueError, match="wildcards are not supported"):
        PathRuleEnforcer([{"root": "s3://warehouse/*"}])


def test_path_rule_enforcer_is_disabled_when_no_roots_are_published():
    enforcer = PathRuleEnforcer([])

    assert enforcer.enabled is False

    enforcer.check("s3://any-bucket/any/path.parquet")
    enforcer.check("/local/dev/path.parquet")


def test_path_rule_enforcer_normalizes_uri_traversal_before_root_check():
    enforcer = PathRuleEnforcer([{"root": "s3://analytics-demo/delta"}])

    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://analytics-demo/delta/part/../../secrets.parquet")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://analytics-demo/delta%2F..%2Fsecrets.parquet")


def test_path_rule_enforcer_rejects_credential_and_query_roots():
    with pytest.raises(ValueError, match="credentials"):
        PathRuleEnforcer([{"root": "s3://user:password@analytics-demo/delta"}])
    with pytest.raises(ValueError, match="query or fragment"):
        PathRuleEnforcer([{"root": "s3://analytics-demo/delta?token=secret"}])


def test_path_rule_enforcer_accepts_explicit_local_file_uris():
    enforcer = PathRuleEnforcer([{"root": "file:///warehouse/curated"}])

    enforcer.check("file:///warehouse/curated/orders/data.parquet")
    enforcer.check("/warehouse/curated/orders/data.parquet")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("file:///warehouse/private/data.parquet")


def test_path_rule_enforcer_rejects_remote_file_authorities():
    with pytest.raises(ValueError, match="must be local"):
        PathRuleEnforcer([{"root": "file://remote-host/warehouse"}])


def test_path_rule_enforcer_rejects_non_string_roots():
    with pytest.raises(ValueError, match="must be strings"):
        PathRuleEnforcer([{"root": 42}])


def test_path_rule_enforcer_allows_all_paths_within_uri_authority_root():
    enforcer = PathRuleEnforcer([{"root": "s3://analytics-demo/"}])

    enforcer.check("s3://analytics-demo/warehouse/data.parquet")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://other-bucket/warehouse/data.parquet")
