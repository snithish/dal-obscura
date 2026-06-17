from __future__ import annotations

import pytest


def test_path_rule_enforcer_allows_configured_roots_and_descendants():
    from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer

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
    from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer

    with pytest.raises(ValueError, match="glob patterns are no longer supported"):
        PathRuleEnforcer([{"glob": "s3://warehouse/*", "allow": True}])

    with pytest.raises(ValueError, match="wildcards are not supported"):
        PathRuleEnforcer([{"root": "s3://warehouse/*"}])


def test_path_rule_enforcer_preserves_existing_behavior_when_rules_are_empty():
    from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer

    enforcer = PathRuleEnforcer([])

    enforcer.check("s3://any-bucket/any/path.parquet")
    enforcer.check("/local/dev/path.parquet")
