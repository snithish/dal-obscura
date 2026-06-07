from __future__ import annotations

import pytest


def test_path_rule_enforcer_allows_and_denies_ordered_globs():
    from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer

    enforcer = PathRuleEnforcer(
        [
            {"glob": "s3://warehouse/private/*", "allow": False},
            {"glob": "s3://warehouse/*", "allow": True},
        ]
    )

    enforcer.check("s3://warehouse/public/users/data.parquet")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://warehouse/private/users/data.parquet")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://other-bucket/users/data.parquet")


def test_path_rule_enforcer_preserves_existing_behavior_when_rules_are_empty():
    from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer

    enforcer = PathRuleEnforcer([])

    enforcer.check("s3://any-bucket/any/path.parquet")
    enforcer.check("/local/dev/path.parquet")
