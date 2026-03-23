import textwrap

import pytest

from dal_obscura.domain.access_control.models import Principal
from dal_obscura.domain.access_control.policy_resolution import resolve_access
from dal_obscura.infrastructure.adapters.policy_file_authorizer import load_policy_config


def test_resolve_access_allows_columns(tmp_path):
    policy_text = textwrap.dedent(
        """
        version: 1
        catalogs:
          analytics:
            targets:
              "catalog.db.table":
                rules:
                  - principals: ["user1", "group:analyst"]
                    columns: ["id", "name"]
                    masks:
                      name: {type: "redact", value: "***"}
                    row_filter: "region = 'us'"
        """
    )
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(policy_text)
    policy = load_policy_config(policy_path)
    principal = Principal(id="user1", groups=["analyst"], attributes={})

    allowed, masks, row_filter = resolve_access(
        policy,
        principal,
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["id", "name", "region"],
    )

    assert allowed == ["id", "name"]
    assert "name" in masks
    assert row_filter == "(region = 'us')"


def test_resolve_access_unions_columns_and_filters(tmp_path):
    policy_text = textwrap.dedent(
        """
        version: 1
        paths:
          - target: "/landing/*.parquet"
            rules:
              - principals: ["user1"]
                columns: ["id"]
                row_filter: "region = 'us'"
          - target: "/landing/*.parquet"
            rules:
              - principals: ["user1"]
                columns: ["name"]
                masks:
                  name: {type: "redact", value: "***"}
                row_filter: "active = true"
        """
    )
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(policy_text)
    policy = load_policy_config(policy_path)
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


def test_policy_config_supports_string_mask_shorthand(tmp_path):
    policy_text = textwrap.dedent(
        """
        version: 1
        catalogs:
          analytics:
            targets:
              "catalog.db.table":
                rules:
                  - principals: ["user1"]
                    columns: ["id", "name"]
                    masks:
                      name: redact
        """
    )
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(policy_text)

    policy = load_policy_config(policy_path)
    principal = Principal(id="user1", groups=[], attributes={})

    _allowed, masks, _row_filter = resolve_access(
        policy,
        principal,
        target="catalog.db.table",
        catalog="analytics",
        requested_columns=["id", "name"],
    )

    assert masks["name"].type == "redact"


def test_policy_config_rejects_unknown_keys(tmp_path):
    policy_text = textwrap.dedent(
        """
        version: 1
        catalogs:
          analytics:
            targets:
              "catalog.db.table":
                rules:
                  - principals: ["user1"]
                    columns: ["id"]
                    unknown: true
        """
    )
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(policy_text)

    with pytest.raises(ValueError, match="unknown"):
        load_policy_config(policy_path)
