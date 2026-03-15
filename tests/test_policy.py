import textwrap

from dal_obscura.policy import Principal, load_policy, resolve_access


def test_resolve_access_allows_columns(tmp_path):
    policy_text = textwrap.dedent(
        """
        version: 1
        datasets:
          - table: "catalog.db.table"
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
    policy = load_policy(policy_path)
    principal = Principal(id="user1", groups=["analyst"], attributes={})

    allowed, masks, row_filter = resolve_access(
        policy,
        principal,
        "catalog.db.table",
        ["id", "name", "region"],
    )

    assert allowed == ["id", "name"]
    assert "name" in masks
    assert row_filter == "(region = 'us')"


def test_resolve_access_unions_columns_and_filters(tmp_path):
    policy_text = textwrap.dedent(
        """
        version: 1
        datasets:
          - table: "catalog.db.table"
            rules:
              - principals: ["user1"]
                columns: ["id"]
                row_filter: "region = 'us'"
              - principals: ["user1"]
                columns: ["name"]
                masks:
                  name: {type: "redact", value: "***"}
                row_filter: "active = true"
        """
    )
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(policy_text)
    policy = load_policy(policy_path)
    principal = Principal(id="user1", groups=[], attributes={})

    allowed, masks, row_filter = resolve_access(
        policy,
        principal,
        "catalog.db.table",
        ["id", "name", "region"],
    )

    assert allowed == ["id", "name"]
    assert "name" in masks
    assert row_filter == "(region = 'us') AND (active = true)"
