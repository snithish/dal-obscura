from dal_obscura.domain.access_control import MaskRule
from dal_obscura.infrastructure.adapters.duckdb_transform import DefaultMaskingAdapter


def test_mask_select_list_basic():
    selection = DefaultMaskingAdapter().apply(
        ["id", "name"],
        {"name": MaskRule(type="redact", value="***")},
    )
    assert "name" in selection.masked_columns
    assert "'***'" in selection.select_list[1]


def test_nested_mask_expression():
    selection = DefaultMaskingAdapter().apply(
        ["user.address.zip"],
        {"user.address.zip": MaskRule(type="hash")},
    )
    assert "struct_update" in selection.select_list[0]


def test_default_mask_renders_literal():
    selection = DefaultMaskingAdapter().apply(
        ["status"],
        {"status": MaskRule(type="default", value="unknown")},
    )
    assert "'unknown'" in selection.select_list[0]
