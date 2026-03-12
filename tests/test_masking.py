from dal_obscura.masking import build_select_list
from dal_obscura.policy import MaskRule


def test_mask_select_list_basic():
    selection = build_select_list(
        ["id", "name"],
        {"name": MaskRule(type="redact", value="***")},
    )
    assert "name" in selection.masked_columns
    assert "'***'" in selection.select_list[1]


def test_nested_mask_expression():
    selection = build_select_list(
        ["user.address.zip"],
        {"user.address.zip": MaskRule(type="hash")},
    )
    assert "struct_update" in selection.select_list[0]
