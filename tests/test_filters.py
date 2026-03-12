from dal_obscura.filters import normalize_filter


def test_normalize_filter():
    assert normalize_filter("  ").where_sql is None
    assert normalize_filter("a = 1").where_sql == "a = 1"
