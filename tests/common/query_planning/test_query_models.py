import pytest

from dal_obscura.common.query_planning.models import PlanRequest


def test_plan_request_copies_caller_columns() -> None:
    columns = ["id"]
    request = PlanRequest(target="users", columns=columns)

    columns.append("email")

    assert request.columns == ["id"]


@pytest.mark.parametrize(
    "target,columns",
    [
        ("", ["id"]),
        ("users", []),
        ("users", ["id", "id"]),
        ("users", ["*", "id"]),
        ("users", ["id", 1]),
    ],
)
def test_plan_request_rejects_invalid_projection_shape(target, columns) -> None:
    with pytest.raises(ValueError):
        PlanRequest(target=target, columns=columns)
