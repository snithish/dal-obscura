from dal_obscura.common.query_planning.models import PlanRequest


def test_plan_request_copies_caller_columns() -> None:
    columns = ["id"]
    request = PlanRequest(target="users", columns=columns)

    columns.append("email")

    assert request.columns == ["id"]
