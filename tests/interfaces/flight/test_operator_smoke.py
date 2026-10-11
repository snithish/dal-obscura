"""Authenticated operator smoke reads through the real Flight transport."""

from __future__ import annotations

import pyarrow as pa
import pytest

from dal_obscura.interfaces.cli.operator import smoke_read
from tests.support.flight import (
    StubTableFormat,
    build_flight_service,
    make_jwt,
    running_flight_client,
)


@pytest.mark.socket
@pytest.mark.parametrize("case", ["success", "wrong_values", "invalid_token"])
def test_deployment_smoke_qualifies_authenticated_values(case, monkeypatch):
    schema = pa.schema(
        [("id", pa.int64()), ("tags", pa.list_(pa.string()))], metadata={b"fixture": b"operator"}
    )
    batch = pa.RecordBatch.from_pylist(
        [
            {"id": 1, "tags": ["alpha", None]},
            {"id": 2, "tags": []},
        ],
        schema=schema,
    )
    source = StubTableFormat(
        catalog_name="synthetic",
        table_name="default.smoke",
        format="fixture",
        schema=schema,
        batches=(batch,),
    )
    server = build_flight_service(table_format=source)
    monkeypatch.setenv(
        "DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN", "invalid" if case == "invalid_token" else make_jwt()
    )
    settings = {
        "uri": f"grpc+tcp://127.0.0.1:{server.port}",
        "catalog": "synthetic",
        "target": "default.smoke",
        "columns": ["id", "tags"],
        "expected_rows": [{"id": 2, "tags": []}, {"id": 1, "tags": ["alpha", None]}],
    }
    if case == "wrong_values":
        settings["expected_rows"][0]["id"] = 99
    with running_flight_client(server):
        if case == "success":
            smoke_read(settings)
        else:
            with pytest.raises(
                ValueError,
                match="values differ" if case == "wrong_values" else "Flight smoke failed",
            ):
                smoke_read(settings)
