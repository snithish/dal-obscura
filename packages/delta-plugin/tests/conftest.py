from datetime import UTC, datetime, timedelta

import pytest
from dal_obscura_plugin_api import ExecutionContext


@pytest.fixture
def context():
    return ExecutionContext(datetime.now(UTC) + timedelta(minutes=2), "delta-test")


@pytest.fixture(scope="session")
def native_spark():
    from dal_obscura_plugin_conformance.native_delta import delta_writer

    with delta_writer() as spark:
        yield spark


@pytest.fixture
def native_delta(native_spark):
    from functools import partial

    from dal_obscura_plugin_conformance.native_delta import delete_delta_rows

    return partial(delete_delta_rows, spark=native_spark)
