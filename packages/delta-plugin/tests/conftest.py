from datetime import UTC, datetime, timedelta

import pytest
from dal_obscura_plugin_api import ExecutionContext


@pytest.fixture
def context():
    return ExecutionContext(datetime.now(UTC) + timedelta(minutes=2), "delta-test")
