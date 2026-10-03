from datetime import datetime, timedelta, timezone

import pytest
from dal_obscura_plugin_api import ExecutionContext


@pytest.fixture
def context():
    return ExecutionContext(datetime.now(timezone.utc) + timedelta(minutes=2), "delta-test")
