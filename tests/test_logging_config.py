import json
import logging

from dal_obscura.logging_config import JsonFormatter


def test_json_formatter_includes_extra_fields():
    record = logging.LogRecord(
        name="dal_obscura.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.table = "users"
    record.columns = ["id"]
    record.resident_memory_bytes = 1234

    payload = json.loads(JsonFormatter().format(record))

    assert payload["message"] == "hello"
    assert payload["table"] == "users"
    assert payload["columns"] == ["id"]
    assert payload["resident_memory_bytes"] == 1234
