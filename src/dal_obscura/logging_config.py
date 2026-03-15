from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

_STANDARD_LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    json: bool = True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(
            {
                key: value
                for key, value in record.__dict__.items()
                if key not in _STANDARD_LOG_RECORD_FIELDS and not key.startswith("_")
            }
        )
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), default=str)


def resolve_logging_config() -> LoggingConfig:
    level = os.getenv("DAL_OBSCURA_LOG_LEVEL", "INFO")
    json_enabled = os.getenv("DAL_OBSCURA_LOG_JSON", "true").lower() in {"1", "true", "yes"}
    return LoggingConfig(level=level, json=json_enabled)


def setup_logging(config: LoggingConfig | None = None) -> None:
    config = config or resolve_logging_config()
    root = logging.getLogger()
    root.setLevel(config.level.upper())
    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler()
    if config.json:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S%z",
            )
        )
    root.addHandler(handler)
