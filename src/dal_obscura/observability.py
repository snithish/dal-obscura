"""Small observability helpers shared by service adapters.

Example:
    ```python
    rss = get_resident_memory_bytes()
    ```
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock
from time import perf_counter

import psutil

_PROCESS = psutil.Process()


def get_resident_memory_bytes() -> int:
    """Returns resident set size for the current process in bytes."""
    return _PROCESS.memory_info().rss


@dataclass(frozen=True, slots=True)
class MetricSnapshot:
    """Bounded aggregate for one operation and outcome."""

    count: int
    duration_seconds: float


class ServiceMetrics:
    """Thread-safe low-cardinality request metrics.

    Metric keys are operation names supplied by trusted service code.  Dynamic
    request values such as principals, table names, URIs, and correlation IDs
    are deliberately not accepted, preventing accidental aggregate leaks and
    unbounded cardinality.
    """

    _MAX_OPERATION_LENGTH = 64
    _MAX_OPERATIONS = 64

    def __init__(self) -> None:
        self._lock = Lock()
        self._values: dict[tuple[str, str], MetricSnapshot] = {}

    @contextmanager
    def measure(self, operation: str) -> Iterator[None]:
        """Record one operation as ``success`` or ``error`` with elapsed time."""

        started = perf_counter()
        try:
            yield
        except Exception:
            self.observe(operation, "error", perf_counter() - started)
            raise
        else:
            self.observe(operation, "success", perf_counter() - started)

    def observe(self, operation: str, outcome: str, duration_seconds: float = 0.0) -> None:
        """Add one bounded aggregate observation."""

        if (
            not operation
            or len(operation) > self._MAX_OPERATION_LENGTH
            or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for char in operation)
        ):
            raise ValueError("metric operation name is invalid or too long")
        if outcome not in {"success", "error"}:
            raise ValueError("metric outcome must be success or error")
        if duration_seconds < 0:
            raise ValueError("metric duration cannot be negative")
        key = (operation, outcome)
        with self._lock:
            operation_count = len({item[0] for item in self._values})
            if key not in self._values and operation_count >= self._MAX_OPERATIONS:
                raise ValueError("metric operation limit exceeded")
            current = self._values.get(key, MetricSnapshot(0, 0.0))
            self._values[key] = MetricSnapshot(
                count=current.count + 1,
                duration_seconds=current.duration_seconds + duration_seconds,
            )

    def snapshot(self) -> dict[str, dict[str, dict[str, float | int]]]:
        """Return a copy suitable for health or metrics endpoints."""

        with self._lock:
            return {
                operation: {
                    outcome: {
                        "count": snapshot.count,
                        "duration_seconds": snapshot.duration_seconds,
                    }
                    for (item_operation, outcome), snapshot in self._values.items()
                    if item_operation == operation
                }
                for operation in sorted({item[0] for item in self._values})
            }
