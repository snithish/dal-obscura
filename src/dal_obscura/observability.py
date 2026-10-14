"""Small observability helpers shared by service adapters."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from math import isfinite
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
        self._values: dict[str, dict[str, MetricSnapshot]] = {}

    @contextmanager
    def measure(self, operation: str) -> Generator[None]:
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
        if not isfinite(duration_seconds) or duration_seconds < 0:
            raise ValueError("metric duration must be finite and non-negative")
        with self._lock:
            if operation not in self._values and len(self._values) >= self._MAX_OPERATIONS:
                raise ValueError("metric operation limit exceeded")
            outcomes = self._values.setdefault(operation, {})
            current = outcomes.get(outcome, MetricSnapshot(0, 0.0))
            outcomes[outcome] = MetricSnapshot(
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
                    for outcome, snapshot in outcomes.items()
                }
                for operation, outcomes in sorted(self._values.items())
            }
