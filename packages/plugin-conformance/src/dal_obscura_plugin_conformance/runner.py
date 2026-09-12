"""Contract-level checks that external plugin distributions can invoke."""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import islice

import pyarrow as pa
from dal_obscura_plugin_api import (
    ExecutionContext,
    SchemaDescriptor,
    TableFormatPlugin,
    TableHandle,
)


@dataclass
class ConformanceResult:
    """Machine-readable result for one plugin pair check run."""

    package: str
    plugin_id: str
    core_version: str = "0.1.0"
    arrow_version: str = pa.__version__
    artifact_identity: str | None = None
    capability_matrix: dict[str, bool] = field(default_factory=dict)
    checks: dict[str, str] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)
    skips: list[str] = field(default_factory=list)

    def record_pass(self, name: str) -> None:
        self.checks[name] = "passed"

    def record_failure(self, name: str, message: str) -> None:
        self.checks[name] = "failed"
        self.failures.append(f"{name}: {message}")

    def to_dict(self) -> dict[str, object]:
        return {
            "package": self.package,
            "plugin_id": self.plugin_id,
            "core_version": self.core_version,
            "arrow_version": self.arrow_version,
            "artifact_identity": self.artifact_identity,
            "capability_matrix": dict(self.capability_matrix),
            "checks": dict(self.checks),
            "failures": list(self.failures),
            "skips": list(self.skips),
            "status": "failed" if self.failures else "passed",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


def check_capabilities(
    advertised: Iterable[str],
    required: Iterable[str],
    *,
    result: ConformanceResult | None = None,
) -> None:
    """Fail when a plugin omits a capability required by a test cell."""

    missing = sorted(set(required) - set(advertised))
    if missing:
        message = f"missing required capabilities: {', '.join(missing)}"
        if result is not None:
            result.record_failure("capabilities", message)
        raise ValueError(message)
    if result is not None:
        result.record_pass("capabilities")


def check_schema_descriptor(
    descriptor: SchemaDescriptor,
    *,
    result: ConformanceResult | None = None,
) -> None:
    """Validate the public schema descriptor's bounded identity contract."""

    if descriptor.arrow_schema is None or not isinstance(descriptor.arrow_schema, pa.Schema):
        raise ValueError("schema descriptor must contain an Arrow schema")
    if result is not None:
        result.record_pass("schema_descriptor")


def check_record_batches(
    schema: pa.Schema,
    batches: Sequence[pa.RecordBatch],
    *,
    result: ConformanceResult | None = None,
) -> None:
    """Ensure execution cannot add columns or change the declared schema."""

    for index, batch in enumerate(batches):
        if batch.schema != schema:
            raise ValueError(f"batch {index} schema differs from the declared output schema")
        if set(batch.schema.names) != set(schema.names):
            raise ValueError(f"batch {index} contains undeclared output columns")
    if result is not None:
        result.record_pass("record_batches")


def run_format_checks(
    plugin: TableFormatPlugin,
    handle: TableHandle,
    schema: SchemaDescriptor,
    context: ExecutionContext,
    *,
    projection: Sequence[str] = (),
    row_filter: str | None = None,
    max_tasks: int = 64,
    required_capabilities: Iterable[str] = (),
    artifact_identity: str | None = None,
) -> ConformanceResult:
    """Run bounded plan/schema/output checks against one admitted format plugin."""

    descriptor = plugin.descriptor
    result = ConformanceResult(
        package=descriptor.distribution,
        plugin_id=descriptor.plugin_id,
        artifact_identity=artifact_identity,
        capability_matrix=dict.fromkeys(sorted(descriptor.capabilities), True),
    )
    try:
        if context.deadline <= datetime.now(timezone.utc):
            raise TimeoutError("execution context deadline has expired")
        if context.cancel_check is not None and context.cancel_check():
            raise RuntimeError("execution context was cancelled")
        check_capabilities(
            descriptor.capabilities,
            required_capabilities,
            result=result,
        )
        check_schema_descriptor(schema, result=result)
        if max_tasks <= 0:
            raise ValueError("max_tasks must be positive")
        planned = plugin.plan(
            handle,
            schema,
            context,
            projection=projection,
            row_filter=row_filter,
            max_tasks=max_tasks,
        )
        tasks = list(islice(planned, max_tasks + 1))
        if len(tasks) > max_tasks:
            raise ValueError("format returned more tasks than requested")
        result.record_pass("bounded_plan")
        for task in tasks:
            if context.cancel_check is not None and context.cancel_check():
                raise RuntimeError("execution context was cancelled")
            output_schema, batches = plugin.execute(task, context)
            if output_schema != schema.arrow_schema:
                raise ValueError("format output schema differs from the declared schema")
            check_record_batches(output_schema, list(batches), result=result)
        result.record_pass("execution")
    except Exception as exc:
        result.record_failure("format", str(exc))
    finally:
        close = getattr(plugin, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:
                result.record_failure("cleanup", str(exc))
        else:
            result.record_pass("cleanup")
    return result
