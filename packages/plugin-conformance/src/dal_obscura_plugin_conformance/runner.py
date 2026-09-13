"""Contract-level checks that external plugin distributions can invoke."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pyarrow as pa
from dal_obscura_plugin_api import (
    CatalogPlugin,
    DiscoveryPage,
    ExecutionContext,
    SchemaDescriptor,
    TableFormatPlugin,
    TableHandle,
    TableIdentifier,
)

DEFAULT_MAX_OUTPUT_BATCHES = 1_024
DEFAULT_MAX_OUTPUT_ROWS = 1_000_000
DEFAULT_MAX_DISCOVERY_PAGES = 64
DEFAULT_MAX_DISCOVERY_TABLES = 10_000
DEFAULT_MAX_SCHEMA_BYTES = 1_048_576
DEFAULT_MAX_SCHEMA_FIELDS = 4_096
DEFAULT_MAX_SCHEMA_DEPTH = 64
DEFAULT_MAX_BATCH_BYTES = 16 * 1024 * 1024


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

    def record_skip(self, name: str, reason: str) -> None:
        """Record an intentionally unrun check without making it pass."""

        self.checks[name] = "skipped"
        self.skips.append(f"{name}: {reason}")

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
    max_bytes: int = DEFAULT_MAX_SCHEMA_BYTES,
    max_fields: int = DEFAULT_MAX_SCHEMA_FIELDS,
    max_depth: int = DEFAULT_MAX_SCHEMA_DEPTH,
) -> None:
    """Validate the public schema descriptor's bounded identity contract."""

    if descriptor.arrow_schema is None or not isinstance(descriptor.arrow_schema, pa.Schema):
        raise ValueError("schema descriptor must contain an Arrow schema")
    if max_bytes <= 0 or max_fields <= 0 or max_depth <= 0:
        raise ValueError("schema descriptor budgets must be positive")
    nodes = 0
    pending = [(field, 1) for field in descriptor.arrow_schema]
    while pending:
        field, depth = pending.pop()
        nodes += 1
        if nodes > max_fields:
            raise ValueError(f"schema descriptor has more than {max_fields} fields")
        if depth > max_depth:
            raise ValueError(f"schema descriptor exceeds {max_depth} nesting levels")
        field_type = field.type
        if pa.types.is_struct(field_type):
            pending.extend((child, depth + 1) for child in field_type)
        elif pa.types.is_list(field_type) or pa.types.is_large_list(field_type):
            pending.append((field_type.value_field, depth + 1))
        elif pa.types.is_map(field_type):
            pending.extend(
                (
                    (field_type.key_field, depth + 1),
                    (field_type.item_field, depth + 1),
                )
            )
        elif pa.types.is_fixed_size_list(field_type):
            pending.append((field_type.value_field, depth + 1))
    if descriptor.arrow_schema.serialize().size > max_bytes:
        raise ValueError(f"schema descriptor exceeds {max_bytes} serialized bytes")
    if result is not None:
        result.record_pass("schema_descriptor")


def check_discovery_page(page: DiscoveryPage, *, result: ConformanceResult | None = None) -> None:
    """Validate one catalog page without trusting provider object shapes."""

    if not isinstance(page, DiscoveryPage):
        raise ValueError("catalog returned an invalid discovery page")
    for identifier in page.entries:
        if not isinstance(identifier, TableIdentifier):
            raise ValueError("catalog returned an invalid table identifier")
    if page.continuation is not None and (
        not isinstance(page.continuation, str) or not page.continuation
    ):
        raise ValueError("catalog continuation must be a non-empty string or null")
    if result is not None:
        result.record_pass("discovery_page")


def check_record_batches(  # noqa: C901
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    *,
    result: ConformanceResult | None = None,
    max_batches: int = DEFAULT_MAX_OUTPUT_BATCHES,
    max_rows: int = DEFAULT_MAX_OUTPUT_ROWS,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
    cancel_check: Callable[[], bool] | None = None,
    deadline: datetime | None = None,
) -> None:
    """Incrementally validate output without allowing unbounded materialization.

    ``cancel_check`` and ``deadline`` are sampled before every batch so a
    provider cannot continue producing output after the core has withdrawn the
    request.  The iterator is deliberately never collected into a table.
    """

    if max_batches <= 0 or max_rows <= 0 or max_batch_bytes <= 0:
        raise ValueError("output budgets must be positive")
    row_count = 0

    iterator = iter(batches)
    index = 0
    while True:
        if deadline is not None and datetime.now(timezone.utc) >= deadline:
            raise TimeoutError("execution context deadline expired while reading output")
        if cancel_check is not None and cancel_check():
            raise RuntimeError("execution context was cancelled while reading output")
        try:
            batch = next(iterator)
        except StopIteration:
            break
        if index >= max_batches:
            raise ValueError(f"format returned more than {max_batches} output batches")
        if not isinstance(batch, pa.RecordBatch):
            raise ValueError(f"batch {index} is not an Arrow record batch")
        if batch.schema != schema:
            raise ValueError(f"batch {index} schema differs from the declared output schema")
        if batch.nbytes > max_batch_bytes:
            raise ValueError(f"batch {index} exceeds the {max_batch_bytes}-byte batch byte budget")
        if set(batch.schema.names) != set(schema.names):
            raise ValueError(f"batch {index} contains undeclared output columns")
        row_count += batch.num_rows
        if row_count > max_rows:
            raise ValueError(f"format returned more than {max_rows} output rows")
        index += 1
    if result is not None:
        result.record_pass("record_batches")


def _check_task_coverage(
    tasks: Sequence[object],
    expected_task_ids: Iterable[str] | None,
    task_identity: Callable[[object], str] | None,
    result: ConformanceResult,
) -> None:
    if expected_task_ids is None:
        result.record_skip("task_coverage", "no expected task identities supplied")
        return
    if task_identity is None:
        raise ValueError("task_identity is required when expected_task_ids are supplied")
    expected = tuple(expected_task_ids)
    if len(expected) != len(set(expected)):
        raise ValueError("expected task identities must be unique")
    actual = tuple(task_identity(task) for task in tasks)
    if any(not isinstance(identity, str) or not identity for identity in actual):
        raise ValueError("task identities must be non-empty strings")
    if len(actual) != len(set(actual)):
        raise ValueError("format returned duplicate task identities")
    if set(actual) != set(expected):
        raise ValueError("format task identities do not match expected coverage")
    result.record_pass("task_coverage")


def run_catalog_checks(  # noqa: C901
    plugin: CatalogPlugin,
    context: ExecutionContext,
    *,
    page_size: int = 128,
    max_pages: int = DEFAULT_MAX_DISCOVERY_PAGES,
    max_tables: int = DEFAULT_MAX_DISCOVERY_TABLES,
    expected_table_ids: Iterable[str] | None = None,
    artifact_identity: str | None = None,
) -> ConformanceResult:
    """Run bounded discovery checks against one admitted catalog plugin."""

    descriptor = plugin.descriptor
    result = ConformanceResult(
        package=descriptor.distribution,
        plugin_id=descriptor.plugin_id,
        artifact_identity=artifact_identity,
        capability_matrix=dict.fromkeys(sorted(descriptor.capabilities), True),
    )
    try:
        if descriptor.kind != "catalog":
            raise ValueError("catalog conformance requires a catalog plugin descriptor")
        if page_size <= 0 or max_pages <= 0 or max_tables <= 0:
            raise ValueError("catalog discovery budgets must be positive")
        continuation: str | None = None
        seen_continuations: set[str] = set()
        identifiers: list[TableIdentifier] = []
        for _page_index in range(max_pages):
            if datetime.now(timezone.utc) >= context.deadline:
                raise TimeoutError("execution context deadline expired while discovering")
            if context.cancel_check is not None and context.cancel_check():
                raise RuntimeError("execution context was cancelled while discovering")
            page = plugin.list_tables(context, continuation=continuation, limit=page_size)
            check_discovery_page(page)
            if len(page.entries) > page_size:
                raise ValueError("catalog returned more entries than requested")
            identifiers.extend(page.entries)
            if len(identifiers) > max_tables:
                raise ValueError(f"catalog returned more than {max_tables} tables")
            if page.continuation is None:
                break
            if page.continuation in seen_continuations:
                raise ValueError("catalog returned a repeated continuation token")
            seen_continuations.add(page.continuation)
            continuation = page.continuation
        else:
            raise ValueError(f"catalog returned more than {max_pages} discovery pages")
        identities = [(*entry.namespace, entry.name) for entry in identifiers]
        if len(identities) != len(set(identities)):
            raise ValueError("catalog returned duplicate table identities")
        result.record_pass("bounded_discovery")
        if expected_table_ids is None:
            result.record_skip("discovery_coverage", "no expected table identities supplied")
        elif {".".join(identity) for identity in identities} != set(expected_table_ids):
            raise ValueError("catalog table identities do not match expected coverage")
        else:
            result.record_pass("discovery_coverage")
    except Exception as exc:
        result.record_failure("catalog", str(exc))
    finally:
        close = getattr(plugin, "close", None)
        if callable(close):
            try:
                close()
                result.record_pass("cleanup")
            except Exception as exc:
                result.record_failure("cleanup", str(exc))
        else:
            result.record_pass("cleanup")
    return result


def run_format_checks(  # noqa: C901
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
    expected_task_ids: Iterable[str] | None = None,
    task_identity: Callable[[object], str] | None = None,
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
        tasks: list[object] = []
        iterator = iter(planned)
        while len(tasks) <= max_tasks:
            if datetime.now(timezone.utc) >= context.deadline:
                raise TimeoutError("execution context deadline expired while planning")
            if context.cancel_check is not None and context.cancel_check():
                raise RuntimeError("execution context was cancelled while planning")
            try:
                tasks.append(next(iterator))
            except StopIteration:
                break
        if len(tasks) > max_tasks:
            raise ValueError("format returned more tasks than requested")
        result.record_pass("bounded_plan")
        _check_task_coverage(tasks, expected_task_ids, task_identity, result)
        for task in tasks:
            if context.cancel_check is not None and context.cancel_check():
                raise RuntimeError("execution context was cancelled")
            output_schema, batches = plugin.execute(task, context)
            if output_schema != schema.arrow_schema:
                raise ValueError("format output schema differs from the declared schema")
            check_record_batches(
                output_schema,
                batches,
                result=result,
                cancel_check=context.cancel_check,
                deadline=context.deadline,
            )
        result.record_pass("execution")
    except Exception as exc:
        result.record_failure("format", str(exc))
    finally:
        close = getattr(plugin, "close", None)
        if callable(close):
            try:
                close()
                result.record_pass("cleanup")
            except Exception as exc:
                result.record_failure("cleanup", str(exc))
        else:
            result.record_pass("cleanup")
    return result
