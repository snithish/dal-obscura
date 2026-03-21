from __future__ import annotations

import json
import pickle
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import ALWAYS_TRUE

from dal_obscura.domain.catalog.ports import ResolvedTable
from dal_obscura.domain.format_handler.ports import FormatHandler, Plan, ScanTask
from dal_obscura.domain.query_planning.models import DatasetSelector, PlanRequest


@dataclass(frozen=True)
class IcebergReadSpec:
    """Serialized Iceberg scan plan stored inside a signed ticket."""

    dataset: DatasetSelector
    catalog_name: str
    catalog_options: dict
    table_identifier: str
    columns: list[str]
    tasks: list[bytes]


class IcebergHandler(FormatHandler):
    """Format handler that plans and reads Iceberg tables via PyIceberg."""

    def __init__(self) -> None:
        self._catalog_cache: dict[str, Catalog] = {}

    @property
    def supported_format(self) -> str:
        return "iceberg"

    def get_schema(self, table: ResolvedTable) -> Any:
        """Loads the Iceberg table schema used by planning and validation."""
        catalog_name, catalog_options, table_identifier = _iceberg_config(table)
        pyiceberg_table = self._load_table(catalog_name, catalog_options, table_identifier)
        return pyiceberg_table.schema().as_arrow()

    def plan(self, table: ResolvedTable, request: PlanRequest, max_tickets: int) -> Plan:
        """Plans file tasks and distributes them across the available tickets."""
        catalog_name, catalog_options, table_identifier = _iceberg_config(table)
        pyiceberg_table = self._load_table(catalog_name, catalog_options, table_identifier)

        column_tuple = tuple(request.columns)
        base_schema = pyiceberg_table.schema().as_arrow()

        try:
            scan = pyiceberg_table.scan(selected_fields=column_tuple)
        except TypeError:
            scan = pyiceberg_table.scan().select(*column_tuple)

        file_tasks = list(scan.plan_files())
        groups = _chunk_by_max_tickets(file_tasks, max_tickets)
        if not groups:
            groups = [[]]

        dataset = DatasetSelector(catalog=request.catalog, target=request.target)

        tasks = [
            ScanTask(
                format=self.supported_format,
                schema=base_schema,
                payload=pickle.dumps(
                    IcebergReadSpec(
                        dataset=dataset,
                        catalog_name=catalog_name,
                        catalog_options=catalog_options,
                        table_identifier=table_identifier,
                        columns=list(column_tuple),
                        tasks=[pickle.dumps(task) for task in group],
                    )
                ),
            )
            for group in groups
        ]
        return Plan(schema=base_schema, tasks=tasks)

    def execute(self, payload: bytes) -> tuple[pa.Schema, Iterable[Any]]:
        """Executes the pre-planned Iceberg file tasks stored in the ticket."""
        spec = _decode_spec(payload)
        table = self._load_table(
            spec.catalog_name,
            spec.catalog_options,
            spec.table_identifier,
        )
        column_tuple = tuple(spec.columns)
        file_tasks = [pickle.loads(task) for task in spec.tasks]

        projected_schema = table.schema()
        if column_tuple:
            projected_schema = projected_schema.select(*column_tuple)

        arrow_schema = projected_schema.as_arrow()

        if not file_tasks:
            return arrow_schema, iter(())

        arrow_scan = ArrowScan(
            table_metadata=table.metadata,
            io=table.io,
            projected_schema=projected_schema,
            row_filter=ALWAYS_TRUE,
        )
        return arrow_schema, arrow_scan.to_record_batches(file_tasks)

    def _load_table(
        self,
        catalog_name: str,
        catalog_options: dict,
        table_identifier: str,
    ):
        """Loads the table and enforces the supported Iceberg format versions."""
        catalog = self._load_catalog(catalog_name, catalog_options)
        table = catalog.load_table(table_identifier)
        format_version = int(getattr(table.metadata, "format_version", 1))
        if format_version not in {2, 3}:
            raise ValueError(f"Unsupported Iceberg format version: {format_version}")
        return table

    def _load_catalog(self, catalog_name: str, catalog_options: dict) -> Catalog:
        """Caches catalog instances because they are reused across many requests."""
        cache_key = json.dumps(
            {
                "catalog_name": catalog_name,
                "catalog_options": catalog_options,
            },
            sort_keys=True,
        )
        cached = self._catalog_cache.get(cache_key)
        if cached is not None:
            return cached
        loaded = load_catalog(catalog_name, **catalog_options)
        self._catalog_cache[cache_key] = loaded
        return loaded


def _decode_spec(payload: bytes) -> IcebergReadSpec:
    """Deserializes and validates the Iceberg read specification."""
    spec = pickle.loads(payload)
    if not isinstance(spec, IcebergReadSpec):
        raise ValueError("Invalid read payload for Iceberg handler")
    return spec


def _iceberg_config(table: ResolvedTable) -> tuple[str, dict, str]:
    """Normalizes the table object into catalog load arguments."""
    if not isinstance(table.table_object, dict):
        raise ValueError("Iceberg format handler requires table_object to be a dict")

    config = table.table_object
    catalog_name = str(config.get("catalog_name", "")).strip()
    catalog_options = _catalog_options(config.get("catalog_options", {}))
    table_identifier = str(config.get("table_identifier", "")).strip()

    if not catalog_name or not table_identifier:
        raise ValueError(
            "Iceberg handler requires catalog_name and table_identifier in table_object"
        )
    return catalog_name, catalog_options, table_identifier


def _catalog_options(value: object) -> dict:
    """Validates catalog options before forwarding them to PyIceberg."""
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    raise ValueError("Iceberg handler requires catalog_options to be a mapping")


def _chunk_by_max_tickets(tasks: list, max_tickets: int) -> list[list]:
    """Balances planned Iceberg file tasks across the available tickets."""
    if not tasks:
        return []
    max_tickets = max(1, max_tickets)
    group_count = min(max_tickets, len(tasks))
    groups: list[list] = [[] for _ in range(group_count)]
    for index, task in enumerate(tasks):
        groups[index % group_count].append(task)
    return groups
