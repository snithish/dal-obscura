from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from typing import Iterable, Mapping

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import ALWAYS_TRUE

from dal_obscura.domain.query_planning import (
    DatasetSelector,
    Plan,
    ReadPayload,
    ReadSpec,
    ResolvedBackendTarget,
)


@dataclass(frozen=True)
class IcebergReadSpec:
    dataset: DatasetSelector
    catalog_name: str
    catalog_type: str
    catalog_options: dict
    table_identifier: str
    columns: list[str]
    tasks: list[bytes]
    schema: object


class IcebergBackend:
    def __init__(self) -> None:
        self._catalog_cache: dict[str, Catalog] = {}

    def get_schema(self, target: ResolvedBackendTarget):
        catalog_name, catalog_type, catalog_options, table_identifier = _iceberg_config(
            target.handle
        )
        table = self._load_table(catalog_name, catalog_type, catalog_options, table_identifier)
        return table.schema().as_arrow()

    def plan(self, target: ResolvedBackendTarget, columns: Iterable[str], max_tickets: int) -> Plan:
        catalog_name, catalog_type, catalog_options, table_identifier = _iceberg_config(
            target.handle
        )
        table = self._load_table(catalog_name, catalog_type, catalog_options, table_identifier)
        column_tuple = tuple(columns)
        base_schema = table.schema().as_arrow()
        try:
            scan = table.scan(selected_fields=column_tuple)
        except TypeError:
            scan = table.scan().select(*column_tuple)

        file_tasks = list(scan.plan_files())
        groups = _chunk_by_max_tickets(file_tasks, max_tickets)
        if not groups:
            groups = [[]]

        tasks = [
            ReadPayload(
                payload=pickle.dumps(
                    IcebergReadSpec(
                        dataset=target.dataset_identity,
                        catalog_name=catalog_name,
                        catalog_type=catalog_type,
                        catalog_options=catalog_options,
                        table_identifier=table_identifier,
                        columns=list(column_tuple),
                        tasks=[pickle.dumps(task) for task in group],
                        schema=base_schema,
                    )
                )
            )
            for group in groups
        ]
        return Plan(schema=base_schema, tasks=tasks)

    def read_spec(self, read_payload: bytes) -> ReadSpec:
        spec = _decode_spec(read_payload)
        return ReadSpec(dataset=spec.dataset, columns=list(spec.columns), schema=spec.schema)

    def read_stream(self, read_payload: bytes):
        spec = _decode_spec(read_payload)
        table = self._load_table(
            spec.catalog_name,
            spec.catalog_type,
            spec.catalog_options,
            spec.table_identifier,
        )
        column_tuple = tuple(spec.columns)
        file_tasks = [pickle.loads(task) for task in spec.tasks]

        projected_schema = table.schema()
        if column_tuple:
            projected_schema = projected_schema.select(*column_tuple)
        if not file_tasks:
            return iter(())

        arrow_scan = ArrowScan(
            table_metadata=table.metadata,
            io=table.io,
            projected_schema=projected_schema,
            row_filter=ALWAYS_TRUE,
        )
        return arrow_scan.to_record_batches(file_tasks)

    def _load_table(
        self,
        catalog_name: str,
        catalog_type: str,
        catalog_options: dict,
        table_identifier: str,
    ):
        catalog = self._load_catalog(catalog_name, catalog_type, catalog_options)
        table = catalog.load_table(table_identifier)
        format_version = int(getattr(table.metadata, "format_version", 1))
        if format_version not in {2, 3}:
            raise ValueError(f"Unsupported Iceberg format version: {format_version}")
        return table

    def _load_catalog(self, catalog_name: str, catalog_type: str, catalog_options: dict) -> Catalog:
        cache_key = json.dumps(
            {
                "catalog_name": catalog_name,
                "catalog_type": catalog_type,
                "catalog_options": catalog_options,
            },
            sort_keys=True,
        )
        cached = self._catalog_cache.get(cache_key)
        if cached is not None:
            return cached
        loaded = load_catalog(catalog_name, type=catalog_type, **catalog_options)
        self._catalog_cache[cache_key] = loaded
        return loaded


def _decode_spec(read_payload: bytes) -> IcebergReadSpec:
    spec = pickle.loads(read_payload)
    if not isinstance(spec, IcebergReadSpec):
        raise ValueError("Invalid read payload for Iceberg backend")
    return spec


def _iceberg_config(handle: dict[str, object]) -> tuple[str, str, dict, str]:
    catalog_name = str(handle.get("catalog_name", "")).strip()
    catalog_type = str(handle.get("catalog_type", "")).strip()
    catalog_options = _catalog_options(handle.get("catalog_options"))
    table_identifier = str(handle.get("table_identifier", "")).strip()
    if not catalog_name or not catalog_type or not table_identifier:
        raise ValueError(
            "Iceberg backend requires catalog_name, catalog_type, and table_identifier"
        )
    return catalog_name, catalog_type, catalog_options, table_identifier


def _catalog_options(value: object) -> dict:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    raise ValueError("Iceberg backend requires catalog_options to be a mapping")


def _chunk_by_max_tickets(tasks: list, max_tickets: int) -> list[list]:
    if not tasks:
        return []
    max_tickets = max(1, max_tickets)
    group_count = min(max_tickets, len(tasks))
    groups: list[list] = [[] for _ in range(group_count)]
    for index, task in enumerate(tasks):
        groups[index % group_count].append(task)
    return groups
