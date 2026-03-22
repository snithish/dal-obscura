import csv
import json
import threading
import time
from contextlib import suppress
from pathlib import Path

import jwt
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq
import pytest
import yaml

from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.infrastructure.adapters.catalog_registry import DynamicCatalogRegistry
from dal_obscura.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.infrastructure.adapters.format_registry import DynamicFormatRegistry
from dal_obscura.infrastructure.adapters.identity_default import (
    AuthConfig,
    DefaultIdentityAdapter,
)
from dal_obscura.infrastructure.adapters.policy_file_authorizer import PolicyFileAuthorizer
from dal_obscura.infrastructure.adapters.service_config import load_service_config
from dal_obscura.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter
from dal_obscura.interfaces.flight.server import DataAccessFlightService

JWT_SECRET = "test-jwt-secret-32-characters-long"


def _start_server(server: DataAccessFlightService) -> threading.Thread:
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        if server.port > 0:
            return thread
        time.sleep(0.05)
    raise RuntimeError("Flight server failed to start")


def _build_server(
    catalog_registry: DynamicCatalogRegistry,
    format_registry: DynamicFormatRegistry,
    policy_path: Path,
    max_tickets: int = 4,
):
    identity = DefaultIdentityAdapter(AuthConfig(jwt_secret=JWT_SECRET))
    authorizer = PolicyFileAuthorizer(policy_path)
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter("secret")
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=catalog_registry,
        format_registry=format_registry,
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=max_tickets,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
        format_registry=format_registry,
        masking=masking,
        row_transform=row_transform,
        ticket_codec=ticket_codec,
    )
    return DataAccessFlightService(
        location="grpc+tcp://0.0.0.0:0",
        plan_access_use_case=plan_access,
        fetch_stream_use_case=fetch_stream,
    )


def _flight_options(principal_id: str = "user1") -> flight.FlightCallOptions:
    token = jwt.encode({"sub": principal_id}, JWT_SECRET, algorithm="HS256")
    return flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {token}".encode())])


def _flight_info(client: flight.FlightClient, payload: dict[str, object]):
    descriptor = flight.FlightDescriptor.for_command(json.dumps(payload).encode("utf-8"))
    options = _flight_options()
    info = client.get_flight_info(descriptor, options=options)
    return info, options


def _read_table(client: flight.FlightClient, info: flight.FlightInfo, options):
    batches = []
    for endpoint in info.endpoints:
        reader = client.do_get(endpoint.ticket, options=options)
        batches.extend(reader.read_all().to_batches())
    return pa.Table.from_batches(batches) if batches else pa.table({})


def _flight_request(client: flight.FlightClient, payload: dict[str, object]):
    info, options = _flight_info(client, payload)
    return info, _read_table(client, info, options)


def _build_registries(service_config_path: Path):
    service_config = load_service_config(service_config_path)
    return DynamicCatalogRegistry(service_config), DynamicFormatRegistry()


def _write_csv_files(base_dir: Path, file_count: int, rows_per_file: int) -> str:
    base_dir.mkdir(parents=True, exist_ok=True)
    for file_index in range(file_count):
        path = base_dir / f"part-{file_index}.csv"
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["id", "email", "region"])
            writer.writeheader()
            start = file_index * rows_per_file
            for row_index in range(rows_per_file):
                current = start + row_index + 1
                writer.writerow(
                    {
                        "id": current,
                        "email": f"user{current}@example.com",
                        "region": "us" if current % 2 == 0 else "eu",
                    }
                )
    return str(base_dir / "*.csv")


def _write_ndjson_files(base_dir: Path, file_count: int, rows_per_file: int) -> str:
    base_dir.mkdir(parents=True, exist_ok=True)
    for file_index in range(file_count):
        path = base_dir / f"part-{file_index}.json"
        start = file_index * rows_per_file
        path.write_text(
            "\n".join(
                json.dumps(
                    {
                        "id": start + row_index + 1,
                        "email": f"user{start + row_index + 1}@example.com",
                        "region": "us" if (start + row_index + 1) % 2 == 0 else "eu",
                    }
                )
                for row_index in range(rows_per_file)
            )
            + "\n"
        )
    return str(base_dir / "*.json")


def _write_parquet_files(base_dir: Path, file_count: int, rows_per_file: int) -> str:
    base_dir.mkdir(parents=True, exist_ok=True)
    for file_index in range(file_count):
        start = file_index * rows_per_file
        ids = list(range(start + 1, start + rows_per_file + 1))
        table = pa.table(
            {
                "id": ids,
                "email": [f"user{i}@example.com" for i in ids],
                "region": ["us" if i % 2 == 0 else "eu" for i in ids],
            }
        )
        pq.write_table(table, base_dir / f"part-{file_index}.parquet")
    return str(base_dir / "*.parquet")


def _create_iceberg_table(
    tmp_path: Path, catalog_name: str, warehouse_name: str, values: list[int]
) -> str:
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    warehouse = tmp_path / warehouse_name
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=f"sqlite:///{tmp_path / f'{catalog_name}.db'}",
        warehouse=str(warehouse),
    )
    identifier = "default.users"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="email", field_type=StringType(), required=False),
        NestedField(field_id=3, name="region", field_type=StringType(), required=False),
    )
    with suppress(Exception):
        catalog.create_namespace("default")
    table = catalog.create_table(
        identifier=identifier,
        schema=schema,
        properties={"format-version": "2"},
    )
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("email", pa.string(), nullable=True),
            pa.field("region", pa.string(), nullable=True),
        ]
    )
    table.append(
        pa.table(
            {
                "id": values,
                "email": [f"user{i}@example.com" for i in values],
                "region": ["us" if i % 2 == 0 else "eu" for i in values],
            },
            schema=arrow_schema,
        )
    )
    return identifier


def _write_service_and_policy(
    tmp_path: Path,
    *,
    csv_glob: str,
    json_glob: str,
    parquet_glob: str,
    raw_glob: str,
) -> tuple[Path, Path]:
    service_config = {
        "catalogs": {
            "ice_one": {
                "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                "options": {
                    "type": "sql",
                    "uri": f"sqlite:///{tmp_path / 'ice_one.db'}",
                    "warehouse": str(tmp_path / "warehouse-one"),
                },
                "targets": {
                    "users_csv": {
                        "backend": "duckdb_file",
                        "format": "csv",
                        "paths": [csv_glob],
                        "options": {"sample_rows": 2000, "sample_files": 2},
                    }
                },
            },
            "ice_two": {
                "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                "options": {
                    "type": "sql",
                    "uri": f"sqlite:///{tmp_path / 'ice_two.db'}",
                    "warehouse": str(tmp_path / "warehouse-two"),
                },
            },
            "local_files": {
                "module": "dal_obscura.infrastructure.adapters.catalog_registry.StaticCatalog",
                "targets": {
                    "users_csv": {
                        "backend": "duckdb_file",
                        "format": "csv",
                        "paths": [csv_glob],
                        "options": {"sample_rows": 2000, "sample_files": 2},
                    },
                    "events_json": {
                        "backend": "duckdb_file",
                        "format": "json",
                        "paths": [json_glob],
                        "options": {"sample_rows": 2000, "sample_files": 2},
                    },
                    "facts_parquet": {
                        "backend": "duckdb_file",
                        "format": "parquet",
                        "paths": [parquet_glob],
                    },
                },
            },
        },
        "paths": [
            {"glob": raw_glob, "options": {"sample_rows": 1000, "sample_files": 1}},
        ],
    }
    policy = {
        "version": 1,
        "catalogs": {
            "ice_one": {
                "targets": {
                    "default.users": {
                        "rules": [
                            {
                                "principals": ["user1"],
                                "columns": ["id", "email", "region"],
                                "masks": {"id": {"type": "hash"}},
                                "row_filter": "region = 'us'",
                            }
                        ]
                    },
                    "users_csv": {
                        "rules": [
                            {
                                "principals": ["user1"],
                                "columns": ["id", "email", "region"],
                                "masks": {"id": {"type": "hash"}},
                                "row_filter": "region = 'us'",
                            }
                        ]
                    },
                }
            },
            "ice_two": {
                "targets": {
                    "default.users": {
                        "rules": [
                            {
                                "principals": ["user1"],
                                "columns": ["id", "email", "region"],
                            }
                        ]
                    }
                }
            },
            "local_files": {
                "targets": {
                    "users_csv": {
                        "rules": [
                            {
                                "principals": ["user1"],
                                "columns": ["id", "email", "region"],
                                "masks": {"id": {"type": "hash"}},
                                "row_filter": "region = 'us'",
                            }
                        ]
                    },
                    "events_json": {
                        "rules": [
                            {
                                "principals": ["user1"],
                                "columns": ["id", "email", "region"],
                                "masks": {"id": {"type": "hash"}},
                                "row_filter": "region = 'us'",
                            }
                        ]
                    },
                    "facts_parquet": {
                        "rules": [
                            {
                                "principals": ["user1"],
                                "columns": ["id", "email", "region"],
                                "masks": {"id": {"type": "hash"}},
                                "row_filter": "region = 'us'",
                            }
                        ]
                    },
                }
            },
        },
        "paths": [
            {
                "target": raw_glob,
                "rules": [
                    {
                        "principals": ["user1"],
                        "columns": ["id", "email", "region"],
                        "masks": {"id": {"type": "hash"}},
                        "row_filter": "region = 'us'",
                    }
                ],
            }
        ],
    }

    service_config_path = tmp_path / "service.yaml"
    policy_path = tmp_path / "policy.yaml"
    service_config_path.write_text(yaml.safe_dump(service_config, sort_keys=False))
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False))
    return service_config_path, policy_path


def test_flight_plan_and_get_with_iceberg_multi_catalog(tmp_path):
    table_id_one = _create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    table_id_two = _create_iceberg_table(tmp_path, "ice_two", "warehouse-two", [10, 11, 12, 13])
    csv_glob = _write_csv_files(tmp_path / "csv", file_count=1, rows_per_file=4)
    json_glob = _write_ndjson_files(tmp_path / "json", file_count=1, rows_per_file=4)
    parquet_glob = _write_parquet_files(tmp_path / "parquet", file_count=1, rows_per_file=4)
    raw_glob = _write_csv_files(tmp_path / "raw", file_count=1, rows_per_file=4)
    service_config_path, policy_path = _write_service_and_policy(
        tmp_path,
        csv_glob=csv_glob,
        json_glob=json_glob,
        parquet_glob=parquet_glob,
        raw_glob=raw_glob,
    )

    catalog_registry, format_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, format_registry, policy_path, max_tickets=4)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")

    info_one, table_one = _flight_request(
        client,
        {"catalog": "ice_one", "target": table_id_one, "columns": ["id", "email", "region"]},
    )
    _info_two, table_two = _flight_request(
        client,
        {"catalog": "ice_two", "target": table_id_two, "columns": ["id", "email", "region"]},
    )

    assert info_one.schema.field("id").type == pa.string()
    assert table_one.num_rows == 2
    assert set(table_one.column("region").to_pylist()) == {"us"}
    assert set(table_two.column("region").to_pylist()) == {"eu", "us"}
    assert table_two.num_rows == 4

    server.shutdown()
    thread.join(timeout=2)


def test_flight_plan_and_get_with_mixed_catalog_formats(tmp_path):
    table_id_one = _create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    csv_glob = _write_csv_files(tmp_path / "csv", file_count=2, rows_per_file=10)
    json_glob = _write_ndjson_files(tmp_path / "json", file_count=1, rows_per_file=4)
    parquet_glob = _write_parquet_files(tmp_path / "parquet", file_count=1, rows_per_file=4)
    raw_glob = _write_csv_files(tmp_path / "raw", file_count=1, rows_per_file=4)
    service_config_path, policy_path = _write_service_and_policy(
        tmp_path,
        csv_glob=csv_glob,
        json_glob=json_glob,
        parquet_glob=parquet_glob,
        raw_glob=raw_glob,
    )

    catalog_registry, format_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, format_registry, policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")

    iceberg_info, iceberg_table = _flight_request(
        client,
        {"catalog": "ice_one", "target": table_id_one, "columns": ["id", "email", "region"]},
    )
    file_info, file_table = _flight_request(
        client,
        {"catalog": "ice_one", "target": "users_csv", "columns": ["id", "email", "region"]},
    )

    assert iceberg_info.schema.field("id").type == pa.string()
    assert iceberg_table.num_rows == 2
    assert file_info.schema.field("id").type == pa.string()
    assert file_table.num_rows == 10
    assert set(file_table.column("region").to_pylist()) == {"us"}

    server.shutdown()
    thread.join(timeout=2)


def test_flight_plan_and_get_with_json_catalog(tmp_path):
    csv_glob = _write_csv_files(tmp_path / "csv", file_count=1, rows_per_file=4)
    json_glob = _write_ndjson_files(tmp_path / "json", file_count=2, rows_per_file=10)
    parquet_glob = _write_parquet_files(tmp_path / "parquet", file_count=1, rows_per_file=4)
    raw_glob = _write_csv_files(tmp_path / "raw", file_count=1, rows_per_file=6)
    service_config_path, policy_path = _write_service_and_policy(
        tmp_path,
        csv_glob=csv_glob,
        json_glob=json_glob,
        parquet_glob=parquet_glob,
        raw_glob=raw_glob,
    )

    catalog_registry, format_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, format_registry, policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    info, table = _flight_request(
        client,
        {"catalog": "local_files", "target": "events_json", "columns": ["id", "email", "region"]},
    )

    assert info.schema.field("id").type == pa.string()
    assert table.num_rows == 10
    assert set(table.column("region").to_pylist()) == {"us"}

    server.shutdown()
    thread.join(timeout=2)


def test_flight_plan_and_get_with_raw_path(tmp_path):
    csv_glob = _write_csv_files(tmp_path / "csv", file_count=1, rows_per_file=4)
    json_glob = _write_ndjson_files(tmp_path / "json", file_count=1, rows_per_file=4)
    parquet_glob = _write_parquet_files(tmp_path / "parquet", file_count=1, rows_per_file=4)
    raw_glob = _write_csv_files(tmp_path / "raw", file_count=2, rows_per_file=8)
    service_config_path, policy_path = _write_service_and_policy(
        tmp_path,
        csv_glob=csv_glob,
        json_glob=json_glob,
        parquet_glob=parquet_glob,
        raw_glob=raw_glob,
    )

    catalog_registry, format_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, format_registry, policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    info, table = _flight_request(
        client,
        {"target": raw_glob, "columns": ["id", "email", "region"]},
    )

    assert info.schema.field("id").type == pa.string()
    assert table.num_rows == 8
    assert set(table.column("region").to_pylist()) == {"us"}

    server.shutdown()
    thread.join(timeout=2)


def test_flight_plan_and_get_with_parquet_multi_file_large(tmp_path):
    csv_glob = _write_csv_files(tmp_path / "csv", file_count=1, rows_per_file=4)
    json_glob = _write_ndjson_files(tmp_path / "json", file_count=1, rows_per_file=4)
    parquet_glob = _write_parquet_files(tmp_path / "parquet", file_count=2, rows_per_file=100_000)
    raw_glob = _write_csv_files(tmp_path / "raw", file_count=1, rows_per_file=6)
    service_config_path, policy_path = _write_service_and_policy(
        tmp_path,
        csv_glob=csv_glob,
        json_glob=json_glob,
        parquet_glob=parquet_glob,
        raw_glob=raw_glob,
    )

    catalog_registry, format_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, format_registry, policy_path, max_tickets=4)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    info, table = _flight_request(
        client,
        {"catalog": "local_files", "target": "facts_parquet", "columns": ["id", "email", "region"]},
    )

    assert len(info.endpoints) == 2
    assert info.schema.field("id").type == pa.string()
    assert table.num_rows == 100_000
    assert set(table.column("region").to_pylist()) == {"us"}

    server.shutdown()
    thread.join(timeout=2)


def test_hot_reload_does_not_break_format_registry(tmp_path):
    csv_glob_v1 = _write_csv_files(tmp_path / "csv-v1", file_count=1, rows_per_file=4)
    csv_glob_v2 = _write_csv_files(tmp_path / "csv-v2", file_count=1, rows_per_file=6)
    json_glob = _write_ndjson_files(tmp_path / "json", file_count=1, rows_per_file=4)
    parquet_glob = _write_parquet_files(tmp_path / "parquet", file_count=1, rows_per_file=4)
    raw_glob = _write_csv_files(tmp_path / "raw", file_count=1, rows_per_file=4)

    service_config_path, policy_path = _write_service_and_policy(
        tmp_path,
        csv_glob=csv_glob_v1,
        json_glob=json_glob,
        parquet_glob=parquet_glob,
        raw_glob=raw_glob,
    )
    catalog_registry, format_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, format_registry, policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")

    old_info, options = _flight_info(
        client,
        {"catalog": "local_files", "target": "users_csv", "columns": ["id", "email", "region"]},
    )
    old_table = _read_table(client, old_info, options)
    old_ticket = old_info.endpoints[0].ticket

    service_config = load_service_config(service_config_path)
    updated_catalogs = dict(service_config.catalogs)
    updated_target = updated_catalogs["local_files"].targets["users_csv"]
    updated_catalogs["local_files"] = type(updated_catalogs["local_files"])(
        name=updated_catalogs["local_files"].name,
        module=updated_catalogs["local_files"].module,
        options=updated_catalogs["local_files"].options,
        targets={
            **updated_catalogs["local_files"].targets,
            "users_csv": type(updated_target)(
                backend=updated_target.backend,
                table=updated_target.table,
                format=updated_target.format,
                paths=(csv_glob_v2,),
                options=updated_target.options,
            ),
        },
    )
    catalog_registry.reload(
        type(service_config)(catalogs=updated_catalogs, paths=service_config.paths)
    )

    new_info, new_options = _flight_info(
        client,
        {"catalog": "local_files", "target": "users_csv", "columns": ["id", "email", "region"]},
    )
    new_table = _read_table(client, new_info, new_options)

    assert old_table.num_rows == 2
    assert new_table.num_rows == 3

    client.do_get(old_ticket, options=options).read_all()

    server.shutdown()
    thread.join(timeout=2)
