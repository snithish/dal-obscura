import json
import threading
import time
from contextlib import suppress
from pathlib import Path

import jwt
import pyarrow as pa
import pyarrow.flight as flight
import pytest
import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.infrastructure.adapters.catalog_registry import DynamicCatalogRegistry
from dal_obscura.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.infrastructure.adapters.identity_default import (
    AuthConfig,
    DefaultIdentityAdapter,
)
from dal_obscura.infrastructure.adapters.policy_file_authorizer import PolicyFileAuthorizer
from dal_obscura.infrastructure.adapters.service_config import load_catalog_config
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
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=max_tickets,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
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
    service_config = load_catalog_config(service_config_path)
    return DynamicCatalogRegistry(service_config)


def _create_iceberg_table(
    tmp_path: Path,
    catalog_name: str,
    warehouse_name: str,
    values: list[int] | None = None,
    *,
    identifier: str = "default.users",
    append_batches: list[list[int]] | None = None,
) -> str:
    warehouse = tmp_path / warehouse_name
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=f"sqlite:///{tmp_path / f'{catalog_name}.db'}",
        warehouse=str(warehouse),
    )
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="email", field_type=StringType(), required=False),
        NestedField(field_id=3, name="region", field_type=StringType(), required=False),
    )
    namespace = ".".join(identifier.split(".")[:-1])
    with suppress(Exception):
        catalog.create_namespace(namespace)
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
    batches = append_batches or [values or []]
    for batch_values in batches:
        table.append(
            pa.table(
                {
                    "id": batch_values,
                    "email": [f"user{i}@example.com" for i in batch_values],
                    "region": ["us" if i % 2 == 0 else "eu" for i in batch_values],
                },
                schema=arrow_schema,
            )
        )
    return identifier


def _write_yaml_files(
    tmp_path: Path,
    *,
    service_config: dict[str, object],
    policy: dict[str, object],
) -> tuple[Path, Path]:
    service_config_path = tmp_path / "service.yaml"
    policy_path = tmp_path / "policy.yaml"
    service_config_path.write_text(yaml.safe_dump(service_config, sort_keys=False))
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False))
    return service_config_path, policy_path


def test_flight_plan_and_get_with_iceberg_multi_catalog(tmp_path):
    table_id_one = _create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    table_id_two = _create_iceberg_table(tmp_path, "ice_two", "warehouse-two", [10, 11, 12, 13])
    service_config_path, policy_path = _write_yaml_files(
        tmp_path,
        service_config={
            "catalogs": {
                "ice_one": {
                    "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'ice_one.db'}",
                        "warehouse": str(tmp_path / "warehouse-one"),
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
            },
        },
        policy={
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
            },
        },
    )

    catalog_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, policy_path, max_tickets=4)
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


def test_flight_plan_and_get_with_static_catalog_alias(tmp_path):
    table_id = _create_iceberg_table(
        tmp_path,
        "shared",
        "shared-warehouse",
        [1, 2, 3, 4],
        identifier="analytics.users",
    )
    service_config_path, policy_path = _write_yaml_files(
        tmp_path,
        service_config={
            "catalogs": {
                "shared": {
                    "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'shared.db'}",
                        "warehouse": str(tmp_path / "shared-warehouse"),
                    },
                    "targets": {
                        "users_alias": {
                            "table": table_id,
                        }
                    },
                },
            }
        },
        policy={
            "version": 1,
            "catalogs": {
                "shared": {
                    "targets": {
                        table_id: {
                            "rules": [
                                {
                                    "principals": ["user1"],
                                    "columns": ["id", "email", "region"],
                                }
                            ]
                        },
                        "users_alias": {
                            "rules": [
                                {
                                    "principals": ["user1"],
                                    "columns": ["id", "email", "region"],
                                    "masks": {"id": {"type": "hash"}},
                                    "row_filter": "region = 'us'",
                                }
                            ]
                        },
                    },
                },
            },
        },
    )

    catalog_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")

    direct_info, direct_table = _flight_request(
        client,
        {"catalog": "shared", "target": table_id, "columns": ["id", "email", "region"]},
    )
    alias_info, alias_table = _flight_request(
        client,
        {"catalog": "shared", "target": "users_alias", "columns": ["id", "email", "region"]},
    )

    assert direct_info.schema.field("id").type == pa.int64()
    assert direct_table.num_rows == 4
    assert alias_info.schema.field("id").type == pa.string()
    assert alias_table.num_rows == 2
    assert set(alias_table.column("region").to_pylist()) == {"us"}

    server.shutdown()
    thread.join(timeout=2)


def test_flight_plan_and_get_with_multiple_static_alias_targets(tmp_path):
    users_id = _create_iceberg_table(
        tmp_path,
        "shared",
        "shared-warehouse",
        [1, 2, 3, 4],
        identifier="analytics.users",
    )
    events_id = _create_iceberg_table(
        tmp_path,
        "shared",
        "shared-warehouse",
        [10, 11, 12, 13],
        identifier="analytics.events",
    )
    service_config_path, policy_path = _write_yaml_files(
        tmp_path,
        service_config={
            "catalogs": {
                "shared": {
                    "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'shared.db'}",
                        "warehouse": str(tmp_path / "shared-warehouse"),
                    },
                    "targets": {
                        "users_alias": {"table": users_id},
                        "events_alias": {"table": events_id},
                    },
                },
            }
        },
        policy={
            "version": 1,
            "catalogs": {
                "shared": {
                    "targets": {
                        "users_alias": {
                            "rules": [
                                {
                                    "principals": ["user1"],
                                    "columns": ["id", "email", "region"],
                                    "row_filter": "region = 'us'",
                                }
                            ]
                        },
                        "events_alias": {
                            "rules": [
                                {
                                    "principals": ["user1"],
                                    "columns": ["id", "email", "region"],
                                }
                            ]
                        },
                    }
                },
            },
        },
    )

    catalog_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")

    users_info, users_table = _flight_request(
        client,
        {"catalog": "shared", "target": "users_alias", "columns": ["id", "email", "region"]},
    )
    events_info, events_table = _flight_request(
        client,
        {
            "catalog": "shared",
            "target": "events_alias",
            "columns": ["id", "email", "region"],
        },
    )

    assert users_info.schema.field("id").type == pa.int64()
    assert users_table.num_rows == 2
    assert set(users_table.column("region").to_pylist()) == {"us"}
    assert events_info.schema.field("id").type == pa.int64()
    assert events_table.num_rows == 4

    server.shutdown()
    thread.join(timeout=2)


def test_flight_plan_rejects_direct_target_without_catalog(tmp_path):
    _create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    service_config_path, policy_path = _write_yaml_files(
        tmp_path,
        service_config={
            "catalogs": {
                "ice_one": {
                    "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'ice_one.db'}",
                        "warehouse": str(tmp_path / "warehouse-one"),
                    },
                },
            },
        },
        policy={
            "version": 1,
            "catalogs": {
                "ice_one": {
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
            },
        },
    )

    catalog_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")

    descriptor = flight.FlightDescriptor.for_command(
        json.dumps({"target": "default.users", "columns": ["id", "email", "region"]}).encode(
            "utf-8"
        )
    )
    with pytest.raises(flight.FlightInternalError):
        client.get_flight_info(descriptor, options=_flight_options())

    server.shutdown()
    thread.join(timeout=2)


def test_flight_plan_and_get_with_iceberg_multi_file_large(tmp_path):
    table_id = _create_iceberg_table(
        tmp_path,
        "ice_one",
        "warehouse-one",
        identifier="default.users",
        append_batches=[
            list(range(1, 100_001)),
            list(range(100_001, 200_001)),
        ],
    )
    service_config_path, policy_path = _write_yaml_files(
        tmp_path,
        service_config={
            "catalogs": {
                "ice_one": {
                    "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'ice_one.db'}",
                        "warehouse": str(tmp_path / "warehouse-one"),
                    },
                },
            },
        },
        policy={
            "version": 1,
            "catalogs": {
                "ice_one": {
                    "targets": {
                        table_id: {
                            "rules": [
                                {
                                    "principals": ["user1"],
                                    "columns": ["id", "email", "region"],
                                    "masks": {"id": {"type": "hash"}},
                                    "row_filter": "region = 'us'",
                                }
                            ]
                        }
                    }
                },
            },
        },
    )

    catalog_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, policy_path, max_tickets=4)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    info, table = _flight_request(
        client,
        {"catalog": "ice_one", "target": table_id, "columns": ["id", "email", "region"]},
    )

    assert len(info.endpoints) == 2
    assert info.schema.field("id").type == pa.string()
    assert table.num_rows == 100_000
    assert set(table.column("region").to_pylist()) == {"us"}

    server.shutdown()
    thread.join(timeout=2)


def test_hot_reload_does_not_break_iceberg_catalog_registry(tmp_path):
    v1_table_id = _create_iceberg_table(
        tmp_path,
        "shared",
        "shared-warehouse",
        [1, 2, 3, 4],
        identifier="analytics.users_v1",
    )
    v2_table_id = _create_iceberg_table(
        tmp_path,
        "shared",
        "shared-warehouse",
        [10, 11, 12, 13, 14, 15],
        identifier="analytics.users_v2",
    )
    service_config_path, policy_path = _write_yaml_files(
        tmp_path,
        service_config={
            "catalogs": {
                "shared": {
                    "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'shared.db'}",
                        "warehouse": str(tmp_path / "shared-warehouse"),
                    },
                    "targets": {
                        "users_alias": {"table": v1_table_id},
                    },
                },
            }
        },
        policy={
            "version": 1,
            "catalogs": {
                "shared": {
                    "targets": {
                        "users_alias": {
                            "rules": [
                                {
                                    "principals": ["user1"],
                                    "columns": ["id", "email", "region"],
                                    "row_filter": "region = 'us'",
                                }
                            ]
                        }
                    }
                },
            },
        },
    )

    catalog_registry = _build_registries(service_config_path)
    server = _build_server(catalog_registry, policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")

    old_info, options = _flight_info(
        client,
        {"catalog": "shared", "target": "users_alias", "columns": ["id", "email", "region"]},
    )
    old_table = _read_table(client, old_info, options)
    old_ticket = old_info.endpoints[0].ticket

    service_config = load_catalog_config(service_config_path)
    updated_catalogs = dict(service_config.catalogs)
    updated_target = updated_catalogs["shared"].targets["users_alias"]
    updated_catalogs["shared"] = type(updated_catalogs["shared"])(
        name=updated_catalogs["shared"].name,
        module=updated_catalogs["shared"].module,
        options=updated_catalogs["shared"].options,
        targets={
            **updated_catalogs["shared"].targets,
            "users_alias": type(updated_target)(
                backend=updated_target.backend,
                table=v2_table_id,
                format=updated_target.format,
                paths=updated_target.paths,
                options=updated_target.options,
            ),
        },
    )
    catalog_registry.reload(
        type(service_config)(catalogs=updated_catalogs, paths=service_config.paths)
    )

    new_info, new_options = _flight_info(
        client,
        {"catalog": "shared", "target": "users_alias", "columns": ["id", "email", "region"]},
    )
    new_table = _read_table(client, new_info, new_options)

    assert old_table.num_rows == 2
    assert new_table.num_rows == 3

    client.do_get(old_ticket, options=options).read_all()

    server.shutdown()
    thread.join(timeout=2)
