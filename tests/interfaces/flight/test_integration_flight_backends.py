import hashlib

import pyarrow as pa
import pytest
from dal_obscura_plugin_conformance.iceberg_fixtures import create_iceberg_table

from dal_obscura.sources.catalogs import (
    CatalogRegistry,
)
from tests.support.flight.flight import (
    build_flight_service,
    command_descriptor,
    flight_call_options,
    flight_request,
    running_flight_client,
)
from tests.support.flight.iceberg_flight import service_config_from_raw
from tests.support.policy.policy import allow_rule

pytestmark = [pytest.mark.heavy, pytest.mark.socket]


def test_flight_plan_and_get_with_iceberg_multi_catalog(tmp_path):
    table_id_one = create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    table_id_two = create_iceberg_table(tmp_path, "ice_two", "warehouse-two", [10, 11, 12, 13])
    service_config = service_config_from_raw(
        {
            "catalogs": {
                "ice_one": {
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'ice_one.db'}",
                        "warehouse": str(tmp_path / "warehouse-one"),
                    },
                },
                "ice_two": {
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'ice_two.db'}",
                        "warehouse": str(tmp_path / "warehouse-two"),
                    },
                },
            },
        },
    )

    catalog_registry = CatalogRegistry(service_config)
    server = build_flight_service(
        catalog_registry=catalog_registry,
        policy_rules_by_dataset={
            ("ice_one", table_id_one): [
                allow_rule(
                    ["id", "email", "region"],
                    masks={"id": {"type": "hash"}},
                    row_filter="region = 'us'",
                )
            ],
            ("ice_two", table_id_two): [allow_rule(["id", "email", "region"])],
        },
        max_tickets=4,
    )
    with running_flight_client(server) as client:
        info_one, table_one = flight_request(
            client,
            {"catalog": "ice_one", "target": table_id_one, "columns": ["id", "email", "region"]},
        )
        _info_two, table_two = flight_request(
            client,
            {"catalog": "ice_two", "target": table_id_two, "columns": ["id", "email", "region"]},
        )

        assert info_one.schema.field("id").type == pa.string()
        assert table_one.to_pylist() == [
            {"id": hashlib.sha256(b"2").hexdigest(), "email": "user2@example.com", "region": "us"},
            {"id": hashlib.sha256(b"4").hexdigest(), "email": "user4@example.com", "region": "us"},
        ]
        assert table_two.to_pylist() == [
            {"id": 10, "email": "user10@example.com", "region": "us"},
            {"id": 11, "email": "user11@example.com", "region": "eu"},
            {"id": 12, "email": "user12@example.com", "region": "us"},
            {"id": 13, "email": "user13@example.com", "region": "eu"},
        ]


def test_flight_plan_and_get_with_iceberg_requested_row_filter_on_unprojected_column(tmp_path):
    table_id = create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    service_config = service_config_from_raw(
        {
            "catalogs": {
                "ice_one": {
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'ice_one.db'}",
                        "warehouse": str(tmp_path / "warehouse-one"),
                    },
                },
            },
        },
    )

    server = build_flight_service(
        catalog_registry=CatalogRegistry(service_config),
        policy_rules=[
            {
                "principals": ["user1"],
                "columns": ["id", "email", "region"],
                "row_filter": "id > 1",
                "effect": "allow",
            }
        ],
        max_tickets=4,
    )

    with running_flight_client(server) as client:
        info, table = flight_request(
            client,
            {
                "catalog": "ice_one",
                "target": table_id,
                "columns": ["id", "email"],
                "row_filter": "LOWER(region) = 'us'",
            },
        )

    assert info.schema.names == ["id", "email"]
    assert table.column("id").to_pylist() == [2, 4]


def test_flight_plan_rejects_direct_target_without_catalog(tmp_path):
    create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    service_config = service_config_from_raw(
        {
            "catalogs": {
                "ice_one": {
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'ice_one.db'}",
                        "warehouse": str(tmp_path / "warehouse-one"),
                    },
                },
            },
        },
    )

    catalog_registry = CatalogRegistry(service_config)
    server = build_flight_service(
        catalog_registry=catalog_registry,
        policy_rules=[
            {
                "principals": ["user1"],
                "columns": ["id", "email", "region"],
                "effect": "allow",
            }
        ],
        max_ticket_exchanges=2,
    )
    with running_flight_client(server) as client:
        descriptor = command_descriptor(
            {"target": "default.users", "columns": ["id", "email", "region"]}
        )
        with pytest.raises(pa.ArrowInvalid, match="Invalid request"):
            client.get_flight_info(descriptor, options=flight_call_options())


def test_flight_plan_and_get_with_iceberg_multi_file_large(tmp_path):
    table_id = create_iceberg_table(
        tmp_path,
        "ice_one",
        "warehouse-one",
        identifier="default.users",
        append_batches=[
            list(range(1, 100_001)),
            list(range(100_001, 200_001)),
        ],
    )
    service_config = service_config_from_raw(
        {
            "catalogs": {
                "ice_one": {
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'ice_one.db'}",
                        "warehouse": str(tmp_path / "warehouse-one"),
                    },
                },
            },
        },
    )

    catalog_registry = CatalogRegistry(service_config)
    server = build_flight_service(
        catalog_registry=catalog_registry,
        policy_rules=[
            {
                "principals": ["user1"],
                "columns": ["id", "email", "region"],
                "masks": {"id": {"type": "hash"}},
                "row_filter": "region = 'us'",
                "effect": "allow",
            }
        ],
        max_tickets=4,
    )
    with running_flight_client(server) as client:
        info, table = flight_request(
            client,
            {"catalog": "ice_one", "target": table_id, "columns": ["id", "email", "region"]},
        )

        assert len(info.endpoints) == 2
        assert info.schema.field("id").type == pa.string()
        assert table.num_rows == 100_000
        assert set(
            zip(
                table.column("id").to_pylist(),
                table.column("email").to_pylist(),
                table.column("region").to_pylist(),
                strict=True,
            )
        ) == {
            (hashlib.sha256(str(value).encode()).hexdigest(), f"user{value}@example.com", "us")
            for value in range(2, 200_001, 2)
        }
