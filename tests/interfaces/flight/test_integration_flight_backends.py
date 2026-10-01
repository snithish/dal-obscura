from pathlib import Path

import pyarrow as pa
import pytest

from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from tests.support.flight import (
    build_flight_service,
    command_descriptor,
    flight_call_options,
    flight_request,
    running_flight_client,
)
from tests.support.iceberg import create_iceberg_table
from tests.support.iceberg_flight import service_config_from_raw
from tests.support.policy import allow_rule

pytestmark = pytest.mark.heavy


def _build_registry(
    catalogs: dict[str, CatalogConfig],
) -> CatalogRegistry:
    return CatalogRegistry(ServiceConfig(catalogs=catalogs))


def _build_registry_from_config(service_config: ServiceConfig) -> CatalogRegistry:
    return CatalogRegistry(service_config)


def build_service_config(
    tmp_path: Path,
    *,
    service_config: dict[str, object],
    policy: dict[str, object],
) -> tuple[ServiceConfig, None]:
    del tmp_path, policy
    return service_config_from_raw(service_config), None


def test_flight_plan_and_get_with_iceberg_multi_catalog(tmp_path):
    table_id_one = create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    table_id_two = create_iceberg_table(tmp_path, "ice_two", "warehouse-two", [10, 11, 12, 13])
    service_config, _policy_path = build_service_config(
        tmp_path,
        service_config={
            "catalogs": {
                "ice_one": {
                    "type": "iceberg",
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'ice_one.db'}",
                        "warehouse": str(tmp_path / "warehouse-one"),
                    },
                },
                "ice_two": {
                    "type": "iceberg",
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

    catalog_registry = _build_registry_from_config(service_config)
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
        assert table_one.num_rows == 2
        assert set(table_one.column("region").to_pylist()) == {"us"}
        assert set(table_two.column("region").to_pylist()) == {"eu", "us"}
        assert table_two.num_rows == 4


def test_flight_plan_and_get_with_iceberg_requested_row_filter_on_unprojected_column(tmp_path):
    table_id = create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    service_config, _policy_path = build_service_config(
        tmp_path,
        service_config={
            "catalogs": {
                "ice_one": {
                    "type": "iceberg",
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
                                    "row_filter": "id > 1",
                                }
                            ]
                        }
                    }
                }
            },
        },
    )

    server = build_flight_service(
        catalog_registry=_build_registry_from_config(service_config),
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
    service_config, _policy_path = build_service_config(
        tmp_path,
        service_config={
            "catalogs": {
                "ice_one": {
                    "type": "iceberg",
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

    catalog_registry = _build_registry_from_config(service_config)
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
    service_config, _policy_path = build_service_config(
        tmp_path,
        service_config={
            "catalogs": {
                "ice_one": {
                    "type": "iceberg",
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

    catalog_registry = _build_registry_from_config(service_config)
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
        assert set(table.column("region").to_pylist()) == {"us"}
