from pathlib import Path

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    DynamicCatalogRegistry,
    ServiceConfig,
)
from tests.support.flight import (
    build_flight_service,
    command_descriptor,
    flight_call_options,
    flight_info,
    flight_request,
    read_table,
    running_flight_client,
)
from tests.support.iceberg import create_iceberg_table
from tests.support.iceberg_flight import ICEBERG_CATALOG_MODULE, service_config_from_raw
from tests.support.policy import allow_rule

pytestmark = pytest.mark.heavy


def _build_registry(
    catalogs: dict[str, CatalogConfig],
) -> DynamicCatalogRegistry:
    return DynamicCatalogRegistry(ServiceConfig(catalogs=catalogs, paths=()))


def _build_registry_from_config(service_config: ServiceConfig) -> DynamicCatalogRegistry:
    return DynamicCatalogRegistry(service_config)


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
                    "module": ICEBERG_CATALOG_MODULE,
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path / 'ice_one.db'}",
                        "warehouse": str(tmp_path / "warehouse-one"),
                    },
                },
                "ice_two": {
                    "module": ICEBERG_CATALOG_MODULE,
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


def test_flight_plan_and_get_with_static_catalog_alias(tmp_path):
    table_id = create_iceberg_table(
        tmp_path,
        "shared",
        "shared-warehouse",
        [1, 2, 3, 4],
        identifier="analytics.users",
    )
    service_config, _policy_path = build_service_config(
        tmp_path,
        service_config={
            "catalogs": {
                "shared": {
                    "module": ICEBERG_CATALOG_MODULE,
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

    catalog_registry = _build_registry_from_config(service_config)
    server = build_flight_service(
        catalog_registry=catalog_registry,
        policy_rules_by_dataset={
            ("shared", table_id): [allow_rule(["id", "email", "region"])],
            ("shared", "users_alias"): [
                allow_rule(
                    ["id", "email", "region"],
                    masks={"id": {"type": "hash"}},
                    row_filter="region = 'us'",
                )
            ],
        },
    )
    with running_flight_client(server) as client:
        direct_info, direct_table = flight_request(
            client,
            {"catalog": "shared", "target": table_id, "columns": ["id", "email", "region"]},
        )
        alias_info, alias_table = flight_request(
            client,
            {"catalog": "shared", "target": "users_alias", "columns": ["id", "email", "region"]},
        )

        assert direct_info.schema.field("id").type == pa.int64()
        assert direct_table.num_rows == 4
        assert alias_info.schema.field("id").type == pa.string()
        assert alias_table.num_rows == 2
        assert set(alias_table.column("region").to_pylist()) == {"us"}


def test_flight_plan_and_get_with_iceberg_requested_row_filter_on_unprojected_column(tmp_path):
    table_id = create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    service_config, _policy_path = build_service_config(
        tmp_path,
        service_config={
            "catalogs": {
                "ice_one": {
                    "module": ICEBERG_CATALOG_MODULE,
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


def test_flight_plan_and_get_with_multiple_static_alias_targets(tmp_path):
    users_id = create_iceberg_table(
        tmp_path,
        "shared",
        "shared-warehouse",
        [1, 2, 3, 4],
        identifier="analytics.users",
    )
    events_id = create_iceberg_table(
        tmp_path,
        "shared",
        "shared-warehouse",
        [10, 11, 12, 13],
        identifier="analytics.events",
    )
    service_config, _policy_path = build_service_config(
        tmp_path,
        service_config={
            "catalogs": {
                "shared": {
                    "module": ICEBERG_CATALOG_MODULE,
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

    catalog_registry = _build_registry_from_config(service_config)
    server = build_flight_service(
        catalog_registry=catalog_registry,
        policy_rules_by_dataset={
            ("shared", "users_alias"): [
                allow_rule(["id", "email", "region"], row_filter="region = 'us'")
            ],
            ("shared", "events_alias"): [allow_rule(["id", "email", "region"])],
        },
    )
    with running_flight_client(server) as client:
        users_info, users_table = flight_request(
            client,
            {"catalog": "shared", "target": "users_alias", "columns": ["id", "email", "region"]},
        )
        events_info, events_table = flight_request(
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


def test_flight_plan_rejects_direct_target_without_catalog(tmp_path):
    create_iceberg_table(tmp_path, "ice_one", "warehouse-one", [1, 2, 3, 4])
    service_config, _policy_path = build_service_config(
        tmp_path,
        service_config={
            "catalogs": {
                "ice_one": {
                    "module": ICEBERG_CATALOG_MODULE,
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
    )
    with running_flight_client(server) as client:
        descriptor = command_descriptor(
            {"target": "default.users", "columns": ["id", "email", "region"]}
        )
        with pytest.raises(flight.FlightInternalError):
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
                    "module": ICEBERG_CATALOG_MODULE,
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


def test_hot_reload_does_not_break_iceberg_catalog_registry(tmp_path):
    v1_table_id = create_iceberg_table(
        tmp_path,
        "shared",
        "shared-warehouse",
        [1, 2, 3, 4],
        identifier="analytics.users_v1",
    )
    v2_table_id = create_iceberg_table(
        tmp_path,
        "shared",
        "shared-warehouse",
        [10, 11, 12, 13, 14, 15],
        identifier="analytics.users_v2",
    )
    service_config, _policy_path = build_service_config(
        tmp_path,
        service_config={
            "catalogs": {
                "shared": {
                    "module": ICEBERG_CATALOG_MODULE,
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

    catalog_registry = _build_registry_from_config(service_config)
    server = build_flight_service(
        catalog_registry=catalog_registry,
        policy_rules=[
            {
                "principals": ["user1"],
                "columns": ["id", "email", "region"],
                "row_filter": "region = 'us'",
                "effect": "allow",
            }
        ],
    )
    with running_flight_client(server) as client:
        old_info, options = flight_info(
            client,
            {"catalog": "shared", "target": "users_alias", "columns": ["id", "email", "region"]},
        )
        old_table = read_table(client, old_info, options)
        old_ticket = old_info.endpoints[0].ticket

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

        new_info, new_options = flight_info(
            client,
            {"catalog": "shared", "target": "users_alias", "columns": ["id", "email", "region"]},
        )
        new_table = read_table(client, new_info, new_options)

        assert old_table.num_rows == 2
        assert new_table.num_rows == 3

        client.do_get(old_ticket, options=options).read_all()
