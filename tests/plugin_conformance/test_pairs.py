from __future__ import annotations

from dal_obscura_iceberg_rest.catalog import DESCRIPTOR as REST_CATALOG_DESCRIPTOR
from dal_obscura_manifest_parquet.catalog import CATALOG_DESCRIPTOR as MANIFEST_CATALOG_DESCRIPTOR
from dal_obscura_manifest_parquet.format import FORMAT_DESCRIPTOR as PARQUET_FORMAT_DESCRIPTOR


def test_independent_catalog_and_format_descriptors_are_pairable() -> None:
    pair_capabilities = (
        MANIFEST_CATALOG_DESCRIPTOR.capabilities & PARQUET_FORMAT_DESCRIPTOR.capabilities
    )

    assert MANIFEST_CATALOG_DESCRIPTOR.kind == "catalog"
    assert PARQUET_FORMAT_DESCRIPTOR.kind == "table_format"
    assert {"nested_schema", "splittable_scan"} <= pair_capabilities
    assert MANIFEST_CATALOG_DESCRIPTOR.distribution == "dal-obscura-manifest-parquet"
    assert PARQUET_FORMAT_DESCRIPTOR.distribution == "dal-obscura-manifest-parquet"


def test_rest_catalog_descriptor_stays_in_the_public_plugin_contract() -> None:
    assert REST_CATALOG_DESCRIPTOR.kind == "catalog"
    assert REST_CATALOG_DESCRIPTOR.plugin_id == "iceberg.rest"
    assert REST_CATALOG_DESCRIPTOR.api_version == "1"
    assert REST_CATALOG_DESCRIPTOR.config_version == 1
    assert REST_CATALOG_DESCRIPTOR.distribution == "dal-obscura-iceberg-rest"
    assert {"nested_schema", "snapshot_reads", "splittable_scan"} <= (
        REST_CATALOG_DESCRIPTOR.capabilities
    )
