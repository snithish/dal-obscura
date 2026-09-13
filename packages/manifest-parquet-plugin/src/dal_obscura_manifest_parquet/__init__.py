"""Independent manifest-governed Parquet plugin distribution."""

from dal_obscura_manifest_parquet.catalog import ManifestCatalog, manifest_factory
from dal_obscura_manifest_parquet.format import (
    ParquetDatasetFormat,
    ParquetRowGroupTask,
    parquet_factory,
)

__all__ = [
    "ManifestCatalog",
    "ParquetDatasetFormat",
    "ParquetRowGroupTask",
    "manifest_factory",
    "parquet_factory",
]
