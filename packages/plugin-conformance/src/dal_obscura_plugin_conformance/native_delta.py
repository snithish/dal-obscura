"""Native Delta Spark mutations for the JVM qualification lane."""

import os
import sys
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from pathlib import Path
from subprocess import TimeoutExpired
from tempfile import TemporaryDirectory
from typing import Any, cast

from dal_obscura_plugin_api.execution import close_preserving_error


@contextmanager
def delta_writer() -> Generator[Any]:
    """Own a disposable Spark driver; never start Python workers or expose its UI."""
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    if SparkContext._active_spark_context is not None:
        raise RuntimeError("Native fixture writer requires its own Spark driver")

    previous = os.environ.get("SPARK_LOCAL_IP")
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    with TemporaryDirectory(prefix="dal-delta-writer-") as directory:
        spark = None
        try:
            spark = (
                SparkSession.builder.master("local[1]")
                .appName("dal-native-delta-writer")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.jars.ivy", directory + "/ivy")
                .config("spark.sql.warehouse.dir", directory + "/warehouse")
                .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
            cast(Any, spark).sparkContext.setLogLevel("ERROR")
            yield spark
        finally:
            primary_error = sys.exception()
            cleanup_error = None
            try:
                if spark is not None:
                    spark.stop()
            except BaseException as exc:
                cleanup_error = exc
            try:
                # PySpark's cached gateway refers to this driver's temporary jars.
                gateway = SparkContext._gateway
                if gateway is not None:
                    try:
                        _shutdown_gateway(gateway)
                    finally:
                        SparkContext._gateway = None
                        SparkContext._jvm = None
            except BaseException as exc:
                if cleanup_error is None:
                    cleanup_error = exc
            finally:
                if previous is None:
                    os.environ.pop("SPARK_LOCAL_IP", None)
                else:
                    os.environ["SPARK_LOCAL_IP"] = previous
            if primary_error is None and cleanup_error is not None:
                raise cleanup_error


def _shutdown_gateway(gateway: Any) -> None:
    try:
        gateway.shutdown()
    finally:
        process = getattr(gateway, "proc", None)
        if process is not None:
            try:
                try:
                    process.wait(timeout=10)
                except TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=10)
            finally:
                primary_error = sys.exception()
                cleanup_error = None
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None:
                        try:
                            close_preserving_error(stream)
                        except BaseException as error:
                            if cleanup_error is None:
                                cleanup_error = error
                if primary_error is None and cleanup_error is not None:
                    raise cleanup_error


def delete_delta_rows(path: Path, identifiers: Iterable[int], *, spark: Any = None) -> None:
    """Delete explicit fixture IDs using Delta's native SQL writer."""
    identifiers = tuple(identifiers)
    if not identifiers or any(type(value) is not int for value in identifiers):
        raise ValueError("Delta delete identifiers must be nonempty integers")
    if spark is None:
        with delta_writer() as writer:
            delete_delta_rows(path, identifiers, spark=writer)
        return
    location = str(path.resolve()).replace("`", "``")
    predicate = ",".join(str(value) for value in identifiers)
    spark.sql(f"DELETE FROM delta.`{location}` WHERE id IN ({predicate})")
