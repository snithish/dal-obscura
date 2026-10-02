from collections.abc import Mapping

import duckdb

TRANSFORM_CONFIG: dict[str, str | bool | int | float | list[str]] = {
    "enable_external_access": "false",
    "autoload_known_extensions": "false",
    "autoinstall_known_extensions": "false",
    "threads": 1,
}


def connect(
    config: Mapping[str, str | bool | int | float | list[str]] | None = None,
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config=dict(config or TRANSFORM_CONFIG))
    try:
        con.execute("SET enable_progress_bar = false")
        return con
    except BaseException:
        con.close()
        raise
