from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
import pyarrow.flight as flight

PROTOCOL_VERSION = 1


class DalObscuraClient:
    """Small Python SDK for the dal-obscura Arrow Flight read contract."""

    def __init__(self, uri: str, *, auth_token: str) -> None:
        self._client = flight.FlightClient(_location_for(uri))
        self._auth_token = auth_token
        self._owns_client = True

    @classmethod
    def from_flight_client(
        cls,
        client: flight.FlightClient,
        *,
        auth_token: str,
    ) -> DalObscuraClient:
        instance = cls.__new__(cls)
        instance._client = client
        instance._auth_token = auth_token
        instance._owns_client = False
        return instance

    def fetch_schema(
        self,
        *,
        catalog: str | None,
        target: str,
        columns: Iterable[str] = ("*",),
        row_filter: str | None = None,
    ) -> pa.Schema:
        descriptor = _descriptor(catalog, target, columns, row_filter)
        return self._client.get_schema(descriptor, options=self._call_options()).schema

    def plan(
        self,
        *,
        catalog: str | None,
        target: str,
        columns: Iterable[str],
        row_filter: str | None = None,
    ) -> flight.FlightInfo:
        descriptor = _descriptor(catalog, target, columns, row_filter)
        return self._client.get_flight_info(descriptor, options=self._call_options())

    def read_batches(
        self,
        *,
        catalog: str | None,
        target: str,
        columns: Iterable[str],
        row_filter: str | None = None,
    ) -> Iterator[pa.RecordBatch]:
        info = self.plan(catalog=catalog, target=target, columns=columns, row_filter=row_filter)
        for endpoint in info.endpoints:
            reader = self._client.do_get(endpoint.ticket, options=self._call_options())
            yield from reader.read_all().to_batches()

    def read_table(
        self,
        *,
        catalog: str | None,
        target: str,
        columns: Iterable[str],
        row_filter: str | None = None,
    ) -> pa.Table:
        info = self.plan(catalog=catalog, target=target, columns=columns, row_filter=row_filter)
        batches: list[pa.RecordBatch] = []
        for endpoint in info.endpoints:
            reader = self._client.do_get(endpoint.ticket, options=self._call_options())
            batches.extend(reader.read_all().to_batches())
        if batches:
            return pa.Table.from_batches(batches, schema=info.schema)
        return pa.Table.from_batches([], schema=info.schema)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> DalObscuraClient:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        self.close()

    def _call_options(self) -> flight.FlightCallOptions:
        return flight.FlightCallOptions(
            headers=[(b"authorization", f"Bearer {self._auth_token}".encode())]
        )


class DuckDBDalObscuraReader:
    """Exposes SDK reads as DuckDB relations for local analytical clients."""

    def __init__(
        self,
        client: DalObscuraClient,
        *,
        connection: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self._client = client
        self._connection = connection or duckdb.connect()
        self._owns_connection = connection is None

    def relation(
        self,
        *,
        catalog: str | None,
        target: str,
        columns: Iterable[str],
        row_filter: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        table = self._client.read_table(
            catalog=catalog,
            target=target,
            columns=columns,
            row_filter=row_filter,
        )
        return self._connection.from_arrow(table)

    def close(self) -> None:
        if self._owns_connection:
            self._connection.close()

    def __enter__(self) -> DuckDBDalObscuraReader:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        self.close()


def _descriptor(
    catalog: str | None,
    target: str,
    columns: Iterable[str],
    row_filter: str | None,
) -> flight.FlightDescriptor:
    payload: dict[str, object] = {
        "protocol_version": PROTOCOL_VERSION,
        "catalog": catalog,
        "target": target,
        "columns": list(columns),
    }
    if row_filter is not None:
        payload["row_filter"] = row_filter
    return flight.FlightDescriptor.for_command(json.dumps(payload).encode("utf-8"))


def _location_for(uri: str) -> flight.Location:
    parsed = urlparse(uri)
    if parsed.hostname is None or parsed.port is None:
        raise ValueError(f"Invalid dal-obscura Flight URI: {uri}")
    if parsed.scheme == "grpc+tcp":
        return flight.Location.for_grpc_tcp(parsed.hostname, parsed.port)
    if parsed.scheme == "grpc+tls":
        return flight.Location.for_grpc_tls(parsed.hostname, parsed.port)
    raise ValueError(f"Unsupported dal-obscura Flight URI scheme: {parsed.scheme}")
