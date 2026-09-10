"""Python client helpers for the public dal-obscura Flight read contract.

Example:
    ```python
    from dal_obscura.connectors import DalObscuraClient

    with DalObscuraClient("grpc+tcp://localhost:8815", auth_token=token) as client:
        table = client.read_table(
            catalog="analytics",
            target="default.users",
            columns=["id", "email"],
            row_filter="region = 'us'",
        )
    ```
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
import pyarrow.flight as flight

from dal_obscura.common.flight_contract import FLIGHT_PROTOCOL_VERSION, encode_plan_command

PROTOCOL_VERSION = FLIGHT_PROTOCOL_VERSION


class DalObscuraBatchStream(Iterator[pa.RecordBatch]):
    """A context-managed, sequential stream of Flight record batches."""

    def __init__(self, client: flight.FlightClient, info: flight.FlightInfo, options) -> None:
        self.schema = info.schema
        self._client = client
        self._endpoints = iter(info.endpoints)
        self._options = options
        self._reader = None
        self._closed = False

    def __iter__(self) -> DalObscuraBatchStream:
        return self

    def __next__(self) -> pa.RecordBatch:
        if self._closed:
            raise StopIteration
        try:
            while True:
                if self._reader is None:
                    endpoint = next(self._endpoints)
                    self._reader = self._client.do_get(endpoint.ticket, options=self._options)
                try:
                    chunk = self._reader.read_chunk()
                except StopIteration:
                    self._close_reader()
                    continue
                if chunk.data is not None:
                    return chunk.data
        except StopIteration:
            self.close()
            raise
        except BaseException:
            self.close()
            raise

    def cancel(self) -> None:
        """Cancels local consumption and closes the active Flight reader."""
        self.close()

    def close(self) -> None:
        """Closes the active reader; repeated calls are safe."""
        if self._closed:
            return
        self._closed = True
        self._close_reader()

    def __enter__(self) -> DalObscuraBatchStream:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        self.close()

    def _close_reader(self) -> None:
        reader, self._reader = self._reader, None
        if reader is None:
            return
        close = getattr(reader, "close", None)
        if callable(close):
            close()


class DalObscuraClient:
    """Small Python SDK for the dal-obscura Arrow Flight read contract.

    Example:
        ```python
        with DalObscuraClient("grpc+tcp://localhost:8815", auth_token=token) as client:
            schema = client.fetch_schema(
                catalog="analytics",
                target="default.users",
                columns=["id", "email"],
            )
        ```
    """

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
        """Wraps an existing PyArrow Flight client without taking ownership.

        Example:
            ```python
            flight_client = flight.FlightClient("grpc+tcp://localhost:8815")
            client = DalObscuraClient.from_flight_client(flight_client, auth_token=token)
            ```
        """
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
        """Returns the authorized output schema without reading rows.

        Example:
            ```python
            schema = client.fetch_schema(
                catalog="analytics",
                target="default.users",
                columns=["id", "email"],
            )
            ```
        """
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
        """Plans a governed read and returns Flight endpoints with opaque tickets.

        Example:
            ```python
            info = client.plan(
                catalog="analytics",
                target="default.users",
                columns=["id"],
            )
            ```
        """
        descriptor = _descriptor(catalog, target, columns, row_filter)
        return self._client.get_flight_info(descriptor, options=self._call_options())

    def read_batches(
        self,
        *,
        catalog: str | None,
        target: str,
        columns: Iterable[str],
        row_filter: str | None = None,
    ) -> DalObscuraBatchStream:
        """Yields authorized record batches from every planned endpoint.

        Example:
            ```python
            for batch in client.read_batches(
                catalog="analytics",
                target="default.users",
                columns=["id", "email"],
            ):
                handle(batch)
            ```
        """
        info = self.plan(catalog=catalog, target=target, columns=columns, row_filter=row_filter)
        return DalObscuraBatchStream(self._client, info, self._call_options())

    def read_table(
        self,
        *,
        catalog: str | None,
        target: str,
        columns: Iterable[str],
        row_filter: str | None = None,
    ) -> pa.Table:
        """Reads all authorized batches into a PyArrow table.

        Example:
            ```python
            table = client.read_table(
                catalog="analytics",
                target="default.users",
                columns=["id", "email"],
            )
            ```
        """
        stream = self.read_batches(
            catalog=catalog,
            target=target,
            columns=columns,
            row_filter=row_filter,
        )
        with stream:
            batches = list(stream)
        if batches:
            return pa.Table.from_batches(batches, schema=stream.schema)
        return pa.Table.from_batches([], schema=stream.schema)

    def close(self) -> None:
        """Closes the owned Flight client, if this instance created it."""
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
    """Exposes SDK reads as DuckDB relations for local analytical clients.

    Example:
        ```python
        with DalObscuraClient("grpc+tcp://localhost:8815", auth_token=token) as client:
            reader = DuckDBDalObscuraReader(client)
            relation = reader.relation(
                catalog="analytics",
                target="default.users",
                columns=["id", "email"],
            )
        ```
    """

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
        """Reads a governed table and registers it as a DuckDB relation."""
        schema = self._client.fetch_schema(
            catalog=catalog,
            target=target,
            columns=columns,
            row_filter=row_filter,
        )
        batches = self._client.read_batches(
            catalog=catalog,
            target=target,
            columns=columns,
            row_filter=row_filter,
        )
        return self._connection.from_arrow(pa.RecordBatchReader.from_batches(schema, batches))

    def close(self) -> None:
        """Closes the owned DuckDB connection, if this instance created it."""
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
    return flight.FlightDescriptor.for_command(
        encode_plan_command(
            protocol_version=PROTOCOL_VERSION,
            catalog=catalog,
            target=target,
            columns=list(columns),
            row_filter=row_filter,
            include_typed_paths=True,
        )
    )


def _location_for(uri: str) -> flight.Location:
    parsed = urlparse(uri)
    if parsed.hostname is None or parsed.port is None:
        raise ValueError(f"Invalid dal-obscura Flight URI: {uri}")
    if parsed.scheme == "grpc+tcp":
        return flight.Location.for_grpc_tcp(parsed.hostname, parsed.port)
    if parsed.scheme == "grpc+tls":
        return flight.Location.for_grpc_tls(parsed.hostname, parsed.port)
    raise ValueError(f"Unsupported dal-obscura Flight URI scheme: {parsed.scheme}")
