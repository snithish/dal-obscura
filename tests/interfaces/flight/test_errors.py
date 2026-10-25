"""Flight status, private diagnostics and cleanup at the actual RPC boundary."""

from types import SimpleNamespace

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from tests.support.arrow import id_region_batch, id_region_schema
from tests.support.flight.flight import (
    StubTableFormat,
    authorization_header,
    build_flight_service,
    command_descriptor,
    flight_call_options,
    running_flight_client,
)

pytestmark = pytest.mark.socket


@pytest.mark.parametrize("after_first_batch", [False, True])
def test_stream_resource_failure_is_unavailable_and_closes_source(after_first_batch):
    from dal_obscura.application.reads.execution import StreamResourceError

    _assert_stream_failure(
        after_first_batch,
        StreamResourceError("Governed stream memory budget exhausted"),
        flight.FlightUnavailableError,
        "memory budget exhausted",
    )


@pytest.mark.parametrize("operation", ["get_schema", "get_flight_info", "do_get"])
@pytest.mark.parametrize("error_type", [RuntimeError, ValueError, PermissionError])
def test_provider_errors_preserve_status_without_disclosing_details(operation, error_type, caplog):
    from tests.support.read.fakes import FixtureTaskCodec
    from tests.support.read.fakes import StubTableFormat as MemorySource

    failure = error_type("credential-do-not-disclose" + "private" * 2000)

    class FailingSource(MemorySource):
        def get_schema(self):
            if operation == "get_schema":
                raise failure
            return super().get_schema()

        def plan(self, request, max_tickets):
            if operation == "get_flight_info":
                raise failure
            return super().plan(request, max_tickets)

    class FailingCodec(FixtureTaskCodec):
        def decode(self, payload):
            raise failure

    server = build_flight_service(
        table_format=FailingSource(
            catalog_name="analytics",
            table_name="test.table",
            format="stub_format",
            schema=id_region_schema(),
            batches=(id_region_batch([1], ["us"]),),
        ),
        task_codec=FailingCodec(),
    )
    expected = flight.FlightInternalError
    if error_type is PermissionError:
        expected = flight.FlightUnauthorizedError
    elif error_type is ValueError and operation != "do_get":
        expected = pa.ArrowInvalid
    schema_error = operation == "get_schema" and error_type is not ValueError
    descriptor = command_descriptor(
        {"catalog": "analytics", "target": "test.table", "columns": ["id"]}
    )
    options = flight_call_options("user1")
    with running_flight_client(server) as client:
        ticket = (
            client.get_flight_info(descriptor, options=options).endpoints[0].ticket
            if operation == "do_get"
            else None
        )
        # PyArrow's schema RPC wraps Flight status exceptions in ArrowException.
        with pytest.raises(pa.ArrowException if schema_error else expected) as caught:
            if ticket is not None:
                client.do_get(ticket, options=options).read_all()
            else:
                getattr(client, operation)(descriptor, options=options)
    if schema_error:
        assert f"pyarrow._flight.{expected.__name__}:" in str(caught.value)
    assert "credential-do-not-disclose" not in str(caught.value)
    assert len(str(caught.value)) < 4096
    assert "credential-do-not-disclose" not in caplog.text
    if error_type is RuntimeError:
        record = next(
            record for record in caplog.records if record.message == "flight_operation_failed"
        )
        assert record.error_type == "RuntimeError"
        assert (
            record.phase
            == {"get_schema": "schema", "get_flight_info": "plan", "do_get": "fetch"}[operation]
        )
        assert record.correlation_id


@pytest.mark.parametrize("after_first_batch", [False, True])
@pytest.mark.parametrize("failure_on_close", [False, True])
def test_unexpected_stream_errors_are_private_and_close_output(after_first_batch, failure_on_close):
    observed = _assert_stream_failure(
        after_first_batch,
        RuntimeError("credential-do-not-disclose" + "private" * 2000),
        flight.FlightInternalError,
        "Read operation failed",
        failure_on_close=failure_on_close,
    )
    assert "credential-do-not-disclose" not in observed
    assert len(observed) < 4096


def _assert_stream_failure(
    after_first_batch,
    failure,
    expected_error,
    message,
    *,
    consume_chunks=False,
    failure_on_close=False,
):
    from threading import Thread

    from dal_obscura.interfaces.flight.streaming import make_stream

    closed = []
    schema = pa.schema([pa.field("id", pa.int64())])

    def batches():
        if after_first_batch:
            yield pa.record_batch([pa.array([1])], schema=schema)
        if not failure_on_close:
            raise failure

    class Output:
        def __iter__(self):
            return batches()

        def close(self):
            closed.append(True)
            if failure_on_close:
                raise failure

    class Server(flight.FlightServerBase):
        def do_get(self, context, ticket):
            return make_stream(schema, Output())

    server = Server("grpc://127.0.0.1:0")
    thread = Thread(target=server.serve)
    thread.start()
    try:
        with (
            flight.FlightClient(f"grpc://127.0.0.1:{server.port}") as client,
            pytest.raises(expected_error, match=message) as caught,
        ):
            reader = client.do_get(flight.Ticket(b"test"))
            if consume_chunks:
                # read_all converts ArrowCancelled into SIGINT; chunks expose Flight status.
                for _ in range(2):
                    reader.read_chunk()
            else:
                reader.read_all()
        assert closed == [True]
        return str(caught.value)
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.parametrize("operation", ["get_schema", "get_flight_info"])
def test_provider_capacity_failure_returns_unavailable(operation):
    from dal_obscura.application.reads.sources import AccessContextUnavailable

    class UnavailableAuthorizer:
        def authorize(self, *args, **kwargs):
            raise AccessContextUnavailable("Catalog provider capacity is unavailable; retry later")

    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub",
        schema=id_region_schema(),
        batches=(),
    )
    server = build_flight_service(table_format=table_format, authorizer=UnavailableAuthorizer())
    descriptor = command_descriptor(
        {"catalog": "analytics", "target": "test.table", "columns": ["id"]}
    )
    try:
        with pytest.raises(flight.FlightUnavailableError, match="retry later"):
            getattr(server, operation)(
                SimpleNamespace(
                    headers=[authorization_header("user1")], is_cancelled=lambda: False
                ),
                descriptor,
            )
    finally:
        server.shutdown()


@pytest.mark.parametrize("after_first_batch", [False, True])
@pytest.mark.parametrize("failure_kind", ["supervisor_cancel", "provider_cancel", "deadline"])
def test_stream_interruption_status_and_cleanup(after_first_batch, failure_kind):
    from dal_obscura.application.reads.execution import StreamCancelledError

    failure, expected, message = {
        "supervisor_cancel": (
            StreamCancelledError("Stream cancelled"),
            flight.FlightCancelledError,
            "cancelled",
        ),
        "provider_cancel": (
            InterruptedError("credential-do-not-disclose"),
            flight.FlightCancelledError,
            "cancelled",
        ),
        "deadline": (
            TimeoutError("credential-do-not-disclose"),
            flight.FlightTimedOutError,
            "Read deadline exceeded",
        ),
    }[failure_kind]
    observed = _assert_stream_failure(
        after_first_batch,
        failure,
        expected,
        message,
        consume_chunks=True,
    )
    assert "credential-do-not-disclose" not in observed


@pytest.mark.parametrize("denied", [False, True])
def test_lazy_source_initialization_keeps_its_transport_error_classification(denied):
    from dal_obscura.application.reads.execution import (
        SourceInitializationPermissionError,
        SourceInitializationValueError,
    )

    if denied:
        _assert_stream_failure(
            False,
            SourceInitializationPermissionError("source rejected"),
            flight.FlightUnauthorizedError,
            "Unauthorized",
        )
    else:
        _assert_stream_failure(
            False,
            SourceInitializationValueError("source rejected"),
            flight.FlightInternalError,
            "Ticket payload mismatch",
        )
