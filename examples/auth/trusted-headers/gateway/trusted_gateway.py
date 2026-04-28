from __future__ import annotations

import os

import pyarrow.flight as flight


class TrustedHeaderGateway(flight.FlightServerBase):
    def __init__(self, location: str, backend_uri: str) -> None:
        super().__init__(location)
        self._backend = flight.connect(backend_uri)

    def get_schema(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.SchemaResult:
        del context
        return self._backend.get_schema(descriptor, options=_trusted_options())

    def get_flight_info(
        self, context: flight.ServerCallContext, descriptor: flight.FlightDescriptor
    ) -> flight.FlightInfo:
        del context
        return self._backend.get_flight_info(descriptor, options=_trusted_options())

    def do_get(
        self, context: flight.ServerCallContext, ticket: flight.Ticket
    ) -> flight.RecordBatchStream:
        del context
        reader = self._backend.do_get(ticket, options=_trusted_options())
        return flight.RecordBatchStream(reader.read_all())


def main() -> None:
    gateway = TrustedHeaderGateway(
        os.environ.get("GATEWAY_LOCATION", "grpc://0.0.0.0:8816"),
        os.environ["BACKEND_FLIGHT_URI"],
    )
    gateway.serve()


def _trusted_options() -> flight.FlightCallOptions:
    headers = [
        (b"x-dal-obscura-proxy-secret", os.environ["DAL_OBSCURA_PROXY_SECRET"].encode()),
        (b"x-auth-request-user", os.environ.get("TRUSTED_HEADER_SUBJECT", "example-user").encode()),
        (
            b"x-auth-request-groups",
            os.environ.get("TRUSTED_HEADER_GROUPS", "compose-example").encode(),
        ),
        (b"x-auth-request-attr-source", b"trusted-gateway"),
    ]
    return flight.FlightCallOptions(headers=headers)


if __name__ == "__main__":
    main()
