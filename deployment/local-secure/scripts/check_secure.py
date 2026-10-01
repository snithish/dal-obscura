"""Prove mTLS enforcement and database role restrictions against live services."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pyarrow.flight as flight
from sqlalchemy import create_engine, text

from dal_obscura.connectors import DalObscuraClient

sys.path.insert(0, "/example/scripts")
check_demo = importlib.import_module("check_demo")


def verify_database_roles() -> None:
    for variable, expected_user in (
        ("DATA_DATABASE_URL", "dal_obscura_reader"),
        ("CONTROL_DATABASE_URL", "dal_obscura_control"),
    ):
        engine = create_engine(os.environ[variable])
        try:
            with engine.connect() as connection:
                assert connection.scalar(text("SELECT current_user")) == expected_user
                assert not connection.scalar(
                    text("SELECT has_schema_privilege(current_user, 'public', 'CREATE')")
                ), "Application role can change schema"
                assert not connection.scalar(
                    text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
                ), "Application role is superuser"
                assert connection.scalar(
                    text("SELECT has_table_privilege(current_user, 'data_plane_tickets', 'INSERT')")
                ), "Ticket exchange grant missing"
                can_write_policy = connection.scalar(
                    text("SELECT has_table_privilege(current_user, 'policy_rules', 'UPDATE')")
                )
                assert can_write_policy == (expected_user == "dal_obscura_control"), (
                    "Data role can author policy"
                )
        finally:
            engine.dispose()
    print("Separate migration/control/data roles and restricted ticket writes verified")


def verify_mtls_rejection(
    name: str, *, certificate: str | None = None, ca: str = "/tls/ca.crt"
) -> None:
    access_token = check_demo.token("asset-owner")
    options = {"tls_root_certs": Path(ca).read_bytes()}
    if certificate:
        options.update(
            {
                "cert_chain": Path(f"/tls/{certificate}.crt").read_bytes(),
                "private_key": Path(f"/tls/{certificate}.key").read_bytes(),
            }
        )
    transport = flight.FlightClient(os.environ["FLIGHT_URI"], **options)
    try:
        with DalObscuraClient.from_flight_client(transport, auth_token=access_token) as client:
            try:
                client.read_table(
                    catalog="retail_demo", target="retail.customer_revenue", columns=["customer_id"]
                )
            except flight.FlightUnavailableError:
                # Require a positive control immediately after rejection. An
                # unavailable server or provider cannot satisfy this check.
                with check_demo.connect(access_token) as control:
                    rows = control.read_table(
                        catalog="retail_demo",
                        target="retail.customer_revenue",
                        columns=["customer_id"],
                    )
                    assert rows.num_rows == 4 and rows.column(0).null_count == 0
                print(f"mTLS rejected {name} as expected")
                return
        raise AssertionError(f"mTLS unexpectedly accepted {name}")
    finally:
        transport.close()


def main() -> None:
    # Positive governed reads establish a live, authenticated service first.
    # Network outages and unrelated backend failures never count as TLS rejection.
    check_demo.main()
    verify_mtls_rejection("missing client certificate")
    verify_mtls_rejection("untrusted client certificate", certificate="untrusted-client")
    verify_mtls_rejection(
        "untrusted server CA", certificate="client", ca="/tls/untrusted-client.crt"
    )
    verify_database_roles()


if __name__ == "__main__":
    main()
