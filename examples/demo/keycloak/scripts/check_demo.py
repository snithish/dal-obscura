"""Verify the actual local services; unrelated failures never count as denial."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pyarrow.flight as flight

from dal_obscura.connectors import DalObscuraClient

USERS = {
    "demo-admin": "DEMO_ADMIN_PASSWORD",
    "asset-owner": "ASSET_OWNER_PASSWORD",
    "us-analyst": "US_ANALYST_PASSWORD",
    "eu-analyst": "EU_ANALYST_PASSWORD",
    "data-steward": "DATA_STEWARD_PASSWORD",
    "blocked-user": "BLOCKED_USER_PASSWORD",
}


@contextmanager
def connect(auth_token: str):
    uri = os.environ["FLIGHT_URI"]
    ca = os.environ.get("FLIGHT_TLS_CA")
    if not ca:
        with DalObscuraClient(uri, auth_token=auth_token) as client:
            yield client
        return
    transport = flight.FlightClient(
        uri,
        tls_root_certs=Path(ca).read_bytes(),
        cert_chain=Path(os.environ["FLIGHT_TLS_CERT"]).read_bytes(),
        private_key=Path(os.environ["FLIGHT_TLS_KEY"]).read_bytes(),
    )
    try:
        with DalObscuraClient.from_flight_client(transport, auth_token=auth_token) as client:
            yield client
    finally:
        transport.close()


def token(username: str) -> str:
    request = Request(
        os.environ["OIDC_TOKEN_URL"],
        method="POST",
        data=urlencode(
            {
                "grant_type": "password",
                "client_id": os.environ["OIDC_CLIENT_ID"],
                "client_secret": os.environ["OIDC_CLI_CLIENT_SECRET"],
                "username": username,
                "password": os.environ[USERS[username]],
            }
        ).encode(),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    with urlopen(request, timeout=15) as response:
        return json.loads(response.read())["access_token"]


def verify_denied(
    client: DalObscuraClient, catalog: str, target: str, expected_rows: int = 4
) -> None:
    # The product's no-grant contract is typed NULL masking, not HTTP/Flight
    # rejection. Require a successful response with no disclosed field values.
    table = client.read_table(catalog=catalog, target=target, columns=["customer_id", "email"])
    assert table.num_rows == expected_rows, "Unexpected no-grant row count"
    assert all(value is None for row in table.to_pylist() for value in row.values()), (
        "Unpermitted reader unexpectedly obtained data."
    )


def _null_leaves(value: object) -> object:
    if isinstance(value, dict):
        return {key: _null_leaves(child) for key, child in value.items()}
    return None


def main() -> None:
    fixture = json.loads(
        (Path(__file__).resolve().parents[1] / "fixtures/demo_fixture.json").read_text()
    )
    for table in fixture["tables"]:
        _check_table(table)
    print(
        "Governed reads passed: all tickets, masks, row filters, nested values, "
        "no-grant readers, and authentication rejection."
    )


def _check_table(table: dict) -> None:
    catalog, target = table["catalog"], table["target"]
    for username in ("us-analyst", "eu-analyst", "data-steward", "asset-owner"):
        with connect(token(username)) as client:
            columns = ["customer_id", "email", "region", "profile"]
            if username in {"asset-owner", "data-steward"}:
                columns.extend(["contacts", "contact_book"])
            actual = client.read_table(
                catalog=catalog,
                target=target,
                columns=columns,
            )
            region = {"us-analyst": "us", "eu-analyst": "eu"}.get(username)
            expected = [row for row in table["rows"] if region is None or row["region"] == region]
            assert actual.num_rows == len(expected), f"{username}: unexpected row count"
            expected_by_id = {row["customer_id"]: row for row in expected}
            for row in actual.to_pylist():
                source = expected_by_id[row["customer_id"]]
                email = source["email"]
                expected_email = email[0] + "***@" + email.split("@")[1] if region else email
                assert row["email"] == expected_email, f"{username}: incorrect email masking"
                assert row["region"] == source["region"], f"{username}: wrong row filter"
                assert row["profile"] == (
                    _null_leaves(source["profile"]) if region else source["profile"]
                ), f"{username}: nested permission mismatch: {row['profile']!r}"
                if region is None:
                    assert row["contacts"] == source["contacts"], "Nested list mismatch"
                    assert dict(row["contact_book"]) == source["contact_book"], (
                        "Nested map mismatch"
                    )
            if username == "asset-owner":
                plan = client.plan(catalog=catalog, target=target, columns=["customer_id"])
                assert len(plan.endpoints) >= 2, "Expected parallel file tickets"
            print(f"{username}: {actual.num_rows} rows verified")
    for username in ("blocked-user", "demo-admin"):
        with connect(token(username)) as client:
            verify_denied(client, catalog, target, len(table["rows"]))
        print(f"{username}: all requested values NULL-masked as expected")
    with connect("invalid.local.token") as client:
        try:
            client.read_table(catalog=catalog, target=target, columns=["customer_id"])
        except (flight.FlightUnauthenticatedError, flight.FlightUnauthorizedError):
            pass
        else:
            raise AssertionError("Unauthenticated request unexpectedly succeeded")


if __name__ == "__main__":
    main()
