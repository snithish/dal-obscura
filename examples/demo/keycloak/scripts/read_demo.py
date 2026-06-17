from __future__ import annotations

import argparse
import json
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pyarrow.flight as flight

USERS = {
    "demo-admin": "DEMO_ADMIN_PASSWORD",
    "asset-owner": "ASSET_OWNER_PASSWORD",
    "us-analyst": "US_ANALYST_PASSWORD",
    "eu-analyst": "EU_ANALYST_PASSWORD",
    "data-steward": "DATA_STEWARD_PASSWORD",
    "blocked-user": "BLOCKED_USER_PASSWORD",
}
EXPECTED_ROWS = {
    "us-analyst": 2,
    "eu-analyst": 2,
    "data-steward": 4,
    "asset-owner": 4,
    "demo-admin": 0,
    "blocked-user": 0,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a demo dal-obscura read.")
    parser.add_argument("--as", dest="username", choices=sorted(USERS), default="us-analyst")
    parser.add_argument(
        "--columns",
        default="customer_id,customer_name,email,region,annual_revenue",
    )
    parser.add_argument("--catalog", default=os.environ.get("DEMO_CATALOG", "retail_demo"))
    parser.add_argument(
        "--target",
        default=os.environ.get("DEMO_TARGET", "retail.customer_revenue"),
    )
    parser.add_argument("--row-filter", default=None)
    args = parser.parse_args()

    token = _token(args.username)
    columns = [column.strip() for column in args.columns.split(",") if column.strip()]
    try:
        table = _read_table(token, args.catalog, args.target, columns, args.row_filter)
    except Exception as exc:
        if args.username == "blocked-user":
            print("blocked-user: access denied as expected")
            return
        raise SystemExit(f"{args.username}: read failed: {exc}") from exc

    expected = EXPECTED_ROWS[args.username]
    if table.num_rows != expected:
        raise SystemExit(f"{args.username}: expected {expected} rows, read {table.num_rows}")
    print(f"{args.username}: read {table.num_rows} rows")
    print(json.dumps(table.to_pylist(), indent=2, sort_keys=True))


def _token(username: str) -> str:
    body = urlencode(
        {
            "grant_type": "password",
            "client_id": os.environ["OIDC_CLIENT_ID"],
            "client_secret": os.environ["OIDC_CLI_CLIENT_SECRET"],
            "username": username,
            "password": os.environ[USERS[username]],
        }
    ).encode()
    request = Request(
        os.environ["OIDC_TOKEN_URL"],
        data=body,
        headers={"content-type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return str(payload["access_token"])


def _read_table(
    token: str,
    catalog: str,
    target: str,
    columns: list[str],
    row_filter: str | None,
):
    client = flight.connect(os.environ["FLIGHT_URI"])
    payload: dict[str, object] = {
        "catalog": catalog,
        "target": target,
        "columns": columns,
    }
    if row_filter:
        payload["row_filter"] = row_filter
    options = flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {token}".encode())])
    info = client.get_flight_info(
        flight.FlightDescriptor.for_command(json.dumps(payload)),
        options=options,
    )
    return client.do_get(info.endpoints[0].ticket, options=options).read_all()


if __name__ == "__main__":
    main()
