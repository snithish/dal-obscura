from __future__ import annotations

import argparse

from read_demo import USERS, _token


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a Keycloak access token for demo CLI reads."
    )
    parser.add_argument("--as", dest="username", choices=sorted(USERS), default="asset-owner")
    args = parser.parse_args()
    print(_token(args.username))


if __name__ == "__main__":
    main()
