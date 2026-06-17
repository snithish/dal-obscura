from __future__ import annotations

import os
import subprocess
import sys

USERS = ("us-analyst", "eu-analyst", "data-steward", "asset-owner", "blocked-user")
TARGETS = (
    ("DEMO_ICEBERG_CATALOG", "DEMO_ICEBERG_TARGET"),
    ("DEMO_DELTA_CATALOG", "DEMO_DELTA_TARGET"),
)


def main() -> None:
    for catalog_env, target_env in TARGETS:
        catalog = os.environ[catalog_env]
        target = os.environ[target_env]
        for username in USERS:
            subprocess.run(
                [
                    sys.executable,
                    "/workspace/demo/scripts/read_demo.py",
                    "--as",
                    username,
                    "--catalog",
                    catalog,
                    "--target",
                    target,
                ],
                check=True,
            )


if __name__ == "__main__":
    main()
