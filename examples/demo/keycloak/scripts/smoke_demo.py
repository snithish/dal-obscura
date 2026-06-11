from __future__ import annotations

import subprocess
import sys

USERS = ("us-analyst", "eu-analyst", "data-steward", "asset-owner", "blocked-user")


def main() -> None:
    for username in USERS:
        subprocess.run(
            [
                sys.executable,
                "/workspace/demo/scripts/read_demo.py",
                "--as",
                username,
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
