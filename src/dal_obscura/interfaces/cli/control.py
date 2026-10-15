"""Control-plane HTTP process entry point."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence

from dal_obscura.bootstrap.control import run_control_plane


def main() -> None:
    """Runs the configured control-plane HTTP server."""

    raise SystemExit(run(argv=sys.argv[1:]))


def run(environment: Mapping[str, str] | None = None, argv: Sequence[str] | None = None) -> int:
    """Parse command flags and required environment, then enter owned startup."""
    _parser().parse_args(argv or ())
    values = os.environ if environment is None else environment
    database_url = _required(values, "DAL_OBSCURA_DATABASE_URL")
    admin_token = _required(values, "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN")
    if database_url is None or admin_token is None:
        return 2
    return run_control_plane(values, database_url, admin_token)


def _required(values: Mapping[str, str], name: str) -> str | None:
    value = values.get(name, "").strip() or None
    if value is None:
        print(f"{name} is required", file=sys.stderr)
    return value


def _parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="dal-obscura-control-plane",
        description="Start the dal-obscura control-plane HTTP server.",
    )
