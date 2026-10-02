"""Export the served API contract used by generated customer and UI clients.

Run from the repository root with ``uv run scripts/export_control_plane_openapi.py``.
No configured database or credentials are needed; schema generation performs no I/O.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dal_obscura.interfaces.http.app import create_app
from dal_obscura.storage.database.db import create_engine_from_url, session_factory


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Fail if the checked-in contract is stale"
    )
    args = parser.parse_args()
    path = Path(__file__).resolve().parents[1] / "apps/governance-ui/openapi/control-plane.json"
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    try:
        schema = create_app(session_factory(engine), admin_token="schema-export-only").openapi()
    finally:
        engine.dispose()
    if args.check:
        if json.loads(path.read_text()) != schema:
            raise SystemExit(
                "OpenAPI snapshot is stale; run scripts/export_control_plane_openapi.py"
            )
    else:
        path.write_text(json.dumps(schema, indent=2) + "\n")


if __name__ == "__main__":
    main()
