"""Bounded command execution and atomic operator workspace files."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any


def run(arguments: list[str], *, capture: bool = False) -> str:
    """Run argument vectors without a shell; never print captured service credentials."""
    result = subprocess.run(arguments, check=True, text=True, capture_output=capture, timeout=900)
    return result.stdout.strip() if capture else ""


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def write_json(path: Path, value: dict[str, Any], *, replace: bool = True) -> None:
    """Replace local drafts atomically; leave existing state intact on failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            if replace:
                os.replace(temporary, path)
            else:
                os.link(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
