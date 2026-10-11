"""Internal container commands for static inspection and per-worker verification."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from urllib.request import urlopen

from dal_obscura.sources.builtins import create_builtin_plugin_registry
from dal_obscura.sources.plugins import load_plugin_lock_file
from dal_obscura.sources.plugins.inspection import inspect_distributions


def main() -> None:
    if sys.argv[1] == "inspect":
        print(json.dumps(inspect_distributions(sys.argv[2:]), sort_keys=True))
        return
    if sys.argv[1] != "verify":
        raise ValueError("Unknown image tooling command")
    path = Path(os.environ["DAL_OBSCURA_PLUGIN_LOCK_FILE"])
    if hashlib.sha256(path.read_bytes()).hexdigest() != sys.argv[3]:
        raise RuntimeError("Worker admission lock differs from selected release")
    lock = load_plugin_lock_file(path)
    admitted = create_builtin_plugin_registry(allowlist=lock).admitted()
    if not set(lock).issubset(admitted):
        raise RuntimeError("Worker is missing a selected plugin")
    variable = (
        "DAL_OBSCURA_CONTROL_PLANE_PORT"
        if sys.argv[2] == "control-plane"
        else "DAL_OBSCURA_DATA_PLANE_HEALTH_PORT"
    )
    port = int(os.environ.get(variable, "8820" if sys.argv[2] == "control-plane" else "8816"))
    with urlopen(f"http://127.0.0.1:{port}/readyz", timeout=10) as response:
        if json.load(response).get("status") != "ready":
            raise RuntimeError("Worker is not ready")
    print(json.dumps({"plugins": len(lock), "status": "verified"}))


if __name__ == "__main__":
    main()
