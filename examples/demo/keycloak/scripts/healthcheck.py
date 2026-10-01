"""Portable readiness probe: avoid shell quoting differences in container engines."""

from __future__ import annotations

import json
import sys
from urllib.request import urlopen

with urlopen(sys.argv[1], timeout=2) as response:
    if response.status != 200 or json.loads(response.read()).get("status") != "ready":
        raise SystemExit(1)
