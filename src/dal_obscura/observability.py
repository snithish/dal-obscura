from __future__ import annotations

import psutil

_PROCESS = psutil.Process()


def get_resident_memory_bytes() -> int:
    return _PROCESS.memory_info().rss
