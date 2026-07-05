"""Small observability helpers shared by service adapters.

Example:
    ```python
    rss = get_resident_memory_bytes()
    ```
"""

from __future__ import annotations

import psutil

_PROCESS = psutil.Process()


def get_resident_memory_bytes() -> int:
    """Returns resident set size for the current process in bytes."""
    return _PROCESS.memory_info().rss
