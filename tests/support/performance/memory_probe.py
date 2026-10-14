"""Measure subprocess RSS from outside its GIL, after an explicit setup barrier."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import psutil


def begin_memory_probe() -> None:
    """Called by the child after fixture setup, immediately before measured work."""
    root = Path(os.environ["DAL_TEST_MEMORY_PROBE"])
    (root / "ready").touch()
    deadline = time.monotonic() + 30
    while not (root / "go").exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("Memory probe monitor did not start")
        time.sleep(0.005)


def run_memory_probe(script: str, arguments: list[str], *, timeout: float = 120) -> dict[str, Any]:
    """Sample child RSS every 5ms, including execution before its first output."""
    with tempfile.TemporaryDirectory(prefix="dal-memory-probe-") as directory:
        root = Path(directory)
        env = {**os.environ, "DAL_TEST_MEMORY_PROBE": directory}
        with subprocess.Popen(
            [sys.executable, "-c", script, *arguments],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        ) as child:
            process = psutil.Process(child.pid)
            stopped = threading.Event()
            measurements: dict[str, int] = {}
            errors: list[BaseException] = []

            def sample() -> None:
                try:
                    while not stopped.wait(0.005):
                        if not (root / "ready").exists():
                            continue
                        rss = process.memory_info().rss
                        if not measurements:
                            measurements.update(baseline_rss=rss, peak_rss=rss, rss_samples=0)
                            (root / "go").touch()
                        measurements["peak_rss"] = max(measurements["peak_rss"], rss)
                        measurements["rss_samples"] += 1
                except psutil.NoSuchProcess:
                    pass
                except BaseException as exc:
                    errors.append(exc)

            monitor = threading.Thread(target=sample, daemon=True)
            monitor.start()
            try:
                stdout, stderr = child.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                child.kill()
                child.communicate()
                raise
            finally:
                stopped.set()
                monitor.join(timeout=5)
            if errors:
                raise RuntimeError("Memory probe monitor failed") from errors[0]
            if child.returncode:
                raise AssertionError(
                    f"Memory probe failed ({child.returncode}):\n{stderr}\n{stdout}"
                )
            if not measurements:
                raise AssertionError("Memory probe never reached its measurement barrier")
            payload = json.loads(stdout.strip().splitlines()[-1])
            return {
                **payload,
                **measurements,
                "rss_delta": measurements["peak_rss"] - measurements["baseline_rss"],
            }
