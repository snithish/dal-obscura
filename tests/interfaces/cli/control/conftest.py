"""Own real control-plane subprocesses and their disposable socket endpoints."""

import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest


@pytest.fixture
def control_server(tmp_path):
    @contextmanager
    def start(environment):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        values = {
            **os.environ,
            **environment,
            "DAL_OBSCURA_CONTROL_PLANE_HOST": "127.0.0.1",
            "DAL_OBSCURA_CONTROL_PLANE_PORT": str(port),
        }
        url = f"http://127.0.0.1:{port}"
        with (tmp_path / "control-server.log").open("w+") as output:
            process = subprocess.Popen(
                [str(Path(sys.executable).with_name("dal-obscura-control-plane"))],
                env=values,
                cwd=tmp_path,
                stdout=output,
                stderr=output,
            )
            try:
                deadline = time.monotonic() + 15
                with httpx.Client(base_url=url, timeout=0.5, trust_env=False) as client:
                    while True:
                        if process.poll() is not None:
                            output.seek(0)
                            pytest.fail(
                                f"Control process exited {process.returncode}: {output.read()}"
                            )
                        try:
                            if client.get("/readyz").status_code == 200:
                                break
                        except httpx.TransportError:
                            pass
                        assert time.monotonic() < deadline, "Control server did not become ready"
                        time.sleep(0.02)
                    yield client
            finally:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    return start
