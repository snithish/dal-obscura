from __future__ import annotations

import os
import sys
from io import BytesIO
from subprocess import TimeoutExpired
from types import ModuleType, SimpleNamespace

import pytest
from dal_obscura_plugin_conformance.native_delta import delta_writer


@pytest.mark.parametrize("body_fails", [False, True])
@pytest.mark.parametrize("stop_error", [RuntimeError, KeyboardInterrupt])
def test_delta_writer_releases_gateway_and_environment_when_stop_fails(
    monkeypatch, body_fails, stop_error
):
    calls = []

    class Builder:
        def master(self, value):
            return self

        def appName(self, value):
            return self

        def config(self, key, value):
            return self

        def getOrCreate(self):
            return SimpleNamespace(
                sparkContext=SimpleNamespace(setLogLevel=lambda level: None), stop=stop
            )

    def stop():
        calls.append("stop")
        raise stop_error("stop sentinel")

    class BrokenPipe:
        def close(self):
            calls.append("stdin")
            raise RuntimeError("pipe close sentinel")

    class Process:
        stdin = BrokenPipe()
        stdout = BytesIO()
        stderr = BytesIO()
        waited = False

        def wait(self, timeout):
            calls.append("wait")
            if not self.waited:
                self.waited = True
                raise TimeoutExpired("spark", timeout)
            return 0

        def terminate(self):
            calls.append("terminate")

    class Gateway:
        def shutdown(self):
            calls.append("shutdown")

        proc = Process()

    context = SimpleNamespace(_active_spark_context=None, _gateway=Gateway(), _jvm=object())
    pyspark = ModuleType("pyspark")
    monkeypatch.setattr(pyspark, "SparkContext", context, raising=False)
    sql = ModuleType("pyspark.sql")
    monkeypatch.setattr(sql, "SparkSession", SimpleNamespace(builder=Builder()), raising=False)
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setenv("SPARK_LOCAL_IP", "original-host")

    error = ValueError if body_fails else stop_error
    message = "body sentinel" if body_fails else "stop sentinel"
    with pytest.raises(error, match=message), delta_writer():
        if body_fails:
            raise ValueError("body sentinel")

    assert calls == ["stop", "shutdown", "wait", "terminate", "wait", "stdin"]
    assert context._gateway is None
    assert Gateway.proc.stdout.closed
    assert Gateway.proc.stderr.closed
    assert os.environ["SPARK_LOCAL_IP"] == "original-host"
    assert context._jvm is None
