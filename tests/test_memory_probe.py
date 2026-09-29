from tests.support.memory_probe import run_memory_probe


def test_memory_probe_sees_allocations_before_any_output():
    result = run_memory_probe(
        """
import json
import time
from tests.support.memory_probe import begin_memory_probe
begin_memory_probe()
payload = bytearray(32 * 1024 * 1024)
time.sleep(0.1)
del payload
print(json.dumps({"finished": True}))
""",
        [],
    )
    assert result["finished"]
    assert result["rss_samples"] > 2
    assert result["rss_delta"] >= 24 * 1024 * 1024
