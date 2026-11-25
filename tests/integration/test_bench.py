"""Task 7: the benchmark runs end to end. Real numbers: `python -m safety_gate.bench`."""
import pytest

from safety_gate import bench
from safety_gate.config import Settings

pytestmark = pytest.mark.integration


def test_benchmark_reports_throughput_and_latency(bootstrap, capsys):
    result = bench.run_once(Settings(bootstrap=bootstrap), batch_size=100, messages=500, rate=50, seconds=3)

    with capsys.disabled():
        print(f"\nbenchmark (small smoke run, not a headline number): {result}")
    assert result["throughput_per_s"] > 0
    assert result["latency_samples"] == result["latency_rate_per_s"] * 3
    assert 0 < result["p50_ms"] <= result["p95_ms"] <= result["p99_ms"]
