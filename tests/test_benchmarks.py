"""M6 — correctness at scale for the benchmark generator/pipeline.

Only correctness is asserted (counts, dedupe, discrepancies, analytics
totals). Timing is NEVER asserted — timing assertions are flaky and the
measured numbers live in benchmarks/RESULTS.md with their conditions.
Smaller sizes than the full benchmark run keep the suite fast.
"""
from pathlib import Path

import pytest

pytest.importorskip("duckdb")

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))

from run_benchmarks import bench_size, generate_csv_bytes  # noqa: E402


def test_generate_csv_deterministic_and_sized():
    a = generate_csv_bytes(100)
    b = generate_csv_bytes(100)
    assert a == b  # seeded -> identical bytes
    lines = a.decode().splitlines()
    assert len(lines) == 101  # header + 100 rows
    assert lines[1].startswith("BENCH-000000,")


def test_bench_size_correctness_1k(tmp_path):
    r = bench_size(1000, tmp_path)
    assert r["rows"] == 1000
    assert r["discrepancies"] == 10  # every 100th of 1000 SKUs drifted
    assert r["total_value"] > 0


def test_bench_size_correctness_10k(tmp_path):
    r = bench_size(10000, tmp_path)
    assert r["rows"] == 10000
    assert r["discrepancies"] == 100
