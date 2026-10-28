"""Small, shared benchmark harness; timings exclude memory tracing."""

from __future__ import annotations

import asyncio
import json
import platform
import sys
import tracemalloc
from collections.abc import Awaitable, Callable
from pathlib import Path
from statistics import median
from time import perf_counter_ns


def _result(label: str, timings: list[int], peak: int) -> dict[str, object]:
    ordered = sorted(timings)

    def percentile(fraction: float) -> float:
        return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))] / 1000

    return {
        "case": label,
        "iterations": len(timings),
        "ops_per_second": 1e9 / median(timings),
        "p50_us": percentile(0.50),
        "p95_us": percentile(0.95),
        "p99_us": percentile(0.99),
        "peak_traced_bytes": peak,
    }


def measure(label: str, operation: Callable[[], object], iterations: int) -> dict[str, object]:
    for _ in range(20):
        operation()
    timings = []
    for _ in range(iterations):
        started = perf_counter_ns()
        operation()
        timings.append(perf_counter_ns() - started)
    tracemalloc.start()
    try:
        for _ in range(min(iterations, 100)):
            operation()
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    return _result(label, timings, peak)


async def measure_async(
    label: str,
    operation: Callable[[], Awaitable[object]],
    iterations: int,
) -> dict[str, object]:
    for _ in range(20):
        await operation()
    timings = []
    for _ in range(iterations):
        started = perf_counter_ns()
        await operation()
        timings.append(perf_counter_ns() - started)
    tracemalloc.start()
    try:
        for _ in range(min(iterations, 100)):
            await operation()
        await asyncio.sleep(0)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    return _result(label, timings, peak)


def report(results: list[dict[str, object]], output: str | None) -> None:
    document = {"python": sys.version, "platform": platform.platform(), "results": results}
    payload = json.dumps(document, indent=2)
    if output:
        Path(output).write_text(payload + "\n", encoding="utf-8")
    print(payload)
