"""Measure worker-backed coalescing from updates through flush and close.

Run with ``python -m benchmarks.benchmark_coalescer``. Results are local
workload measurements, not deployment guarantees.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from statistics import median
from time import perf_counter

from eventflow import KeyedCoalescer


def _positive(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a positive integer") from exc
    if result < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return result


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def _sample(
    key_count: int,
    *,
    producers: int,
    updates_per_key: int,
    flush_callers: int,
) -> dict[str, float]:
    delivered = 0

    def callback(_value: tuple[int, int]) -> None:
        nonlocal delivered
        delivered += 1

    coalescer = KeyedCoalescer(
        interval=3600,
        key=lambda item: item[0],
        merge=lambda first, second: (first[0], first[1] + second[1]),
        callback=callback,
        max_pending_keys=key_count,
    )
    started = perf_counter()
    try:
        if producers == 1:
            for key in range(key_count):
                for _ in range(updates_per_key):
                    coalescer.push((key, 1))
        else:
            barrier = threading.Barrier(producers + 1)

            def push_partition(producer: int) -> None:
                barrier.wait(timeout=10)
                for key in range(producer, key_count, producers):
                    for _ in range(updates_per_key):
                        coalescer.push((key, 1))

            with ThreadPoolExecutor(max_workers=producers) as executor:
                futures = [executor.submit(push_partition, index) for index in range(producers)]
                barrier.wait(timeout=10)
                for future in futures:
                    future.result(timeout=60)
        push_seconds = perf_counter() - started

        drain_started = perf_counter()
        if flush_callers == 1:
            coalescer.flush(timeout=60)
        else:
            barrier = threading.Barrier(flush_callers + 1)

            def flush_pending() -> None:
                barrier.wait(timeout=10)
                coalescer.flush(timeout=60)

            with ThreadPoolExecutor(max_workers=flush_callers) as executor:
                futures = [executor.submit(flush_pending) for _ in range(flush_callers)]
                barrier.wait(timeout=10)
                for future in futures:
                    future.result(timeout=60)
        drain_seconds = perf_counter() - drain_started
        coalescer.close(timeout=60)
        total_seconds = perf_counter() - started

        stats = coalescer.stats()
        expected_merges = key_count * (updates_per_key - 1)
        if delivered != key_count or stats.merged != expected_merges:
            raise RuntimeError("coalescer benchmark lost or duplicated work")
        return {
            "push_seconds": push_seconds,
            "drain_seconds": drain_seconds,
            "total_seconds": total_seconds,
        }
    finally:
        coalescer.close(flush=False, timeout=60)


def _measure_case(
    name: str,
    key_count: int,
    *,
    samples: int,
    producers: int,
    updates_per_key: int,
    flush_callers: int,
) -> dict[str, object]:
    timings = [
        _sample(
            key_count,
            producers=producers,
            updates_per_key=updates_per_key,
            flush_callers=flush_callers,
        )
        for _ in range(samples)
    ]
    totals = [sample["total_seconds"] for sample in timings]
    return {
        "case": name,
        "keys": key_count,
        "pushes_per_key": updates_per_key,
        "producers": producers,
        "flush_callers": flush_callers,
        "samples": samples,
        "median_push_seconds": median(sample["push_seconds"] for sample in timings),
        "median_drain_seconds": median(sample["drain_seconds"] for sample in timings),
        "median_total_seconds": median(totals),
        "p95_total_seconds": _percentile(totals, 0.95),
        "p99_total_seconds": _percentile(totals, 0.99),
        "sample_total_seconds": totals,
    }


def _run(
    key_counts: list[int], samples: int, producers: int, merge_updates: int
) -> dict[str, object]:
    results = []
    for key_count in key_counts:
        cases = (
            ("single-producer", 1, 1, 1),
            ("multi-producer", min(producers, key_count), 1, 1),
            ("merge-heavy", 1, merge_updates, 1),
            ("concurrent-flush", 1, 1, 2),
        )
        for name, producer_count, updates, flush_callers in cases:
            results.append(
                _measure_case(
                    name,
                    key_count,
                    samples=samples,
                    producers=producer_count,
                    updates_per_key=updates,
                    flush_callers=flush_callers,
                )
            )
    return {
        "benchmark": "keyed-coalescer-worker-lifecycle",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "workload": (
            "real worker thread; unique or merged keyed updates; flush and close included; "
            "no-op callback"
        ),
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--key-counts", type=_positive, nargs="+", default=[128, 1024, 4096, 16384])
    parser.add_argument("--samples", type=_positive, default=5)
    parser.add_argument("--producers", type=_positive, default=4)
    parser.add_argument("--merge-updates", type=_positive, default=8)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = _run(args.key_counts, args.samples, args.producers, args.merge_updates)
    rendered = json.dumps(result, indent=2) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
