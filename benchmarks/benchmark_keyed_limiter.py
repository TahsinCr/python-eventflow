"""Measure same-key waiter handoff under queued async work.

Run with ``python -m benchmarks.benchmark_keyed_limiter``. Each worker yields
once while holding its permit so the benchmark measures repeated contention,
not only immediate acquire/release calls.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
from pathlib import Path
from time import perf_counter

from eventflow import KeyedConcurrencyLimiter


async def _sample(waiter_count: int) -> float:
    limiter = KeyedConcurrencyLimiter[str](1)
    holder = limiter.acquire("shared")
    await holder.__aenter__()
    arrivals = 0
    all_arrived = asyncio.Event()

    async def worker() -> None:
        nonlocal arrivals
        arrivals += 1
        if arrivals == waiter_count:
            all_arrived.set()
        async with limiter.acquire("shared"):
            await asyncio.sleep(0)

    tasks = [asyncio.create_task(worker()) for _ in range(waiter_count)]
    holder_released = False
    try:
        await asyncio.wait_for(all_arrived.wait(), timeout=10)
        if limiter.stats().waiters != waiter_count:
            raise RuntimeError("benchmark workers did not all reach the limiter queue")
        started = perf_counter()
        await holder.__aexit__(None, None, None)
        holder_released = True
        await asyncio.gather(*tasks)
        return perf_counter() - started
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if not holder_released:
            await holder.__aexit__(None, None, None)
        await limiter.aclose()


async def _run(waiter_counts: list[int], repeats: int) -> dict[str, object]:
    results = []
    for waiter_count in waiter_counts:
        samples = [await _sample(waiter_count) for _ in range(repeats)]
        results.append(
            {
                "waiters": waiter_count,
                "repeats": repeats,
                "sample_seconds": samples,
                "p50_seconds": statistics.median(samples),
            }
        )
    return {
        "benchmark": "keyed-concurrency-same-key-waiters",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "workload": "one permit; each waiter yields once while holding it",
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--waiters", type=int, nargs="+", default=[100, 1000, 5000])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.repeats < 1 or any(waiter_count < 1 for waiter_count in args.waiters):
        parser.error("waiter counts and repeats must be positive")

    report = asyncio.run(_run(args.waiters, args.repeats))
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
