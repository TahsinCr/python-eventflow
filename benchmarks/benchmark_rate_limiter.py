"""Measure same-key rate-limiter wait distributions under sustained demand.

Run with ``python -m benchmarks.benchmark_rate_limiter``. FIFO ordering is not
promised; these samples expose observed wait and cancellation distributions.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import platform
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import TypedDict

from eventflow import KeyedRateLimiter


class _WorkerResult(TypedDict):
    elapsed_seconds: float
    wait_seconds: list[float]


class _Sample(TypedDict):
    completed_workers: int
    cancelled_workers: int
    completed_acquisitions: int
    acquisition_wait_seconds: list[float]
    worker_completion_seconds: list[float]


def _positive_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a positive integer") from exc
    if result < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return result


def _rate(value: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("rate must be a positive finite number") from exc
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("rate must be a positive finite number")
    return result


def _cancellation_fraction(value: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("cancel fraction must be in [0, 1)") from exc
    if not math.isfinite(result) or not 0 <= result < 1:
        raise argparse.ArgumentTypeError("cancel fraction must be in [0, 1)")
    return result


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


async def _sample(workers: int, acquisitions: int, rate: float, cancel_count: int) -> _Sample:
    limiter = KeyedRateLimiter[str](rate=rate, period=1, burst=1)
    if not limiter.try_acquire("shared"):
        raise RuntimeError("could not reserve the initial rate-limit token")

    arrivals = 0
    all_arrived = asyncio.Event()
    loop = asyncio.get_running_loop()

    async def worker() -> _WorkerResult:
        nonlocal arrivals
        arrivals += 1
        if arrivals == workers:
            all_arrived.set()
        wait_times = []
        started = perf_counter()
        for _ in range(acquisitions):
            wait_started = perf_counter()
            await limiter.acquire("shared")
            wait_times.append(perf_counter() - wait_started)
        return {"elapsed_seconds": perf_counter() - started, "wait_seconds": wait_times}

    tasks = [asyncio.create_task(worker()) for _ in range(workers)]
    try:
        await asyncio.wait_for(all_arrived.wait(), timeout=10)
        deadline = loop.time() + 10
        while limiter.stats().waiters < workers:
            if loop.time() >= deadline:
                raise TimeoutError("rate-limiter workers did not all reach the wait queue")
            await asyncio.sleep(0)

        for task in tasks[-cancel_count:] if cancel_count else ():
            task.cancel()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        completed = []
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                if not isinstance(outcome, asyncio.CancelledError):
                    raise RuntimeError(f"rate-limiter worker failed: {outcome!r}")
            else:
                completed.append(outcome)
        cancelled = sum(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
        wait_times = [wait for outcome in completed for wait in outcome["wait_seconds"]]
        worker_times = [outcome["elapsed_seconds"] for outcome in completed]
        if not wait_times or not worker_times:
            raise RuntimeError("rate-limiter sample produced no completed acquisitions")
        return {
            "completed_workers": len(completed),
            "cancelled_workers": cancelled,
            "completed_acquisitions": len(wait_times),
            "acquisition_wait_seconds": wait_times,
            "worker_completion_seconds": worker_times,
        }
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await limiter.aclose()


async def _run(
    workers: int,
    acquisitions: int,
    rate: float,
    repeats: int,
    cancel_fraction: float,
) -> dict[str, object]:
    total_cancelled = min(round(workers * repeats * cancel_fraction), (workers - 1) * repeats)
    cancellations_per_sample = [
        total_cancelled // repeats + (index < total_cancelled % repeats) for index in range(repeats)
    ]
    samples = [
        await _sample(workers, acquisitions, rate, cancel_count)
        for cancel_count in cancellations_per_sample
    ]
    waits = [
        value
        for sample in samples
        for value in sample["acquisition_wait_seconds"]
        if isinstance(value, float)
    ]
    worker_times = [
        value
        for sample in samples
        for value in sample["worker_completion_seconds"]
        if isinstance(value, float)
    ]
    cancelled = sum(int(sample["cancelled_workers"]) for sample in samples)
    task_count = workers * repeats
    return {
        "benchmark": "keyed-rate-limiter-wait-fairness",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "workload": "same key; one reserved token; each surviving worker reacquires repeatedly",
        "workers_per_sample": workers,
        "acquisitions_per_worker": acquisitions,
        "rate_per_second": rate,
        "repeats": repeats,
        "requested_cancel_fraction": cancel_fraction,
        "observed_cancel_fraction": cancelled / task_count,
        "completed_acquisitions": len(waits),
        "acquisition_wait_seconds": {
            "p50": median(waits),
            "p95": _percentile(waits, 0.95),
            "p99": _percentile(waits, 0.99),
            "max": max(waits),
        },
        "worker_completion_seconds": {
            "p50": median(worker_times),
            "p95": _percentile(worker_times, 0.95),
            "p99": _percentile(worker_times, 0.99),
            "max": max(worker_times),
            "max_to_median_ratio": max(worker_times) / median(worker_times),
        },
        "sample_completed_acquisitions": [sample["completed_acquisitions"] for sample in samples],
        "sample_cancelled_workers": [sample["cancelled_workers"] for sample in samples],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=_positive_int, default=32)
    parser.add_argument("--acquisitions", type=_positive_int, default=16)
    parser.add_argument("--rate", type=_rate, default=1000.0)
    parser.add_argument("--repeats", type=_positive_int, default=5)
    parser.add_argument("--cancel-fraction", type=_cancellation_fraction, default=0.05)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = asyncio.run(
        _run(args.workers, args.acquisitions, args.rate, args.repeats, args.cancel_fraction)
    )
    rendered = json.dumps(result, indent=2) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
