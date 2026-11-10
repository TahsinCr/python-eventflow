"""Benchmark keyed rate limiting with contending and cancelled waiters."""

from __future__ import annotations

import argparse
import asyncio
import math
from time import perf_counter

from eventflow import KeyedRateLimiter

from ._runner import bench_time_func, create_runner, forward_cli_arguments
from ._support import positive_int


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


async def _cancel_waiters(count: int) -> None:
    if not count:
        return
    limiter = KeyedRateLimiter[str](rate=1, period=3_600, burst=1)
    if not limiter.try_acquire("shared"):
        raise RuntimeError("could not reserve the cancellation probe token")
    tasks = [asyncio.create_task(limiter.acquire("shared")) for _ in range(count)]
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10
        while limiter.stats().waiters < count:
            if loop.time() >= deadline:
                raise TimeoutError("cancellation probe workers did not reach the wait queue")
            await asyncio.sleep(0)
        for task in tasks:
            task.cancel()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        if any(not isinstance(outcome, asyncio.CancelledError) for outcome in outcomes):
            raise RuntimeError("cancellation probe did not cancel every queued worker")
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await limiter.aclose()


async def _sample(workers: int, acquisitions: int, rate: float, cancel_count: int) -> float:
    await _cancel_waiters(cancel_count)
    limiter = KeyedRateLimiter[str](rate=rate, period=1, burst=1)
    if not limiter.try_acquire("shared"):
        raise RuntimeError("could not reserve the initial rate-limit token")

    active_workers = workers - cancel_count
    arrivals = 0
    ready = asyncio.Event()
    start = asyncio.Event()

    async def worker() -> None:
        nonlocal arrivals
        arrivals += 1
        if arrivals == active_workers:
            ready.set()
        await start.wait()
        for _ in range(acquisitions):
            await limiter.acquire("shared")

    tasks = [asyncio.create_task(worker()) for _ in range(active_workers)]
    try:
        await ready.wait()
        started = perf_counter()
        start.set()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise RuntimeError("rate-limiter benchmark worker failed") from outcome
        return perf_counter() - started
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await limiter.aclose()


def _time_rate_limiter(
    loops: int,
    workers: int,
    acquisitions: int,
    rate: float,
    cancel_count: int,
) -> float:
    async def measure() -> float:
        elapsed = 0.0
        for _ in range(loops):
            elapsed += await _sample(workers, acquisitions, rate, cancel_count)
        return elapsed

    return asyncio.run(measure())


def main() -> None:
    runner = create_runner(
        __doc__ or "Keyed rate limiter benchmarks",
        add_cmdline_args=forward_cli_arguments(
            "workers", "acquisitions", "rate", "cancel_fraction"
        ),
    )
    runner.argparser.add_argument("--workers", type=positive_int, default=32)
    runner.argparser.add_argument("--acquisitions", type=positive_int, default=16)
    runner.argparser.add_argument("--rate", type=_rate, default=1_000.0)
    runner.argparser.add_argument("--cancel-fraction", type=_cancellation_fraction, default=0.05)
    args = runner.parse_args()

    cancel_count = min(round(args.workers * args.cancel_fraction), args.workers - 1)
    active_acquisitions = (args.workers - cancel_count) * args.acquisitions

    bench_time_func(
        runner,
        "same-key-rate-limit-contention",
        _time_rate_limiter,
        args.workers,
        args.acquisitions,
        args.rate,
        cancel_count,
        inner_loops=active_acquisitions,
        metadata={
            "workers": args.workers,
            "acquisitions_per_worker": args.acquisitions,
            "rate_per_second": args.rate,
            "cancelled_workers": cancel_count,
        },
    )


if __name__ == "__main__":
    main()
