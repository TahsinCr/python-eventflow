"""Benchmark same-key limiter handoff under queued asynchronous contention."""

from __future__ import annotations

import asyncio
from time import perf_counter

from eventflow import KeyedConcurrencyLimiter

from ._runner import bench_time_func, create_runner


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


def _time_waiters(loops: int, waiter_count: int) -> float:
    async def measure() -> float:
        elapsed = 0.0
        for _ in range(loops):
            elapsed += await _sample(waiter_count)
        return elapsed

    return asyncio.run(measure())


def main() -> None:
    runner = create_runner(__doc__ or "Keyed concurrency limiter benchmarks")
    runner.argparser.add_argument("--waiters", type=int, nargs="+", default=[100, 1000, 5000])
    args = runner.parse_args()
    if any(waiter_count < 1 for waiter_count in args.waiters):
        runner.argparser.error("waiter counts must be positive")
    if len(set(args.waiters)) != len(args.waiters):
        runner.argparser.error("waiter counts must be unique")

    for waiter_count in args.waiters:
        bench_time_func(
            runner,
            f"same-key-waiters/{waiter_count}",
            _time_waiters,
            waiter_count,
            inner_loops=waiter_count,
            metadata={"waiters": waiter_count},
        )


if __name__ == "__main__":
    main()
