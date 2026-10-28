"""Measure unchanged bus dispatch, filtering, latency and traced peak allocations.

Run from the repository root: python -m benchmarks.benchmark_publish.
"""

from __future__ import annotations

import argparse
import asyncio

from eventflow import AsyncEventBus, DeliveryMode, EventBus

from ._measure import measure, measure_async, report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=2000)
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.iterations <= 0:
        parser.error("iterations must be positive")
    results = []

    def receive(_event: int) -> None:
        pass

    for subscribers in (0, 1, 10, 100):
        with EventBus[int]() as bus:
            for _ in range(subscribers):
                bus.subscribe(receive)
            results.append(measure(f"sync/{subscribers}", lambda: bus.publish(1), args.iterations))
    for kind in ("type", "predicate"):
        with EventBus[int]() as bus:
            for _ in range(10):
                if kind == "type":
                    bus.subscribe(receive, event_type=int)
                else:
                    bus.subscribe(receive, predicate=lambda event: event > 0)
            results.append(measure(f"sync/filter-{kind}", lambda: bus.publish(1), args.iterations))

    async def asynchronous() -> None:
        async def receive_async(_event: int) -> None:
            pass

        for mode in DeliveryMode:
            for subscribers in (0, 1, 10, 100):
                async with AsyncEventBus[int](delivery_mode=mode) as bus:
                    for _ in range(subscribers):
                        await bus.subscribe(receive_async)
                    results.append(
                        await measure_async(
                            f"async-{mode.value}/{subscribers}",
                            lambda: bus.publish(1),
                            args.iterations,
                        )
                    )

    asyncio.run(asynchronous())
    report(results, args.output)


if __name__ == "__main__":
    main()
