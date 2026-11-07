"""Measure the round-trip cost of EventFlow's thread-to-async bridges.

Run with ``python -m benchmarks.benchmark_bridges``. Results are local workload
measurements, not deployment guarantees.
"""

from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import Future
from queue import Queue
from threading import Thread

from eventflow import EventBus
from eventflow.bridges import AsyncEventQueue, LoopPortal

from ._measure import measure, measure_async, report


async def _noop() -> None:
    return None


async def _run(iterations: int, output: str | None) -> None:
    results = [await measure_async("async-direct-await", _noop, iterations)]
    with LoopPortal() as portal:
        results.append(
            measure(
                "loop-portal-roundtrip",
                lambda: portal.call(_noop, timeout=10),
                iterations,
            )
        )

    sequence = 0
    with EventBus[int]() as bus:
        queue = AsyncEventQueue(bus, capacity=1)
        async with queue:

            async def publish_and_receive() -> int:
                nonlocal sequence
                event = sequence
                sequence += 1
                bus.publish(event)
                received = await queue.receive()
                if received != event:
                    raise RuntimeError("bridge benchmark received an unexpected event")
                return received

            results.append(
                await measure_async(
                    "same-thread-sync-bus-to-async-queue", publish_and_receive, iterations
                )
            )

            requests: Queue[tuple[int, Future[None]] | None] = Queue()

            def publish_from_thread() -> None:
                while (request := requests.get()) is not None:
                    event, completed = request
                    try:
                        bus.publish(event)
                    except BaseException as error:
                        completed.set_exception(error)
                    else:
                        completed.set_result(None)

            producer = Thread(
                target=publish_from_thread,
                name="EventFlowBridgeBenchmark",
                daemon=True,
            )
            producer.start()

            async def publish_from_thread_and_receive() -> int:
                nonlocal sequence
                event = sequence
                sequence += 1
                completed: Future[None] = Future()
                asyncio.get_running_loop().call_soon(requests.put, (event, completed))
                received = await queue.receive()
                await asyncio.wrap_future(completed)
                if received != event:
                    raise RuntimeError("bridge benchmark received an unexpected event")
                return received

            try:
                results.append(
                    await measure_async(
                        "cross-thread-sync-bus-to-async-queue",
                        publish_from_thread_and_receive,
                        iterations,
                    )
                )
            finally:
                requests.put(None)
                producer.join(timeout=2)
                if producer.is_alive():
                    raise RuntimeError("bridge benchmark producer did not stop")

    report(results, output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("iterations must be a positive integer")

    asyncio.run(_run(args.iterations, args.output))


if __name__ == "__main__":
    main()
