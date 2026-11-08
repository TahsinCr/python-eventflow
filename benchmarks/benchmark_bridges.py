"""Benchmark same-thread and cross-thread delivery to an asyncio consumer."""

from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import Future
from queue import Queue
from threading import Thread
from time import perf_counter

from eventflow import EventBus
from eventflow.bridges import AsyncEventQueue, LoopPortal
from eventflow.errors import EventQueueClosedError

from ._runner import bench_async, bench_sync, bench_time_func, create_runner


async def _noop() -> None:
    return None


async def _queue_roundtrip_batch(loops: int, *, cross_thread: bool) -> float:
    with EventBus[int]() as bus:
        queue = AsyncEventQueue(bus, capacity=1)
        async with queue:
            loop = asyncio.get_running_loop()
            acknowledgements: deque[asyncio.Future[int]] = deque()
            consumer_ready = asyncio.Event()

            async def consume() -> None:
                consumer_ready.set()
                try:
                    while True:
                        received = await queue.receive()
                        acknowledgement = acknowledgements.popleft()
                        if not acknowledgement.done():
                            acknowledgement.set_result(received)
                except EventQueueClosedError:
                    return

            consumer = asyncio.create_task(consume())
            await consumer_ready.wait()

            if not cross_thread:
                started = perf_counter()
                for event in range(loops):
                    acknowledgement = loop.create_future()
                    acknowledgements.append(acknowledgement)
                    bus.publish(event)
                    if await acknowledgement != event:
                        raise RuntimeError("bridge delivered an unexpected event")
                elapsed = perf_counter() - started
            else:
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
                started = perf_counter()
                try:
                    for event in range(loops):
                        acknowledgement = loop.create_future()
                        acknowledgements.append(acknowledgement)
                        completed: Future[None] = Future()
                        requests.put((event, completed))
                        await asyncio.wrap_future(completed)
                        if await acknowledgement != event:
                            raise RuntimeError("bridge delivered an unexpected event")
                    elapsed = perf_counter() - started
                finally:
                    requests.put(None)
                    producer.join(timeout=2)
                    if producer.is_alive():
                        raise RuntimeError("bridge benchmark publisher did not stop")

        await consumer
    return elapsed


def _time_queue_roundtrip(loops: int, cross_thread: bool) -> float:
    return asyncio.run(_queue_roundtrip_batch(loops, cross_thread=cross_thread))


def main() -> None:
    runner = create_runner(__doc__ or "Thread-to-async bridge benchmarks")
    runner.parse_args()

    bench_async(runner, "async-direct-await", _noop)

    with LoopPortal() as portal:
        bench_sync(
            runner,
            "loop-portal-roundtrip",
            lambda: portal.call(_noop, timeout=10),
        )

    for cross_thread, name in (
        (False, "same-thread-waiting-consumer-roundtrip"),
        (True, "cross-thread-waiting-consumer-roundtrip"),
    ):
        bench_time_func(
            runner,
            name,
            _time_queue_roundtrip,
            cross_thread,
            metadata={"publisher_thread": "cross" if cross_thread else "same"},
        )


if __name__ == "__main__":
    main()
