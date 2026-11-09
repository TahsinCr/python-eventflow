"""Benchmark same-thread and cross-thread delivery to an asyncio consumer."""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
from threading import Event as ThreadEvent
from threading import Thread
from time import perf_counter

from eventflow import EventBus
from eventflow.bridges import AsyncEventQueue, LoopPortal
from eventflow.errors import EventQueueClosedError

from ._runner import bench_async, bench_sync, bench_time_func, create_runner


async def _noop() -> None:
    return None


async def _queue_roundtrip_batch(loops: int, *, cross_thread: bool) -> float:
    with EventBus[tuple[int, asyncio.Future[int] | ThreadEvent]]() as bus:
        queue = AsyncEventQueue(bus, capacity=1)
        loop = asyncio.get_running_loop()
        consumer_ready = asyncio.Event()

        async def consume() -> None:
            consumer_ready.set()
            try:
                while True:
                    event, acknowledgement = await queue.receive()
                    if isinstance(acknowledgement, asyncio.Future):
                        if not acknowledgement.done():
                            acknowledgement.set_result(event)
                    else:
                        acknowledgement.set()
            except EventQueueClosedError:
                return

        consumer = asyncio.create_task(consume())
        await consumer_ready.wait()

        try:
            if not cross_thread:
                started = perf_counter()
                for event in range(loops):
                    acknowledgement = loop.create_future()
                    bus.publish((event, acknowledgement))
                    if await acknowledgement != event:
                        raise RuntimeError("bridge delivered an unexpected event")
                elapsed = perf_counter() - started
            else:
                completed: Future[float] = Future()

                def publish_from_thread() -> None:
                    started = perf_counter()
                    try:
                        for event in range(loops):
                            acknowledgement = ThreadEvent()
                            bus.publish((event, acknowledgement))
                            if not acknowledgement.wait(timeout=10):
                                raise RuntimeError(
                                    "bridge benchmark consumer did not acknowledge an event"
                                )
                    except BaseException as error:
                        completed.set_exception(error)
                    else:
                        completed.set_result(perf_counter() - started)

                producer = Thread(
                    target=publish_from_thread,
                    name="EventFlowBridgeBenchmark",
                    daemon=True,
                )
                producer.start()
                try:
                    elapsed = await asyncio.wrap_future(completed)
                finally:
                    producer.join(timeout=10)
                    if producer.is_alive():
                        raise RuntimeError("bridge benchmark publisher did not stop")
        finally:
            try:
                await queue.aclose()
            finally:
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
