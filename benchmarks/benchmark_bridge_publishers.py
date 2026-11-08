"""Benchmark wall and process CPU cost for concurrent sync-to-async handoff."""

from __future__ import annotations

import asyncio
import threading
from argparse import Namespace
from concurrent.futures import Future, ThreadPoolExecutor
from time import perf_counter, process_time

from eventflow import EventBus
from eventflow.bridges import AsyncEventQueue
from eventflow.errors import EventQueueClosedError

from ._runner import bench_time_func, create_runner


async def _sample(publishers: int, loops: int) -> tuple[float, float]:
    consumer_ready = asyncio.Event()
    start_barrier = threading.Barrier(publishers + 1)
    start_publishers = threading.Event()
    delivered = 0

    with EventBus[Future[None]]() as bus:
        queue = AsyncEventQueue(bus, capacity=publishers)

        async def consume() -> None:
            nonlocal delivered
            consumer_ready.set()
            try:
                while True:
                    acknowledgement = await queue.receive()
                    delivered += 1
                    if not acknowledgement.done():
                        acknowledgement.set_result(None)
            except EventQueueClosedError:
                return

        def publish(publisher: int) -> None:
            start_barrier.wait(timeout=10)
            if not start_publishers.wait(timeout=10):
                raise TimeoutError("bridge benchmark publishers were not released")
            for _ in range(loops):
                acknowledgement: Future[None] = Future()
                result = bus.publish(acknowledgement)
                if result.failed:
                    error = (
                        result.errors[0] if result.errors else RuntimeError("queue delivery failed")
                    )
                    raise RuntimeError("bridge benchmark event was not accepted") from error
                acknowledgement.result(timeout=20)

        consumer: asyncio.Task[None] | None = None
        try:
            async with queue:
                consumer = asyncio.create_task(consume())
                await consumer_ready.wait()
                with ThreadPoolExecutor(
                    max_workers=publishers,
                    thread_name_prefix="EventFlowPublisherBenchmark",
                ) as executor:
                    workers = [executor.submit(publish, index) for index in range(publishers)]
                    start_barrier.wait(timeout=10)
                    wall_started = perf_counter()
                    cpu_started = process_time()
                    start_publishers.set()
                    outcomes = await asyncio.gather(
                        *(asyncio.wrap_future(worker) for worker in workers),
                        return_exceptions=True,
                    )
                    wall_seconds = perf_counter() - wall_started
                    cpu_seconds = process_time() - cpu_started
                    for outcome in outcomes:
                        if isinstance(outcome, BaseException):
                            raise RuntimeError("bridge benchmark publisher failed") from outcome
        finally:
            if consumer is not None:
                await consumer

    if delivered != publishers * loops:
        raise RuntimeError("bridge benchmark did not deliver every published event")
    return wall_seconds, cpu_seconds


def _wall_time(loops: int, publishers: int) -> float:
    return asyncio.run(_sample(publishers, loops))[0]


def _cpu_time(loops: int, publishers: int) -> float:
    return asyncio.run(_sample(publishers, loops))[1]


def _add_publisher_args(command: list[str], args: Namespace) -> None:
    command.extend(("--publishers", *(str(publisher) for publisher in args.publishers)))


def main() -> None:
    runner = create_runner(
        __doc__ or "Concurrent bridge publisher benchmarks",
        add_cmdline_args=_add_publisher_args,
    )
    runner.argparser.add_argument("--publishers", type=int, nargs="+", default=[2, 4, 8, 16])
    args = runner.parse_args()
    if any(count < 1 for count in args.publishers):
        runner.argparser.error("publisher counts must be positive")
    if len(set(args.publishers)) != len(args.publishers):
        runner.argparser.error("publisher counts must be unique")

    for publishers in args.publishers:
        for metric, time_func in (("wall", _wall_time), ("process-cpu", _cpu_time)):
            bench_time_func(
                runner,
                f"bridge-publishers/{metric}/{publishers}",
                time_func,
                publishers,
                inner_loops=publishers,
                metadata={"publishers": publishers, "metric": metric},
            )


if __name__ == "__main__":
    main()
