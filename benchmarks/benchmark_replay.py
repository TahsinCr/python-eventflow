"""Benchmark full history queries and bounded replay setup with pyperf."""

from __future__ import annotations

import asyncio
from time import perf_counter

from eventflow import AsyncEventBus, EventBus, EventFilter, MemoryEventHistory

from ._runner import bench_sync, bench_time_func, create_runner, forward_cli_arguments


async def _receive_async(_event: int) -> None:
    pass


def _time_async_replay(
    loops: int,
    history: MemoryEventHistory[int],
    replay: int,
) -> float:
    async def measure() -> float:
        async with AsyncEventBus[int](history=history) as bus:
            started = perf_counter()
            for _ in range(loops):
                subscription = await bus.subscribe(_receive_async, replay=replay)
                await subscription.cancel()
            return perf_counter() - started

    return asyncio.run(measure())


def main() -> None:
    runner = create_runner(
        __doc__ or "History and replay benchmarks",
        add_cmdline_args=forward_cli_arguments("events", "replay"),
    )
    runner.argparser.add_argument("--events", type=int, default=100_000)
    runner.argparser.add_argument("--replay", type=int, default=1)
    args = runner.parse_args()
    if args.events < 1 or args.replay < 1:
        runner.argparser.error("events and replay must be positive")

    history = MemoryEventHistory[int](capacity=args.events)
    for event in range(args.events):
        history.append(event)

    sync_bus = EventBus[int](history=history)
    async_bus = AsyncEventBus[int](history=history)
    sparse_filter = EventFilter[int](predicate=lambda event: event == 0)

    def receive_sync(_event: int) -> None:
        pass

    operations = (
        ("history/query-all", history.query),
        ("history/query-window", lambda: history.query(limit=args.replay)),
        (
            "history/query-sparse-filter",
            lambda: history.query(sparse_filter, limit=args.replay),
        ),
        ("history/latest", history.latest),
        ("sync-bus/latest", sync_bus.latest),
        ("sync-bus/query-window", lambda: sync_bus.query(limit=args.replay)),
        ("async-bus/latest", async_bus.latest),
        ("async-bus/query-window", lambda: async_bus.query(limit=args.replay)),
    )
    for name, operation in operations:
        bench_sync(
            runner,
            name,
            operation,
            metadata={"history_events": args.events, "replay_window": args.replay},
        )

    with sync_bus:

        def subscribe_sync() -> None:
            subscription = sync_bus.subscribe(receive_sync, replay=args.replay)
            subscription.cancel()

        bench_sync(
            runner,
            "sync-bus/replay-subscribe-cancel",
            subscribe_sync,
            metadata={"history_events": args.events, "replay_window": args.replay},
        )

    bench_time_func(
        runner,
        "async-bus/replay-subscribe-cancel",
        _time_async_replay,
        history,
        args.replay,
        metadata={"history_events": args.events, "replay_window": args.replay},
    )


if __name__ == "__main__":
    main()
