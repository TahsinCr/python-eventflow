"""Benchmark full history queries and bounded replay setup with pyperf."""

from __future__ import annotations

import asyncio
from time import perf_counter

from eventflow import AsyncEventBus, EventBus, EventFilter, MemoryEventHistory

from ._runner import bench_sync, bench_time_func, create_runner, forward_cli_arguments
from ._support import ignore_event, ignore_event_async


def _time_async_replay(
    loops: int,
    history: MemoryEventHistory[int],
    replay: int,
) -> float:
    async def measure() -> float:
        async with AsyncEventBus[int](history=history) as bus:
            started = perf_counter()
            for _ in range(loops):
                subscription = await bus.subscribe(ignore_event_async, replay=replay)
                await subscription.cancel()
            return perf_counter() - started

    return asyncio.run(measure())


def main() -> None:
    runner = create_runner(
        __doc__ or "History and replay benchmarks",
        add_cmdline_args=forward_cli_arguments("events", "replay"),
    )
    runner.argparser.add_argument("--events", type=int, nargs="+", default=[1_000, 10_000, 100_000])
    runner.argparser.add_argument("--replay", type=int, default=1)
    args = runner.parse_args()
    if any(event_count < 1 for event_count in args.events) or args.replay < 1:
        runner.argparser.error("event counts and replay must be positive")
    if len(set(args.events)) != len(args.events):
        runner.argparser.error("event counts must be unique")

    for event_count in args.events:
        history = MemoryEventHistory[int](capacity=event_count)
        for event in range(event_count):
            history.append(event)

        sync_bus = EventBus[int](history=history)
        async_bus = AsyncEventBus[int](history=history)
        filters = (
            ("oldest-match", EventFilter[int](predicate=lambda event: event == 0)),
            (
                "newest-match",
                EventFilter[int](predicate=lambda event, newest=event_count - 1: event == newest),
            ),
            ("no-match", EventFilter[int](predicate=lambda event: event < 0)),
        )
        operations = (
            (f"history/query-all/{event_count}", history.query),
            (f"history/latest/{event_count}", history.latest),
            (
                f"history/query-window/{event_count}/replay-{args.replay}",
                lambda history=history: history.query(limit=args.replay),
            ),
            (f"sync-bus/latest/{event_count}", sync_bus.latest),
            (
                f"sync-bus/query-window/{event_count}/replay-{args.replay}",
                lambda sync_bus=sync_bus: sync_bus.query(limit=args.replay),
            ),
            (f"async-bus/latest/{event_count}", async_bus.latest),
            (
                f"async-bus/query-window/{event_count}/replay-{args.replay}",
                lambda async_bus=async_bus: async_bus.query(limit=args.replay),
            ),
        )
        for name, operation in operations:
            bench_sync(
                runner,
                name,
                operation,
                metadata={"history_events": event_count, "replay_window": args.replay},
            )

        for filter_name, event_filter in filters:
            bench_sync(
                runner,
                f"history/query-filter/{filter_name}/{event_count}/replay-{args.replay}",
                lambda event_filter=event_filter, history=history: history.query(
                    event_filter, limit=args.replay
                ),
                metadata={
                    "history_events": event_count,
                    "replay_window": args.replay,
                    "filter_case": filter_name,
                },
            )

        with sync_bus:

            def subscribe_sync(bus: EventBus[int] = sync_bus) -> None:
                subscription = bus.subscribe(ignore_event, replay=args.replay)
                subscription.cancel()

            bench_sync(
                runner,
                f"sync-bus/replay-subscribe-cancel/{event_count}/replay-{args.replay}",
                subscribe_sync,
                metadata={"history_events": event_count, "replay_window": args.replay},
            )

        bench_time_func(
            runner,
            f"async-bus/replay-subscribe-cancel/{event_count}/replay-{args.replay}",
            _time_async_replay,
            history,
            args.replay,
            metadata={"history_events": event_count, "replay_window": args.replay},
        )


if __name__ == "__main__":
    main()
