"""Benchmark full AsyncStream consumption and native iterator baselines."""

from __future__ import annotations

from collections.abc import AsyncIterator

from eventflow.stream import AsyncStream

from ._runner import bench_async, create_runner


def main() -> None:
    runner = create_runner(__doc__ or "AsyncStream benchmarks")
    runner.argparser.add_argument("--items", type=int, default=1_000)
    args = runner.parse_args()
    if args.items < 1:
        runner.argparser.error("items must be positive")

    async def source() -> AsyncIterator[int]:
        for value in range(args.items):
            yield value

    async def native_map_filter() -> AsyncIterator[int]:
        async for value in source():
            mapped = value + 1
            if mapped > 0:
                yield mapped

    async def consume_native() -> None:
        async for _ in native_map_filter():
            pass

    async def native_batches() -> AsyncIterator[tuple[int, ...]]:
        batch: list[int] = []
        async for value in source():
            batch.append(value)
            if len(batch) == 64:
                yield tuple(batch)
                batch.clear()
        if batch:
            yield tuple(batch)

    async def consume_native_batches() -> None:
        async for _ in native_batches():
            pass

    async def consume(buffered: bool) -> None:
        stream = AsyncStream(source()).map(lambda value: value + 1).filter(lambda value: value > 0)
        if buffered:
            stream = stream.buffer(16)
        async with stream:
            async for _ in stream:
                pass

    async def direct() -> None:
        async for _ in source():
            pass

    async def consume_batch_within() -> None:
        stream = AsyncStream(source()).batch_within(64, 60.0)
        async with stream:
            async for _ in stream:
                pass

    operations = (
        ("direct-generator", direct),
        ("native-map-filter", consume_native),
        ("unbuffered", lambda: consume(False)),
        ("buffer-16", lambda: consume(True)),
        ("native-batch-64", consume_native_batches),
        ("batch_within-64-burst", consume_batch_within),
    )
    for name, operation in operations:
        bench_async(runner, name, operation, metadata={"items_per_stream": args.items})


if __name__ == "__main__":
    main()
