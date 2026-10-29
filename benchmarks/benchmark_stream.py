"""Measure whole-stream latency, item throughput and bounded peak allocations."""

import argparse
import asyncio
from collections.abc import AsyncIterator
from typing import cast

from eventflow.stream import AsyncStream

from ._measure import measure_async, report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--items", type=int, default=1000)
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.iterations <= 0 or args.items <= 0:
        parser.error("iterations and items must be positive")
    results = []

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

    async def asynchronous() -> None:
        operations = [
            ("direct-generator", direct),
            ("native-map-filter", consume_native),
            ("unbuffered", lambda: consume(False)),
            ("buffer-16", lambda: consume(True)),
        ]
        for label, operation in operations:
            result = await measure_async(label, operation, args.iterations)
            result["items_per_operation"] = args.items
            result["items_per_second"] = cast(float, result["ops_per_second"]) * args.items
            results.append(result)

    asyncio.run(asynchronous())
    report(results, args.output)


if __name__ == "__main__":
    main()
