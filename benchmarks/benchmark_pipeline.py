"""Measure pipeline registration, middleware execution and opt-in metrics."""

import argparse
import asyncio
from collections.abc import Awaitable, Callable
from functools import partial

from eventflow.pipeline import AsyncPipeline, Pipeline
from eventflow.observability import PipelineStats

from ._measure import measure, measure_async, report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=5000)
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.iterations <= 0:
        parser.error("iterations must be positive")
    results = []

    def increment(value: int) -> int:
        return value + 1

    for count in (1, 5, 20):
        for decorator in (False, True):
            pipeline = Pipeline[int]()
            for index in range(count):
                if decorator:
                    pipeline.stage(str(index))(increment)
                else:
                    pipeline.add(str(index), increment)
            results.append(
                measure(
                    f"sync/{count}/{'decorator' if decorator else 'add'}",
                    partial(pipeline.run, 0),
                    args.iterations,
                )
            )
        observed = Pipeline[int](observer=PipelineStats())
        for index in range(count):
            observed.add(str(index), increment)
        results.append(measure(f"sync/{count}/metrics", partial(observed.run, 0), args.iterations))

        def pass_through(context: int, next_: Callable[[int], int]) -> int:
            return next_(context)

        wrapped = Pipeline[int]()
        wrapped.add("increment", increment)
        for index in range(count):
            wrapped.add_middleware(str(index), pass_through)
        results.append(
            measure(
                f"sync/{count}/middleware",
                partial(wrapped.run, 0),
                args.iterations,
            )
        )

    async def asynchronous() -> None:
        async def increment_async(value: int) -> int:
            return value + 1

        async def pass_through_async(
            context: int,
            next_: Callable[[int], Awaitable[int]],
        ) -> int:
            return await next_(context)

        for count in (1, 5, 20):
            for decorator in (False, True):
                pipeline = AsyncPipeline[int]()
                for index in range(count):
                    if decorator:
                        pipeline.stage(str(index))(increment_async)
                    else:
                        pipeline.add(str(index), increment_async)
                results.append(
                    await measure_async(
                        f"async/{count}/{'decorator' if decorator else 'add'}",
                        partial(pipeline.run, 0),
                        args.iterations,
                    )
                )
            wrapped = AsyncPipeline[int]()
            wrapped.add("increment", increment_async)
            for index in range(count):
                wrapped.add_middleware(str(index), pass_through_async)
            results.append(
                await measure_async(
                    f"async/{count}/middleware",
                    partial(wrapped.run, 0),
                    args.iterations,
                )
            )

    asyncio.run(asynchronous())
    report(results, args.output)


if __name__ == "__main__":
    main()
