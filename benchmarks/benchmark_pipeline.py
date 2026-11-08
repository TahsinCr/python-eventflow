"""Benchmark pipeline stages, decorators, middleware and opt-in metrics."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import partial
from time import perf_counter

from eventflow.observability import PipelineStats
from eventflow.pipeline import AsyncPipeline, Pipeline

from ._runner import bench_sync, bench_time_func, create_runner


def _time_async_pipeline(loops: int, count: int, decorator: bool, middleware: bool) -> float:
    async def increment(value: int) -> int:
        return value + 1

    async def pass_through(
        context: int,
        next_: Callable[[int], Awaitable[int]],
    ) -> int:
        return await next_(context)

    async def measure() -> float:
        pipeline = AsyncPipeline[int]()
        if middleware:
            pipeline.add("increment", increment)
            for index in range(count):
                pipeline.add_middleware(str(index), pass_through)
        else:
            for index in range(count):
                if decorator:
                    pipeline.stage(str(index))(increment)
                else:
                    pipeline.add(str(index), increment)

        started = perf_counter()
        for _ in range(loops):
            await pipeline.run(0)
        return perf_counter() - started

    return asyncio.run(measure())


def main() -> None:
    runner = create_runner(__doc__ or "Pipeline benchmarks")
    runner.parse_args()

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
            bench_sync(
                runner,
                f"sync/{count}/{'decorator' if decorator else 'add'}",
                partial(pipeline.run, 0),
                metadata={"stages": count, "registration": "decorator" if decorator else "add"},
            )

        observed = Pipeline[int](observer=PipelineStats())
        for index in range(count):
            observed.add(str(index), increment)
        bench_sync(
            runner,
            f"sync/{count}/metrics",
            partial(observed.run, 0),
            metadata={"stages": count, "metrics": "enabled"},
        )

        def pass_through(context: int, next_: Callable[[int], int]) -> int:
            return next_(context)

        wrapped = Pipeline[int]()
        wrapped.add("increment", increment)
        for index in range(count):
            wrapped.add_middleware(str(index), pass_through)
        bench_sync(
            runner,
            f"sync/{count}/middleware",
            partial(wrapped.run, 0),
            metadata={"middleware": count},
        )

    for count in (1, 5, 20):
        for decorator in (False, True):
            bench_time_func(
                runner,
                f"async/{count}/{'decorator' if decorator else 'add'}",
                _time_async_pipeline,
                count,
                decorator,
                False,
                metadata={"stages": count, "registration": "decorator" if decorator else "add"},
            )

        bench_time_func(
            runner,
            f"async/{count}/middleware",
            _time_async_pipeline,
            count,
            False,
            True,
            metadata={"middleware": count},
        )


if __name__ == "__main__":
    main()
