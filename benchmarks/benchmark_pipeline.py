"""Benchmark pipeline stages, decorators, middleware and opt-in metrics."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import partial

from eventflow.observability import PipelineStats
from eventflow.pipeline import AsyncPipeline, Pipeline

from ._runner import bench_async, bench_sync, create_runner


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
            bench_async(
                runner,
                f"async/{count}/{'decorator' if decorator else 'add'}",
                partial(pipeline.run, 0),
                metadata={"stages": count, "registration": "decorator" if decorator else "add"},
            )

        wrapped_async = AsyncPipeline[int]()
        wrapped_async.add("increment", increment_async)
        for index in range(count):
            wrapped_async.add_middleware(str(index), pass_through_async)
        bench_async(
            runner,
            f"async/{count}/middleware",
            partial(wrapped_async.run, 0),
            metadata={"middleware": count},
        )


if __name__ == "__main__":
    main()
