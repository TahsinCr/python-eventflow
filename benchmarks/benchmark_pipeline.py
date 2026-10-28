"""Compare programmatic/decorator registration, async dispatch and opt-in metrics."""

import argparse
import asyncio
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

    async def asynchronous() -> None:
        async def increment_async(value: int) -> int:
            return value + 1

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

    asyncio.run(asynchronous())
    report(results, args.output)


if __name__ == "__main__":
    main()
