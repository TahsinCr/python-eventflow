"""Alternate old/new dispatch in one process to control run-to-run clock drift.

Supply a trusted baseline checkout, e.g. a git archive of v1.1. The baseline is
imported under a private benchmark-only module name, never into library code.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import platform
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from statistics import median
from time import perf_counter_ns
from typing import Any

import eventflow


def load_baseline(root: Path) -> Any:
    package = root.resolve() / "eventflow"
    spec = importlib.util.spec_from_file_location(
        "_eventflow_benchmark_baseline",
        package / "__init__.py",
        submodule_search_locations=[str(package)],
    )
    if spec is None or spec.loader is None:
        raise ValueError("Baseline must contain an eventflow package")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def result(label: str, samples: dict[str, list[float]]) -> dict[str, object]:
    before, after = median(samples["baseline"]), median(samples["extended"])
    return {
        "case": label,
        "baseline_us": before,
        "extended_us": after,
        "change_percent": (after / before - 1) * 100,
        "samples_us": samples,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=250)
    parser.add_argument("--samples", type=int, default=9)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.iterations <= 0 or args.samples <= 0:
        parser.error("iterations and samples must be positive")
    modules = {"baseline": load_baseline(args.baseline), "extended": eventflow}
    results = []

    def receive(_value: int) -> None:
        pass

    for label, count, filtering in [
        *((f"sync/{n}", n, None) for n in (0, 1, 10, 100)),
        ("sync/filter-type", 10, "type"),
        ("sync/filter-predicate", 10, "predicate"),
    ]:
        buses: dict[str, Any] = {}
        operations: dict[str, Callable[[], object]] = {}
        for name, module in modules.items():
            bus = module.EventBus()
            for _ in range(count):
                options = (
                    {"event_type": int}
                    if filtering == "type"
                    else (
                        {"predicate": lambda value: value > 0} if filtering == "predicate" else {}
                    )
                )
                bus.subscribe(receive, **options)
            buses[name] = bus
            operations[name] = lambda bus=bus: bus.publish(1)
            for _ in range(100):
                operations[name]()
        samples: dict[str, list[float]] = {name: [] for name in modules}
        for trial in range(args.samples):
            order = list(modules) if trial % 2 == 0 else list(reversed(modules))
            for name in order:
                operation = operations[name]
                started = perf_counter_ns()
                for _ in range(args.iterations):
                    operation()
                samples[name].append((perf_counter_ns() - started) / args.iterations / 1000)
        results.append(result(label, samples))
        for bus in buses.values():
            bus.close()

    async def asynchronous() -> None:
        async def receive_async(_value: int) -> None:
            pass

        for mode in ("sequential", "concurrent"):
            for count in (0, 1, 10, 100):
                buses: dict[str, Any] = {}
                operations: dict[str, Callable[[], Awaitable[object]]] = {}
                for name, module in modules.items():
                    bus = module.AsyncEventBus(delivery_mode=module.DeliveryMode(mode))
                    for _ in range(count):
                        await bus.subscribe(receive_async)
                    buses[name] = bus
                    operations[name] = lambda bus=bus: bus.publish(1)
                    for _ in range(100):
                        await operations[name]()
                samples: dict[str, list[float]] = {name: [] for name in modules}
                for trial in range(args.samples):
                    order = list(modules) if trial % 2 == 0 else list(reversed(modules))
                    for name in order:
                        operation = operations[name]
                        started = perf_counter_ns()
                        for _ in range(args.iterations):
                            await operation()
                        samples[name].append((perf_counter_ns() - started) / args.iterations / 1000)
                results.append(result(f"async-{mode}/{count}", samples))
                for bus in buses.values():
                    await bus.close()

    asyncio.run(asynchronous())
    args.output.write_text(
        json.dumps(
            {
                "python": sys.version,
                "platform": platform.platform(),
                "iterations_per_sample": args.iterations,
                "samples": args.samples,
                "results": results,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    for row in results:
        print(row["case"], f"{row['change_percent']:+.1f}%")


if __name__ == "__main__":
    main()
