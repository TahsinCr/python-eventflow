"""Measure route-cache pressure, FIFO research cases, and sparse dispatch matching."""

from __future__ import annotations

import gzip
import json
import math
import random
import statistics
import sys
from collections.abc import Hashable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import cycle
from pathlib import Path
from threading import Event
from time import perf_counter, perf_counter_ns, process_time_ns
from typing import Any

from eventflow import EventBus

from ._runner import (
    bench_sync,
    bench_time_func,
    benchmark_metadata,
    create_runner,
    forward_cli_arguments,
)
from ._support import ignore_event

_ROUTE_CACHE_LIMIT = 64
_ZIPF_SAMPLE_SIZE = 8192


@dataclass(frozen=True)
class RouteCacheCase:
    cardinality: int
    wildcards: int
    profile: str
    churn: str
    publishers: int

    @property
    def batch_size(self) -> int:
        return {"none": 1000, "low": 1000, "high": 100}[self.churn]

    @property
    def name(self) -> str:
        return (
            f"route-cache/{self.cardinality}/{self.wildcards}/"
            f"{self.profile}/{self.churn}/{self.publishers}p"
        )


def _parse_route_case(raw: str) -> RouteCacheCase:
    fields = raw.split(":")
    if len(fields) != 5:
        raise ValueError("route cases use cardinality:wildcards:profile:churn:publishers")
    cardinality, wildcards, profile, churn, publishers_text = fields
    try:
        case = RouteCacheCase(
            cardinality=int(cardinality),
            wildcards=int(wildcards),
            profile=profile,
            churn=churn,
            publishers=int(publishers_text),
        )
    except ValueError as exc:
        raise ValueError(f"invalid route case {raw!r}") from exc
    if case.cardinality < 1 or case.wildcards < 0:
        raise ValueError("route cardinality must be positive and wildcards non-negative")
    if case.profile not in {"uniform", "late-hot", "zipf-0.8", "zipf-1.2", "hot-shift"}:
        raise ValueError(f"unsupported route profile: {case.profile}")
    if case.churn not in {"none", "low", "high"}:
        raise ValueError(f"unsupported churn rate: {case.churn}")
    if case.publishers not in {1, 2, 4, 8}:
        raise ValueError("publisher count must be one of 1, 2, 4, or 8")
    return case


def _route_keys(case: RouteCacheCase) -> tuple[int, ...]:
    if case.profile == "uniform":
        return tuple(range(case.cardinality))
    if case.profile == "late-hot":
        return (case.cardinality - 1,)
    if case.profile == "hot-shift":
        first = max(0, case.cardinality - 8)
        return tuple(key for key in range(first, case.cardinality) for _ in range(100))
    exponent = float(case.profile.removeprefix("zipf-"))
    weights = [1 / rank**exponent for rank in range(1, case.cardinality + 1)]
    return tuple(
        random.Random(0).choices(range(case.cardinality), weights=weights, k=_ZIPF_SAMPLE_SIZE)
    )


class _RouteCacheState:
    def __init__(self, case: RouteCacheCase) -> None:
        self.case = case
        self.bus = EventBus[int](routing_key=lambda event: event)
        self.subscriptions = [
            self.bus.subscribe(ignore_event, routing_key=key) for key in range(case.cardinality)
        ]
        for _ in range(case.wildcards):
            self.bus.subscribe(ignore_event)
        self.churn_subscription: Any = self.subscriptions[0]
        self.rss_before_warmup = _rss_bytes()
        for key in range(min(case.cardinality, _ROUTE_CACHE_LIMIT)):
            self.bus.publish(key)
        self.rss_after_warmup = _rss_bytes()
        keys = _route_keys(case)
        self.publishers: list[Iterator[int]] = []
        for publisher in range(case.publishers):
            sequence = cycle(keys)
            for _ in range(len(keys) * publisher // case.publishers):
                next(sequence)
            self.publishers.append(sequence)

    def churn(self) -> None:
        self.churn_subscription.cancel()
        self.churn_subscription = self.bus.subscribe(ignore_event, routing_key=0)

    def close(self) -> None:
        self.bus.close()


def _rss_bytes() -> int | None:
    try:
        import psutil
    except ImportError:
        return None
    return psutil.Process().memory_info().rss


def _retained_cache_bytes(cache: Mapping[Hashable, object]) -> int:
    return sys.getsizeof(cache) + sum(
        sys.getsizeof(key) + sys.getsizeof(snapshot) for key, snapshot in cache.items()
    )


def _time_serial_route_batch(loops: int, state: _RouteCacheState) -> float:
    total = 0.0
    for _ in range(loops):
        if state.case.churn != "none":
            state.churn()
        started = perf_counter()
        for _ in range(state.case.batch_size):
            state.bus.publish(next(state.publishers[0]))
        total += perf_counter() - started
    return total


def _publish_batch(bus: EventBus[int], keys: Iterator[int], count: int, release: Event) -> None:
    release.wait()
    for _ in range(count):
        bus.publish(next(keys))


def _time_concurrent_route_batches(
    loops: int, state: _RouteCacheState, pool: ThreadPoolExecutor
) -> float:
    total = 0.0
    for _ in range(loops):
        if state.case.churn != "none":
            state.churn()
        release = Event()
        workers = [
            pool.submit(_publish_batch, state.bus, keys, state.case.batch_size, release)
            for keys in state.publishers
        ]
        started = perf_counter()
        release.set()
        for worker in workers:
            worker.result()
        total += perf_counter() - started
    return total


def _collect_route_latencies(case: RouteCacheCase, sample_count: int) -> dict[str, object]:
    state = _RouteCacheState(case)
    samples: list[int] = []
    cache_hits = 0
    cache_misses = 0
    publish_cpu_ns = 0
    churn_ns = 0
    churn_operations = 0
    rss_before_sampling = _rss_bytes()
    try:
        while len(samples) < sample_count:
            if case.churn != "none":
                started = perf_counter_ns()
                state.churn()
                churn_ns += perf_counter_ns() - started
                churn_operations += 1
            batch = min(case.batch_size, sample_count - len(samples))
            cpu_started = process_time_ns()
            for _ in range(batch):
                key = next(state.publishers[0])
                if key in state.bus._route_snapshots:
                    cache_hits += 1
                else:
                    cache_misses += 1
                started = perf_counter_ns()
                state.bus.publish(key)
                samples.append(perf_counter_ns() - started)
            publish_cpu_ns += process_time_ns() - cpu_started
        ordered = sorted(samples)
        rss_after_sampling = _rss_bytes()
        return {
            "case": case.name,
            "measurement": "single-publisher; subscription mutations occur between publish batches",
            "publishers_in_pyperf_case": case.publishers,
            "latency_p50_ns": statistics.median(ordered),
            "latency_p95_ns": ordered[math.ceil(0.95 * len(ordered)) - 1],
            "latency_p99_ns": ordered[math.ceil(0.99 * len(ordered)) - 1],
            "publish_cpu_ns": publish_cpu_ns,
            "publish_wall_sample_sum_ns": sum(samples),
            "churn_operations": churn_operations,
            "churn_total_ns": churn_ns,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "cache_hit_ratio": cache_hits / len(samples),
            "retained_cache_entries": len(state.bus._route_snapshots),
            "retained_cache_shallow_bytes": _retained_cache_bytes(state.bus._route_snapshots),
            "rss_before_cache_warmup_bytes": state.rss_before_warmup,
            "rss_after_cache_warmup_bytes": state.rss_after_warmup,
            "rss_before_sampling_bytes": rss_before_sampling,
            "rss_after_sampling_bytes": rss_after_sampling,
            "raw_latency_samples_ns": samples,
        }
    finally:
        state.close()


def _run_route_cache_suite(runner: Any, raw_cases: str) -> None:
    if not raw_cases:
        runner.argparser.error("--suite route-cache requires --route-cases")
    try:
        cases = [_parse_route_case(raw) for raw in raw_cases.split(";")]
    except ValueError as exc:
        runner.argparser.error(str(exc))
    for case in cases:
        state = _RouteCacheState(case)
        metadata = {
            "routes": case.cardinality,
            "wildcard_subscribers": case.wildcards,
            "access_profile": case.profile,
            "churn_rate": case.churn,
            "publishers": case.publishers,
            "churn_boundary": "subscribe/cancel before each publish batch; excluded from publish timing",
            "cache_limit": _ROUTE_CACHE_LIMIT,
        }
        try:
            if case.publishers == 1:
                bench_time_func(
                    runner,
                    case.name,
                    _time_serial_route_batch,
                    state,
                    inner_loops=case.batch_size,
                    metadata=metadata,
                )
            else:
                with ThreadPoolExecutor(max_workers=case.publishers) as pool:
                    bench_time_func(
                        runner,
                        case.name,
                        _time_concurrent_route_batches,
                        state,
                        pool,
                        inner_loops=case.publishers * case.batch_size,
                        metadata=metadata,
                    )
        finally:
            state.close()


def main() -> None:
    runner = create_runner(
        __doc__ or "Dispatch pressure",
        add_cmdline_args=forward_cli_arguments("suite", "route_cases"),
    )
    runner.argparser.add_argument(
        "--suite", choices=("routing", "matching", "route-cache"), default="routing"
    )
    runner.argparser.add_argument("--route-cases", default="")
    runner.argparser.add_argument("--collect-latencies", metavar="PATH")
    runner.argparser.add_argument("--latency-samples", type=int, default=10000)
    args = runner.parse_args()
    if args.collect_latencies:
        if args.suite != "route-cache" or not args.route_cases:
            runner.argparser.error(
                "latency collection requires --suite route-cache and --route-cases"
            )
        if args.latency_samples < 1000:
            runner.argparser.error("--latency-samples must be at least 1000")
        cases = [_parse_route_case(raw) for raw in args.route_cases.split(";")]
        output = Path(args.collect_latencies)
        output.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(output, "wt", encoding="utf-8") as stream:
            json.dump(
                {
                    "metadata": benchmark_metadata(),
                    "cases": [
                        _collect_route_latencies(case, args.latency_samples) for case in cases
                    ],
                },
                stream,
                separators=(",", ":"),
            )
        return
    if args.suite == "route-cache":
        _run_route_cache_suite(runner, args.route_cases)
        return
    if args.suite == "matching":
        for count in (64, 100, 2048):
            for percent in (0, 1, 10, 50, 100):
                accepted = count * percent // 100
                with EventBus[int]() as bus:
                    for index in range(count):
                        bus.subscribe(
                            ignore_event,
                            predicate=lambda event, index=index, accepted=accepted: (
                                index < accepted
                            ),
                        )
                    bench_sync(
                        runner,
                        f"matching/{count}/{percent}",
                        lambda bus=bus: bus.publish(1),
                        metadata={"subscribers": count, "matched_subscribers": accepted},
                    )
        return
    for count in (16, 64, 65, 80, 256, 1000):
        for wildcard in (0, 100, 1024):
            for profile in ("uniform", "late-hot"):
                with EventBus[int](routing_key=lambda event: event) as bus:
                    for key in range(count):
                        bus.subscribe(ignore_event, routing_key=key)
                    for _ in range(wildcard):
                        bus.subscribe(ignore_event)
                    for key in range(min(count, 64)):
                        bus.publish(key)
                    keys = cycle(range(count) if profile == "uniform" else (count - 1,))
                    bench_sync(
                        runner,
                        f"routing/{count}/{wildcard}/{profile}",
                        lambda bus=bus, keys=keys: bus.publish(next(keys)),
                        metadata={
                            "routes": count,
                            "wildcard_subscribers": wildcard,
                            "access_profile": profile,
                        },
                    )


if __name__ == "__main__":
    main()
