"""Compare Python, Cython and C kernels inside representative EventFlow paths."""

from __future__ import annotations

import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from importlib import import_module
from threading import Barrier
from time import perf_counter
from typing import cast

from benchmarks._runner import bench_sync, bench_time_func, create_runner, forward_cli_arguments
from benchmarks.native import python_kernels
from benchmarks.native._contracts import FilterKernel, MergeKernel
from benchmarks.native.provenance import native_build_metadata
from eventflow.event_bus import EventBus
from eventflow.filtering import EventFilter
from eventflow.history import MemoryEventHistory

from ._support import ignore_event


class Event:
    __slots__ = ("payload",)

    def __init__(self, payload: int) -> None:
        self.payload = payload


class SelectedEvent(Event):
    __slots__ = ()


class SelectedChildEvent(SelectedEvent):
    __slots__ = ()


class RejectedEvent(Event):
    __slots__ = ()


@dataclass(frozen=True, slots=True)
class RoutingEvent:
    key: int
    payload: int


class CandidateBus(EventBus[RoutingEvent]):
    def __init__(
        self,
        *,
        merge: Callable[[dict[int, object], dict[int, object]], tuple[object, ...]] | None,
    ) -> None:
        super().__init__(routing_key=lambda event: event.key)
        self._candidate_merge = merge

    def _subscriber_snapshot(self, event_key: object) -> tuple[object, ...]:
        if self._candidate_merge is None:
            return super()._subscriber_snapshot(event_key)  # type: ignore[arg-type]
        unkeyed = self._unkeyed_subscribers
        keyed = self._keyed_subscribers.get(event_key, {})
        return self._candidate_merge(unkeyed, keyed)  # type: ignore[arg-type]


def _kernels(
    implementation: str,
) -> tuple[
    FilterKernel,
    tuple[tuple[str, MergeKernel], ...],
    tuple[tuple[str, FilterKernel], ...],
]:
    if implementation == "python":
        return (
            python_kernels.filter_by_type,
            (
                ("python_heapq", python_kernels.merge_subscribers),
                ("python_two_pointer", python_kernels.merge_subscribers_two_pointer),
                ("python_sorted", python_kernels.merge_subscribers_sorted),
            ),
            (),
        )
    if implementation == "cython":
        module_name = "eventflow_native_cython"
    elif implementation == "c":
        module_name = "eventflow_native_c"
    else:
        raise ValueError(f"unknown implementation: {implementation}")
    module = import_module(module_name)
    merge_kernels = [(f"{implementation}_key_lookup", cast(MergeKernel, module.merge_subscribers))]
    if implementation == "cython":
        merge_kernels.append(("cython_items", cast(MergeKernel, module.merge_subscribers_items)))
    return (
        cast(FilterKernel, module.filter_by_type),
        tuple(merge_kernels),
        (
            (("c_tuple_indexed", cast(FilterKernel, module.filter_by_type_tuple)),)
            if implementation == "c"
            else ()
        ),
    )


def _history_cases(
    runner,
    implementation: str,
    filter_by_type: FilterKernel,
    tuple_filter_kernels: tuple[tuple[str, FilterKernel], ...],
    matrix: str,
) -> None:
    if matrix == "smoke":
        sizes, selectivities = (1_000,), (2,)
    elif matrix in {"decision", "history"}:
        sizes, selectivities = (100_000,), (2,)
    else:
        sizes, selectivities = (1_000, 10_000, 100_000), (2, 8)
    type_shapes = (
        (("single_type", SelectedEvent),)
        if matrix == "smoke"
        else (("single_type", SelectedEvent), ("type_tuple", (SelectedEvent, RejectedEvent)))
    )
    limits = (None,) if matrix == "smoke" else (None, 1, 10)
    for size in sizes:
        for selected_every in selectivities:
            history = MemoryEventHistory[Event](capacity=size)
            for index in range(size):
                if index % selected_every == 0:
                    event = SelectedChildEvent(index) if index % 4 == 0 else SelectedEvent(index)
                elif index % 13 == 0:
                    event = RejectedEvent(index)
                else:
                    event = Event(index)
                history.append(event)

            snapshot = history.query()
            bench_sync(
                runner,
                f"history_snapshot/{size}/one_in_{selected_every}",
                history.query,
                metadata={
                    "case": "memory_history_snapshot",
                    "history_events": size,
                },
            )
            if implementation == "python":
                predicate_filter = EventFilter[Event](
                    event_type=SelectedEvent,
                    predicate=lambda event: event.payload % 4 == 0,
                )
                predicate_limits = (None,) if matrix == "smoke" else (None, 10)
                for limit in predicate_limits:
                    expected_predicate_result = tuple(
                        event
                        for event in snapshot
                        if isinstance(event, SelectedEvent) and event.payload % 4 == 0
                    )
                    if limit is not None:
                        expected_predicate_result = expected_predicate_result[-limit:]
                    actual_predicate_result = history.query(predicate_filter, limit=limit)
                    if len(actual_predicate_result) != len(expected_predicate_result) or any(
                        actual is not expected
                        for actual, expected in zip(
                            actual_predicate_result, expected_predicate_result, strict=True
                        )
                    ):
                        raise AssertionError("history predicate query changed result order")
                    limit_name = "all" if limit is None else str(limit)
                    bench_sync(
                        runner,
                        f"history_production_predicate_query/{size}/one_in_{selected_every}/limit_{limit_name}",
                        lambda history=history, predicate_filter=predicate_filter, limit=limit: (
                            history.query(predicate_filter, limit=limit)
                        ),
                        metadata={
                            "case": "production_history_type_and_predicate_query",
                            "history_events": size,
                            "match_period": selected_every,
                            "query_limit": limit_name,
                        },
                    )
            for event_type_name, event_type in type_shapes:
                query_filter = EventFilter[Event](event_type=event_type)
                for limit in limits:
                    expected_query = history.query(query_filter, limit=limit)
                    actual_query = filter_by_type(snapshot, event_type, limit)
                    if len(actual_query) != len(expected_query) or any(
                        actual is not expected
                        for actual, expected in zip(actual_query, expected_query, strict=True)
                    ):
                        raise AssertionError(
                            f"{implementation} changed MemoryEventHistory query results"
                        )

                    limit_name = "all" if limit is None else str(limit)
                    case_name = (
                        f"{size}/one_in_{selected_every}/{event_type_name}/limit_{limit_name}"
                    )
                    metadata = {
                        "candidate_implementation": implementation,
                        "case": "history_type_filter_only",
                        "history_events": size,
                        "match_period": selected_every,
                        "event_type_shape": event_type_name,
                        "query_limit": limit_name,
                    }
                    bench_sync(
                        runner,
                        f"history_filter_only/{case_name}",
                        lambda snapshot=snapshot, event_type=event_type, limit=limit: (
                            filter_by_type(snapshot, event_type, limit)
                        ),
                        metadata=metadata,
                    )
                    bench_sync(
                        runner,
                        f"history_snapshot_and_filter/{case_name}",
                        lambda history=history, event_type=event_type, limit=limit: filter_by_type(
                            history.query(), event_type, limit
                        ),
                        metadata={**metadata, "case": "history_snapshot_and_type_filter"},
                    )

                    for candidate_name, candidate_filter in tuple_filter_kernels:
                        if limit is not None:
                            continue
                        candidate_result = candidate_filter(snapshot, event_type)
                        if len(candidate_result) != len(expected_query) or any(
                            actual is not expected
                            for actual, expected in zip(
                                candidate_result, expected_query, strict=True
                            )
                        ):
                            raise AssertionError(
                                f"{implementation}/{candidate_name} changed history query results"
                            )
                        candidate_metadata = {
                            **metadata,
                            "candidate_algorithm": candidate_name,
                            "case": "history_type_filter_only",
                            "query_limit": "all",
                        }
                        bench_sync(
                            runner,
                            f"history_filter_only/{candidate_name}/{case_name}",
                            lambda snapshot=snapshot, event_type=event_type, candidate_filter=candidate_filter: (
                                candidate_filter(snapshot, event_type)
                            ),
                            metadata=candidate_metadata,
                        )
                        bench_sync(
                            runner,
                            f"history_snapshot_and_filter/{candidate_name}/{case_name}",
                            lambda history=history, event_type=event_type, candidate_filter=candidate_filter: (
                                candidate_filter(history.query(), event_type)
                            ),
                            metadata={
                                **candidate_metadata,
                                "case": "history_snapshot_and_type_filter",
                            },
                        )

                    if implementation == "python":
                        bench_sync(
                            runner,
                            f"history_production_query/{case_name}",
                            lambda history=history, query_filter=query_filter, limit=limit: (
                                history.query(query_filter, limit=limit)
                            ),
                            metadata={**metadata, "case": "production_memory_history_query"},
                        )


def _subscribe_fixture(
    count: int,
    *,
    merge: Callable[[dict[int, object], dict[int, object]], tuple[object, ...]] | None,
    key_count: int = 7,
    wildcard_percent: int = 20,
    callback: Callable[[RoutingEvent], None] = ignore_event,
    filtered: bool = False,
) -> EventBus[RoutingEvent]:
    bus: EventBus[RoutingEvent]
    if merge is None:
        bus = EventBus(routing_key=lambda event: event.key)
    else:
        bus = CandidateBus(merge=merge)
    for index in range(count):
        routing_key = None if index % 100 < wildcard_percent else (index * 17) % key_count
        event_type = RejectedEvent if filtered else None
        bus.subscribe(callback, event_type=event_type, routing_key=routing_key)
    return bus


def _light_cpu_event(event: RoutingEvent) -> None:
    value = event.payload
    for _ in range(16):
        value = ((value * 33) ^ event.key) & 0xFFFF_FFFF
    if value < 0:
        raise AssertionError("bounded callback result became negative")


def _routing_cases(
    runner,
    implementation: str,
    merge_kernels: tuple[tuple[str, MergeKernel], ...],
    matrix: str,
) -> None:
    if matrix == "smoke":
        profiles, snapshot_sizes = ((7, 20),), (64,)
    elif matrix == "routing":
        profiles, snapshot_sizes = ((7, 20),), (2_048,)
    elif matrix == "decision":
        profiles, snapshot_sizes = ((7, 20),), (64, 512, 2_048)
    else:
        profiles, snapshot_sizes = ((1, 0), (7, 20), (31, 50)), (0, 1, 4, 8, 64, 512, 2_048)
    publish_cases = (
        ("noop", ignore_event, False),
        ("light_cpu", _light_cpu_event, False),
        ("filtered", ignore_event, True),
    )

    for kernel_name, merge in merge_kernels:
        for key_count, wildcard_percent in profiles:
            for count in snapshot_sizes:
                bus = _subscribe_fixture(
                    count,
                    merge=merge,
                    key_count=key_count,
                    wildcard_percent=wildcard_percent,
                )
                event = RoutingEvent(0, 17)
                try:
                    selected = len(bus._subscriber_snapshot(event.key))
                    reference_bus = _subscribe_fixture(
                        count,
                        merge=None,
                        key_count=key_count,
                        wildcard_percent=wildcard_percent,
                    )
                    try:
                        reference_count = len(reference_bus._subscriber_snapshot(event.key))
                    finally:
                        reference_bus.close()
                    if selected != reference_count:
                        raise AssertionError(
                            f"{kernel_name} selected {selected} subscribers; expected {reference_count}"
                        )
                    metadata = {
                        "candidate_implementation": implementation,
                        "candidate_algorithm": kernel_name,
                        "case": "event_bus_routing_snapshot",
                        "subscriptions": count,
                        "selected_subscriptions": selected,
                        "wildcard_percent": wildcard_percent,
                        "key_count": key_count,
                    }
                    bench_sync(
                        runner,
                        f"routing_snapshot/{kernel_name}/{count}/keys_{key_count}/wildcard_{wildcard_percent}",
                        lambda bus=bus, event=event: bus._subscriber_snapshot(event.key),
                        metadata=metadata,
                    )
                finally:
                    bus.close()

                if implementation == "python" and kernel_name == "python_two_pointer":
                    production_bus = _subscribe_fixture(
                        count,
                        merge=None,
                        key_count=key_count,
                        wildcard_percent=wildcard_percent,
                    )
                    try:
                        bench_sync(
                            runner,
                            f"routing_snapshot_production/{count}/keys_{key_count}/wildcard_{wildcard_percent}",
                            lambda bus=production_bus, event=event: bus._subscriber_snapshot(
                                event.key
                            ),
                            metadata={
                                **metadata,
                                "candidate_implementation": "production_python",
                                "candidate_algorithm": "two_pointer_merge",
                                "case": "event_bus_production_routing_snapshot",
                            },
                        )
                    finally:
                        production_bus.close()

        if matrix == "smoke":
            publish_profiles, publish_sizes = ((7, 20, ("noop",)),), (64,)
        elif matrix == "routing":
            publish_profiles, publish_sizes = (
                (
                    (7, 20, ("noop", "light_cpu", "filtered")),
                    (1, 0, ("noop",)),
                    (31, 50, ("noop",)),
                ),
                (2_048,),
            )
        elif matrix == "decision":
            publish_profiles, publish_sizes = (
                (
                    (7, 20, ("noop", "light_cpu", "filtered")),
                    (1, 0, ("noop",)),
                    (31, 50, ("noop",)),
                ),
                (64, 512, 2_048),
            )
        else:
            publish_profiles, publish_sizes = (
                (
                    (7, 20, ("noop", "light_cpu", "filtered")),
                    (1, 0, ("noop",)),
                    (31, 50, ("noop",)),
                ),
                (4, 64, 512, 2_048),
            )
        for count in publish_sizes:
            for key_count, wildcard_percent, case_names in publish_profiles:
                for case_name, callback, filtered in publish_cases:
                    if case_name not in case_names:
                        continue
                    event = RoutingEvent(0, 17)
                    bus = _subscribe_fixture(
                        count,
                        merge=merge,
                        key_count=key_count,
                        wildcard_percent=wildcard_percent,
                        callback=callback,
                        filtered=filtered,
                    )
                    try:
                        selected = len(bus._subscriber_snapshot(event.key))
                        expected_matched = 0 if filtered else selected
                        expected_delivered = expected_matched
                        result = bus.publish(event)
                        if (result.matched, result.delivered) != (
                            expected_matched,
                            expected_delivered,
                        ):
                            raise AssertionError(
                                f"{kernel_name} publish counts differ from route selection: {result}"
                            )
                        metadata = {
                            "candidate_implementation": implementation,
                            "candidate_algorithm": kernel_name,
                            "case": "complete_event_bus_publish",
                            "subscriptions": count,
                            "matched_callbacks": result.matched,
                            "selected_route_subscriptions": selected,
                            "callback_work": case_name,
                            "wildcard_percent": wildcard_percent,
                            "key_count": key_count,
                        }
                        bench_sync(
                            runner,
                            f"routing_publish/{kernel_name}/{case_name}/{count}/keys_{key_count}/wildcard_{wildcard_percent}",
                            lambda bus=bus, event=event: bus.publish(event),
                            metadata=metadata,
                        )
                    finally:
                        bus.close()

                    if implementation == "python" and kernel_name == "python_two_pointer":
                        production_bus = _subscribe_fixture(
                            count,
                            merge=None,
                            key_count=key_count,
                            wildcard_percent=wildcard_percent,
                            callback=callback,
                            filtered=filtered,
                        )
                        try:
                            production_result = production_bus.publish(event)
                            if (
                                production_result.matched != expected_matched
                                or production_result.delivered != expected_delivered
                            ):
                                raise AssertionError(
                                    "production Python bus returned unexpected publish counts"
                                )
                            bench_sync(
                                runner,
                                f"routing_publish_production/{case_name}/{count}/keys_{key_count}/wildcard_{wildcard_percent}",
                                lambda bus=production_bus, event=event: bus.publish(event),
                                metadata={
                                    **metadata,
                                    "candidate_implementation": "production_python",
                                    "candidate_algorithm": "two_pointer_merge",
                                    "case": "production_event_bus_publish",
                                },
                            )
                        finally:
                            production_bus.close()


def _time_concurrent_publish(
    loops: int,
    bus: EventBus[RoutingEvent],
    event: RoutingEvent,
    publishers: int,
) -> float:
    barrier = Barrier(publishers + 1)

    def publish_many() -> None:
        barrier.wait(timeout=10)
        for _ in range(loops):
            bus.publish(event)

    with ThreadPoolExecutor(max_workers=publishers) as executor:
        futures = [executor.submit(publish_many) for _ in range(publishers)]
        started = perf_counter()
        barrier.wait(timeout=10)
        for future in futures:
            future.result()
        return perf_counter() - started


def _routing_contention_cases(
    runner,
    implementation: str,
    merge_kernels: tuple[tuple[str, MergeKernel], ...],
    matrix: str,
) -> None:
    if matrix == "smoke":
        publishers_counts, subscriptions = (2,), 64
    elif matrix == "routing":
        publishers_counts, subscriptions = (1, 4, 8), 2_048
    elif matrix == "decision":
        publishers_counts, subscriptions = (1, 4, 8), 2_048
    else:
        publishers_counts, subscriptions = (1, 2, 4, 8, 16), 2_048

    event = RoutingEvent(0, 17)
    for kernel_name, merge in merge_kernels:
        bus = _subscribe_fixture(
            subscriptions,
            merge=merge,
            key_count=7,
            wildcard_percent=20,
        )
        try:
            for publishers in publishers_counts:
                metadata = {
                    "candidate_implementation": implementation,
                    "candidate_algorithm": kernel_name,
                    "case": "concurrent_event_bus_publish",
                    "subscriptions": subscriptions,
                    "selected_subscriptions": len(bus._subscriber_snapshot(event.key)),
                    "wildcard_percent": 20,
                    "key_count": 7,
                    "publishers": publishers,
                }
                bench_time_func(
                    runner,
                    f"routing_contention/{kernel_name}/{publishers}/subscriptions_{subscriptions}",
                    _time_concurrent_publish,
                    bus,
                    event,
                    publishers,
                    inner_loops=publishers,
                    metadata=metadata,
                )
        finally:
            bus.close()

        if implementation == "python" and kernel_name == "python_two_pointer":
            production_bus = _subscribe_fixture(
                subscriptions,
                merge=None,
                key_count=7,
                wildcard_percent=20,
            )
            try:
                for publishers in publishers_counts:
                    metadata = {
                        "candidate_implementation": "production_python",
                        "candidate_algorithm": "two_pointer_merge",
                        "case": "production_concurrent_event_bus_publish",
                        "subscriptions": subscriptions,
                        "selected_subscriptions": len(
                            production_bus._subscriber_snapshot(event.key)
                        ),
                        "wildcard_percent": 20,
                        "key_count": 7,
                        "publishers": publishers,
                    }
                    bench_time_func(
                        runner,
                        f"routing_contention_production/{publishers}/subscriptions_{subscriptions}",
                        _time_concurrent_publish,
                        production_bus,
                        event,
                        publishers,
                        inner_loops=publishers,
                        metadata=metadata,
                    )
            finally:
                production_bus.close()


def main() -> int:
    runner = create_runner(
        "EventFlow Python/Cython/C candidate benchmarks",
        add_cmdline_args=forward_cli_arguments("implementation", "matrix"),
    )
    runner.argparser.add_argument(
        "--implementation", choices=("python", "cython", "c"), default="python"
    )
    runner.argparser.add_argument(
        "--matrix",
        choices=("smoke", "routing", "history", "decision", "full"),
        default="full",
    )
    args = runner.parse_args()
    runner.metadata["eventflow_native_matrix"] = args.matrix
    if args.implementation == "python":
        runner.metadata["eventflow_native_backend"] = "python"
    else:
        runner.metadata.update(native_build_metadata(args.implementation))
    gil_check = getattr(sys, "_is_gil_enabled", None)
    gil_was_disabled = gil_check is not None and not gil_check()
    filter_by_type, merge_kernels, tuple_filter_kernels = _kernels(args.implementation)
    if gil_was_disabled and gil_check is not None and gil_check():
        raise RuntimeError(
            f"importing the {args.implementation} candidate re-enabled the free-threaded GIL"
        )
    if args.matrix != "routing":
        _history_cases(
            runner,
            args.implementation,
            filter_by_type,
            tuple_filter_kernels,
            args.matrix,
        )
    if args.matrix != "history":
        _routing_cases(runner, args.implementation, merge_kernels, args.matrix)
    if args.matrix != "history":
        _routing_contention_cases(runner, args.implementation, merge_kernels, args.matrix)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
