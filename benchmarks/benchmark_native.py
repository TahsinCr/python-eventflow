"""Compare Python, Cython and C kernels inside representative EventFlow paths."""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from typing import cast

from benchmarks._runner import bench_sync, create_runner, forward_cli_arguments
from benchmarks.native import python_kernels
from benchmarks.native._contracts import FilterKernel, MergeKernel
from eventflow.event_bus import EventBus
from eventflow.filtering import EventFilter
from eventflow.history import MemoryEventHistory


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


def _kernels(implementation: str) -> tuple[FilterKernel, MergeKernel]:
    if implementation == "python":
        return python_kernels.filter_by_type, python_kernels.merge_subscribers
    module_names = {"cython": "eventflow_native_cython", "c": "eventflow_native_c"}
    module_name = module_names.get(implementation)
    if module_name is None:
        raise ValueError(f"unknown implementation: {implementation}")
    module = import_module(module_name)
    return (
        cast(FilterKernel, module.filter_by_type),
        cast(MergeKernel, module.merge_subscribers),
    )


def _history_cases(
    runner,
    implementation: str,
    filter_by_type: FilterKernel,
) -> None:
    for size in (1_000, 10_000, 100_000):
        for selected_every in (2, 8):
            history = MemoryEventHistory[Event](capacity=size)
            for index in range(size):
                if index % selected_every == 0:
                    event = SelectedChildEvent(index) if index % 4 == 0 else SelectedEvent(index)
                else:
                    event = Event(index)
                history.append(event)

            query_filter = EventFilter[Event](event_type=SelectedEvent)

            def candidate_query(history=history):
                snapshot = history.query()
                return filter_by_type(snapshot, SelectedEvent)

            metadata = {
                "candidate_implementation": implementation,
                "case": "history_snapshot_and_type_filter",
                "history_events": size,
                "match_period": selected_every,
            }
            expected_query = history.query(query_filter)
            actual_query = candidate_query()
            if len(actual_query) != len(expected_query) or any(
                actual is not expected
                for actual, expected in zip(actual_query, expected_query, strict=True)
            ):
                raise AssertionError(f"{implementation} changed MemoryEventHistory query results")
            name = f"history_snapshot_type_filter/{size}/one_in_{selected_every}"
            bench_sync(runner, name, candidate_query, metadata=metadata)

            if implementation == "python":
                bench_sync(
                    runner,
                    f"history_production_query/{size}/one_in_{selected_every}",
                    lambda history=history, query_filter=query_filter: history.query(query_filter),
                    metadata={**metadata, "case": "production_memory_history_query"},
                )


def _subscribe_fixture(
    count: int,
    *,
    merge: Callable[[dict[int, object], dict[int, object]], tuple[object, ...]] | None,
    filtered: bool,
) -> EventBus[RoutingEvent]:
    bus: EventBus[RoutingEvent]
    if merge is None:
        bus = EventBus(routing_key=lambda event: event.key)
    else:
        bus = CandidateBus(merge=merge)
    callback = _ignore_event
    for index in range(count):
        routing_key = None if index % 5 == 0 else index % 7
        event_type = RejectedEvent if filtered else None
        bus.subscribe(callback, event_type=event_type, routing_key=routing_key)
    return bus


def _ignore_event(_event: RoutingEvent) -> None:
    return None


def _routing_cases(
    runner,
    implementation: str,
    merge: Callable[[dict[int, object], dict[int, object]], tuple[object, ...]],
) -> None:
    for count in (64, 512, 2_048):
        bus = _subscribe_fixture(count, merge=merge, filtered=False)
        try:
            selected = bus._subscriber_snapshot(3)
            bench_sync(
                runner,
                f"routing_snapshot/{count}",
                lambda bus=bus: bus._subscriber_snapshot(3),
                metadata={
                    "candidate_implementation": implementation,
                    "case": "event_bus_routing_snapshot",
                    "subscriptions": count,
                    "selected_subscriptions": len(selected),
                    "wildcard_fraction": 0.2,
                    "key_count": 7,
                },
            )
        finally:
            bus.close()

        if implementation == "python":
            production_bus = _subscribe_fixture(count, merge=None, filtered=False)
            try:
                production_snapshot = production_bus._subscriber_snapshot(3)
                bench_sync(
                    runner,
                    f"routing_snapshot_production/{count}",
                    lambda bus=production_bus: bus._subscriber_snapshot(3),
                    metadata={
                        "candidate_implementation": "production_python",
                        "case": "event_bus_production_routing_snapshot",
                        "subscriptions": count,
                        "selected_subscriptions": len(production_snapshot),
                        "wildcard_fraction": 0.2,
                        "key_count": 7,
                    },
                )
            finally:
                production_bus.close()

    event = RoutingEvent(3, 1)
    for count in (64, 512, 2_048):
        for filtered in (False, True):
            bus = _subscribe_fixture(count, merge=merge, filtered=filtered)
            try:
                selected = len(bus._subscriber_snapshot(3))
                result = bus.publish(event)
                expected_matched = 0 if filtered else selected
                expected_delivered = 0 if filtered else selected
                if (result.matched, result.delivered) != (expected_matched, expected_delivered):
                    raise AssertionError(
                        f"{implementation} routing candidate changed publish counts: {result}"
                    )
                case = "publish_filtered" if filtered else "publish_noop_callbacks"
                bench_sync(
                    runner,
                    f"routing_publish/{case}/{count}",
                    lambda bus=bus, event=event: bus.publish(event),
                    metadata={
                        "candidate_implementation": implementation,
                        "case": "complete_event_bus_publish",
                        "subscriptions": count,
                        "matched_callbacks": result.matched,
                        "selected_route_subscriptions": selected,
                        "wildcard_fraction": 0.2,
                        "key_count": 7,
                    },
                )
            finally:
                bus.close()

            if implementation == "python":
                production_bus = _subscribe_fixture(count, merge=None, filtered=filtered)
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
                        f"routing_publish_production/{case}/{count}",
                        lambda bus=production_bus, event=event: bus.publish(event),
                        metadata={
                            "candidate_implementation": "production_python",
                            "case": "production_event_bus_publish",
                            "subscriptions": count,
                            "matched_callbacks": production_result.matched,
                            "selected_route_subscriptions": selected,
                            "wildcard_fraction": 0.2,
                            "key_count": 7,
                        },
                    )
                finally:
                    production_bus.close()


def main() -> int:
    runner = create_runner(
        "EventFlow Python/Cython/C candidate benchmarks",
        add_cmdline_args=forward_cli_arguments("implementation"),
    )
    runner.argparser.add_argument(
        "--implementation", choices=("python", "cython", "c"), default="python"
    )
    args = runner.parse_args()
    gil_check = getattr(sys, "_is_gil_enabled", None)
    gil_was_disabled = gil_check is not None and not gil_check()
    filter_by_type, merge = _kernels(args.implementation)
    if gil_was_disabled and gil_check is not None and gil_check():
        raise RuntimeError(
            f"importing the {args.implementation} candidate re-enabled the free-threaded GIL"
        )
    _history_cases(runner, args.implementation, filter_by_type)
    _routing_cases(runner, args.implementation, merge)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
