"""Check native kernel results against the Python reference before timing."""

from __future__ import annotations

import random
import sys
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
from threading import Barrier
from typing import cast

from benchmarks.native._contracts import FilterKernel, MergeKernel
from benchmarks.native.python_kernels import filter_by_type, merge_subscribers
from eventflow.engine import EventEngine
from eventflow.event_bus import EventBus
from eventflow.filtering import EventFilter
from eventflow.history import MemoryEventHistory


def _load(name: str) -> tuple[FilterKernel, dict[str, MergeKernel]]:
    module_names = {
        "cython": "eventflow_native_cython",
        "c": "eventflow_native_c",
    }
    module_name = module_names.get(name)
    if module_name is None:
        raise ValueError(f"unsupported native implementation: {name}")
    module = import_module(module_name)
    merges = {"merge_subscribers": cast(MergeKernel, module.merge_subscribers)}
    if name == "cython":
        merges["merge_subscribers_items"] = cast(MergeKernel, module.merge_subscribers_items)
    return cast(FilterKernel, module.filter_by_type), merges


class RoutingEvent:
    __slots__ = ("key",)

    def __init__(self, key: int) -> None:
        self.key = key


class CandidateBus(EventBus[RoutingEvent]):
    def __init__(self, merge: MergeKernel) -> None:
        super().__init__(routing_key=lambda event: event.key)
        self._merge = merge

    def _subscriber_snapshot(self, event_key: object) -> tuple[object, ...]:
        unkeyed = self._unkeyed_subscribers
        keyed = self._keyed_subscribers.get(event_key, {})
        if not unkeyed:
            return tuple(keyed.values())
        if not keyed:
            return tuple(unkeyed.values())
        return self._merge(
            cast(dict[int, object], unkeyed),
            cast(dict[int, object], keyed),
        )


def _validate_concurrent_bus(merge: MergeKernel, name: str) -> None:
    bus = CandidateBus(merge)
    engine = EventEngine()
    engine.add("native-validation", bus)
    engine.start()

    def callback(_event: RoutingEvent) -> None:
        return None

    for key in (None, 0, 1, 2) * 3:
        engine.subscribe("native-validation", callback, routing_key=key)
    barrier = Barrier(5)

    def publish_many() -> int:
        barrier.wait()
        for _ in range(250):
            result = engine.publish("native-validation", RoutingEvent(1))
            # Cancellation may deactivate a transient snapshot member before delivery.
            if result.matched < 6 or result.delivered < 6 or result.delivered > result.matched:
                raise AssertionError(
                    f"{name} concurrent publish lost a stable subscriber: {result!r}"
                )
        return 250

    def churn_subscriptions() -> int:
        barrier.wait()
        for index in range(500):
            subscription = engine.subscribe("native-validation", callback, routing_key=index % 3)
            subscription.cancel()
            if subscription.active:
                raise AssertionError(f"{name} could not cancel a churned subscription")
        return 500

    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(publish_many) for _ in range(4)]
            futures.append(executor.submit(churn_subscriptions))
            totals = [future.result() for future in futures]
        if totals != [250, 250, 250, 250, 500]:
            raise AssertionError(f"{name} concurrent routing work did not complete")
    finally:
        engine.stop()
        if not bus.closed:
            raise AssertionError(f"{name} engine shutdown left its bus open")


def validate(name: str) -> int:
    gil_check = getattr(sys, "_is_gil_enabled", None)
    gil_was_disabled = gil_check is not None and not gil_check()
    native_filter, native_merges = _load(name)
    if gil_was_disabled and gil_check is not None and gil_check():
        raise RuntimeError(f"importing {name} enabled the free-threaded GIL")

    class Event:
        __slots__ = ()

    class Selected(Event):
        __slots__ = ()

    class SelectedChild(Selected):
        __slots__ = ()

    events = (Event(), Selected(), SelectedChild(), Event(), SelectedChild())
    expected_filter = filter_by_type(events, Selected)
    actual_filter = native_filter(events, Selected)
    if actual_filter != expected_filter or actual_filter[1] is not events[2]:
        raise AssertionError(f"{name} type filter differs from Python reference")
    for event_type in (Selected, (Selected, Event)):
        for limit in (None, 1, 2, 10, 10**100):
            expected = filter_by_type(events, event_type, limit)
            actual = native_filter(events, event_type, limit)
            if len(actual) != len(expected) or any(
                item is not reference for item, reference in zip(actual, expected, strict=True)
            ):
                raise AssertionError(f"{name} type filter changed tuple or limit semantics")
    for invalid_limit in (0, -1, True):
        try:
            native_filter(events, Selected, invalid_limit)
        except ValueError:
            pass
        else:
            raise AssertionError(f"{name} accepted an invalid type-filter limit")
    history = MemoryEventHistory[Event](capacity=len(events))
    for event in events:
        history.append(event)
    production_query = history.query(EventFilter[Event](event_type=Selected))
    if len(actual_filter) != len(production_query) or any(
        actual is not expected
        for actual, expected in zip(actual_filter, production_query, strict=True)
    ):
        raise AssertionError(f"{name} type filter differs from MemoryEventHistory.query")

    randomizer = random.Random(417_031)
    checks = 0
    for kernel_name, native_merge in native_merges.items():
        for size in (0, 1, 2, 16, 127, 1024):
            for _ in range(20):
                identifiers = randomizer.sample(range(1, max(size * 3, 3) + 1), size)
                split = identifiers[::2]
                left = {identifier: object() for identifier in sorted(split)}
                right = {
                    identifier: object()
                    for identifier in sorted(set(identifiers).difference(split))
                }
                expected = merge_subscribers(left, right)
                actual = native_merge(left, right)
                if actual != expected or any(
                    item is not reference for item, reference in zip(actual, expected, strict=True)
                ):
                    raise AssertionError(
                        f"{name}/{kernel_name} ordered merge differs at input size {size}"
                    )
                checks += 1

        large_id = 10**200
        left = {1: object(), large_id: object(), large_id + 4: object()}
        right = {2: object(), large_id + 2: object()}
        expected = merge_subscribers(left, right)
        actual = native_merge(left, right)
        if any(item is not reference for item, reference in zip(actual, expected, strict=True)):
            raise AssertionError(f"{name}/{kernel_name} failed unbounded integer ordering")
        checks += 1
        _validate_concurrent_bus(native_merge, f"{name}/{kernel_name}")

    print(
        f"{name}: type-filter limits/tuples, {checks} ordered-merge cases, "
        "and concurrent publish/churn passed"
    )
    return 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("implementation", choices=("c", "cython"))
    arguments = parser.parse_args()
    raise SystemExit(validate(arguments.implementation))
