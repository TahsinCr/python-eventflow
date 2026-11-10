"""Check native kernel results against the Python reference before timing."""

from __future__ import annotations

import random
import sys
from collections.abc import Callable

from benchmarks.native.python_kernels import filter_by_type, merge_subscribers
from eventflow.filtering import EventFilter
from eventflow.history import MemoryEventHistory


def _load(name: str) -> tuple[Callable[..., object], Callable[..., object]]:
    if name == "cython":
        from eventflow_native_cython import filter_by_type as native_filter
        from eventflow_native_cython import merge_subscribers as native_merge
    elif name == "c":
        from eventflow_native_c import filter_by_type as native_filter
        from eventflow_native_c import merge_subscribers as native_merge
    else:
        raise ValueError(f"unsupported native implementation: {name}")
    return native_filter, native_merge


def validate(name: str) -> int:
    gil_check = getattr(sys, "_is_gil_enabled", None)
    gil_was_disabled = gil_check is not None and not gil_check()
    native_filter, native_merge = _load(name)
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
    for size in (0, 1, 2, 16, 127, 1024):
        for _ in range(20):
            identifiers = randomizer.sample(range(1, max(size * 3, 3) + 1), size)
            split = identifiers[::2]
            left = {identifier: object() for identifier in sorted(split)}
            right = {
                identifier: object() for identifier in sorted(set(identifiers).difference(split))
            }
            expected = merge_subscribers(left, right)
            actual = native_merge(left, right)
            if actual != expected or any(a is not b for a, b in zip(actual, expected, strict=True)):
                raise AssertionError(f"{name} ordered merge differs at input size {size}")
            checks += 1

    print(f"{name}: type-filter semantics and {checks} ordered-merge cases passed")
    return 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("implementation", choices=("c", "cython"))
    arguments = parser.parse_args()
    raise SystemExit(validate(arguments.implementation))
