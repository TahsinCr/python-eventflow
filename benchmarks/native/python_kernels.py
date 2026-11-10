"""Reference kernels with the same inputs and outputs as the native candidates."""

from __future__ import annotations

import heapq
from collections.abc import Mapping
from typing import TypeVar

T = TypeVar("T")


def filter_by_type(
    events: tuple[object, ...],
    event_type: type[object] | tuple[type[object], ...],
    limit: int | None = None,
) -> tuple[object, ...]:
    if limit is None:
        return tuple(event for event in events if isinstance(event, event_type))
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("history query limit must be a positive integer")

    matched: list[object] = []
    for event in reversed(events):
        if isinstance(event, event_type):
            matched.append(event)
            if len(matched) == limit:
                break
    matched.reverse()
    return tuple(matched)


def merge_subscribers(unkeyed: Mapping[int, T], keyed: Mapping[int, T]) -> tuple[T, ...]:
    if not unkeyed:
        return tuple(keyed.values())
    if not keyed:
        return tuple(unkeyed.values())
    return tuple(value for _identifier, value in heapq.merge(unkeyed.items(), keyed.items()))


def merge_subscribers_two_pointer(
    unkeyed: Mapping[int, T], keyed: Mapping[int, T]
) -> tuple[T, ...]:
    if not unkeyed:
        return tuple(keyed.values())
    if not keyed:
        return tuple(unkeyed.values())

    left_items = iter(unkeyed.items())
    right_items = iter(keyed.items())
    left = next(left_items, None)
    right = next(right_items, None)
    merged: list[T] = []
    while left is not None and right is not None:
        if left[0] <= right[0]:
            merged.append(left[1])
            left = next(left_items, None)
        else:
            merged.append(right[1])
            right = next(right_items, None)
    if left is not None:
        merged.append(left[1])
        merged.extend(value for _identifier, value in left_items)
    if right is not None:
        merged.append(right[1])
        merged.extend(value for _identifier, value in right_items)
    return tuple(merged)


def merge_subscribers_sorted(unkeyed: Mapping[int, T], keyed: Mapping[int, T]) -> tuple[T, ...]:
    pairs = sorted((*unkeyed.items(), *keyed.items()))
    return tuple(value for _identifier, value in pairs)
