"""Reference kernels with the same inputs and outputs as the native candidates."""

from __future__ import annotations

import heapq
from collections.abc import Mapping
from typing import TypeVar

T = TypeVar("T")


def filter_by_type(events: tuple[object, ...], event_type: type[T]) -> tuple[T, ...]:
    return tuple(event for event in events if isinstance(event, event_type))


def merge_subscribers(unkeyed: Mapping[int, T], keyed: Mapping[int, T]) -> tuple[T, ...]:
    if not unkeyed:
        return tuple(keyed.values())
    if not keyed:
        return tuple(unkeyed.values())
    return tuple(value for _identifier, value in heapq.merge(unkeyed.items(), keyed.items()))
