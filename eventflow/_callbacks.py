"""Callback guards shared by opt-in execution and lifecycle components."""

import inspect
from collections.abc import Awaitable
from types import CoroutineType
from typing import Any, TypeGuard, TypeVar

T = TypeVar("T")
_NON_AWAITABLE_TYPES = frozenset(
    {
        type(None),
        bool,
        int,
        float,
        str,
        bytes,
        bytearray,
        complex,
        tuple,
        list,
        dict,
        set,
        frozenset,
    }
)


def is_awaitable(value: object) -> TypeGuard[Awaitable[Any]]:
    return type(value) is CoroutineType or inspect.isawaitable(value)


def is_async_callable(callback: object) -> bool:
    if not callable(callback):
        return False
    return inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(
        type(callback).__call__
    )


def close_awaitable(value: object) -> None:
    """Close an awaitable when it exposes a close method."""
    close = getattr(value, "close", None)
    if callable(close):
        close()


def synchronous_result(result: T) -> T:
    if result is None or type(result) in _NON_AWAITABLE_TYPES:
        return result
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            close_awaitable(result)
        raise TypeError("Synchronous callback returned an awaitable")
    return result
