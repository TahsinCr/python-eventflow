"""Callback guards shared by opt-in execution and lifecycle components."""

import inspect
from typing import TypeVar

T = TypeVar("T")


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
    if result is not None and inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            close_awaitable(result)
        raise TypeError("Synchronous callback returned an awaitable")
    return result
