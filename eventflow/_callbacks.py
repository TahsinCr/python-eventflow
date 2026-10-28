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


def synchronous_result(result: T) -> T:
    if result is not None and inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError("Synchronous callback returned an awaitable")
    return result
