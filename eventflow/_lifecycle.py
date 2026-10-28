"""Wait for owned cleanup even when the caller is cancelled repeatedly."""

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")


async def finish_cleanup(operation: Awaitable[T]) -> T:
    task = asyncio.ensure_future(operation)
    if task is asyncio.current_task():
        raise RuntimeError("Cleanup cannot await itself")
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if task.cancelled():
                raise
            cancellation = exc
        except BaseException:
            # Retrieve the cleanup result below, preserving caller cancellation.
            break
    try:
        result = task.result()
    except BaseException as error:
        if cancellation is not None:
            raise cancellation from error
        raise
    if cancellation is not None:
        raise cancellation
    return result
