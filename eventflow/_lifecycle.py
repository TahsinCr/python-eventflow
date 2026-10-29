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
    if not task.done():
        completed = asyncio.Event()
        task.add_done_callback(lambda _task: completed.set())
        while not task.done():
            try:
                await completed.wait()
            except asyncio.CancelledError as exc:
                cancellation = exc
    try:
        result = task.result()
    except BaseException as error:
        if cancellation is not None:
            raise cancellation from error
        raise
    if cancellation is not None:
        raise cancellation
    return result
