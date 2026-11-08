"""Share event-loop ownership and cleanup operations across async components."""

import asyncio
import inspect
from collections.abc import Awaitable, Coroutine
from typing import Any, TypeVar

from ._compatibility import BaseExceptionGroup, ExceptionGroup

T = TypeVar("T")


def create_task_safely(
    coroutine: Coroutine[Any, Any, T],
    *,
    name: str | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
) -> asyncio.Task[T]:
    """Close a fresh coroutine if its event loop rejects task creation."""
    try:
        if loop is None:
            return asyncio.create_task(coroutine, name=name)
        return loop.create_task(coroutine, name=name)
    except BaseException:
        coroutine.close()
        raise


def ensure_future_safely(operation: Awaitable[T]) -> asyncio.Future[T]:
    """Close a coroutine if its event loop rejects scheduling it."""
    try:
        return asyncio.ensure_future(operation)
    except BaseException:
        if inspect.iscoroutine(operation):
            operation.close()
        raise


def bind_running_loop(
    current: asyncio.AbstractEventLoop | None, owner: str
) -> asyncio.AbstractEventLoop:
    """Bind to the active loop or reject use from another loop."""
    loop = asyncio.get_running_loop()
    if current is not None and current is not loop:
        raise RuntimeError(f"{owner} belongs to a different event loop")
    return loop


def request_task_cancellation(task: asyncio.Task[T], loop: asyncio.AbstractEventLoop) -> bool:
    """Request cancellation on the task's loop; return false when it cannot be queued."""
    if task.done():
        return False
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running is loop:
        return task.cancel()
    try:
        loop.call_soon_threadsafe(task.cancel)
    except RuntimeError:
        return False
    return True


def raise_cleanup_errors(
    errors: list[BaseException],
    *,
    message: str,
    exclude_repeated_cause: bool = False,
) -> None:
    """Raise all cleanup failures without hiding cancellation-like exceptions."""
    if not errors:
        return
    for index, error in enumerate(errors):
        if not isinstance(error, Exception):
            remaining = (
                [candidate for candidate in errors if candidate is not error]
                if exclude_repeated_cause
                else errors[:index] + errors[index + 1 :]
            )
            if remaining:
                raise error from BaseExceptionGroup(message, remaining)
            raise error
    raise ExceptionGroup(message, [error for error in errors if isinstance(error, Exception)])


async def finish_cleanup(operation: Awaitable[T]) -> T:
    task = ensure_future_safely(operation)
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


async def finish_cleanup_errors(
    cleanup: asyncio.Task[list[BaseException]], *, message: str
) -> list[BaseException]:
    try:
        return await finish_cleanup(cleanup)
    except asyncio.CancelledError as cancellation:
        if cleanup.done() and not cleanup.cancelled():
            errors = cleanup.result()
            if errors:
                raise cancellation from BaseExceptionGroup(message, errors)
        raise
