"""Owned asyncio tasks and per-key latest-result coordination."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Hashable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Generic, Literal, TypeVar, cast

from ._callbacks import close_awaitable
from ._lifecycle import bind_running_loop, finish_cleanup, request_task_cancellation
from .base import validate_nonnegative_timeout
from .compatibility import BaseExceptionGroup, ExceptionGroup
from .observability import OperationStats, _require_operation_stats

T = TypeVar("T")
K = TypeVar("K", bound=Hashable)
TaskOperation = Callable[[], Awaitable[T]] | Awaitable[T]
__all__ = [
    "AsyncTaskScope",
    "TaskHandle",
    "TaskScopeSnapshot",
    "LatestTaskCoordinator",
    "LatestTaskSnapshot",
]


@dataclass(frozen=True, slots=True)
class TaskScopeSnapshot:
    active: int
    completed: int
    failed: int
    cancelled: int
    closing: bool


class TaskHandle(Generic[T]):
    """A caller-owned view of an asyncio task; cancellation is only a request."""

    def __init__(self, task: asyncio.Task[T], name: str, loop: asyncio.AbstractEventLoop) -> None:
        self._task = task
        self.name = name
        self._loop = loop

    @property
    def task(self) -> asyncio.Task[T]:
        return self._task

    @property
    def done(self) -> bool:
        return self._task.done()

    def cancel(self) -> bool:
        return request_task_cancellation(self._task, self._loop)

    async def join(self, timeout: float | None = None) -> T:
        """Wait without cancelling the task when this caller times out."""
        timeout = validate_nonnegative_timeout(timeout, name="task join timeout")
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("TaskHandle belongs to a different event loop")
        done, _ = await asyncio.wait((self._task,), timeout=timeout)
        if not done:
            raise TimeoutError(f"Task {self.name!r} did not finish before the timeout")
        return self._task.result()


class AsyncTaskScope:
    """Own background tasks on one loop and cancel or drain them during shutdown."""

    def __init__(
        self,
        *,
        on_error: Callable[[TaskHandle[Any], BaseException], object] | None = None,
        observer: OperationStats | None = None,
    ) -> None:
        if on_error is not None and not callable(on_error):
            raise TypeError("on_error must be callable")
        _require_operation_stats(observer)
        self._on_error = on_error
        self._observer = observer
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: dict[
            asyncio.Task[Any], tuple[TaskHandle[Any], Literal["cancel", "drain"], float]
        ] = {}
        self._closed = False
        self._closing: asyncio.Task[None] | None = None
        self._completed = 0
        self._failed = 0
        self._cancelled = 0

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "AsyncTaskScope")
        self._loop = loop
        return loop

    @property
    def closed(self) -> bool:
        return self._closed

    def start(
        self,
        operation: TaskOperation[T],
        *,
        name: str | None = None,
        shutdown: Literal["cancel", "drain"] = "cancel",
    ) -> TaskHandle[T]:
        """Start an awaitable or zero-argument async factory owned by this scope."""
        if shutdown not in ("cancel", "drain"):
            close_awaitable(operation)
            raise ValueError("shutdown must be 'cancel' or 'drain'")
        if not callable(operation) and not inspect.isawaitable(operation):
            raise TypeError("operation must be an awaitable or async factory")
        try:
            loop = self._bind_loop()
        except BaseException:
            close_awaitable(operation)
            raise
        if self._closed:
            close_awaitable(operation)
            raise RuntimeError("Cannot start a task in a closed scope")
        task_name = "EventFlowTask" if name is None else name
        if not isinstance(task_name, str) or not task_name:
            close_awaitable(operation)
            raise ValueError("task name must be a non-empty string")

        started = False

        async def run() -> T:
            nonlocal started
            started = True
            result = operation() if callable(operation) else operation
            if not inspect.isawaitable(result):
                raise TypeError("task factory must return an awaitable")
            return await cast(Awaitable[T], result)

        coroutine = run()
        try:
            task = loop.create_task(coroutine, name=task_name)
        except BaseException:
            coroutine.close()
            close_awaitable(operation)
            raise
        handle = TaskHandle(task, task_name, loop)
        self._tasks[task] = (cast(TaskHandle[Any], handle), shutdown, loop.time())

        def task_done(completed: asyncio.Task[Any]) -> None:
            if not started and not callable(operation):
                close_awaitable(operation)
            self._task_done(completed)

        task.add_done_callback(task_done)
        return handle

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        record = self._tasks.pop(task, None)
        if record is None:
            return
        loop = self._loop
        assert loop is not None
        handle, _policy, started = record
        self._completed += 1
        if task.cancelled():
            self._cancelled += 1
            if self._observer is not None:
                self._observer.record(
                    "task_scope.task", status="cancelled", elapsed=loop.time() - started
                )
            return
        error = task.exception()
        if error is None:
            if self._observer is not None:
                self._observer.record("task_scope.task", elapsed=loop.time() - started)
            return
        self._failed += 1
        if self._observer is not None:
            self._observer.record("task_scope.task", status="error", elapsed=loop.time() - started)
        if self._on_error is not None:
            try:
                result = self._on_error(handle, error)
                if inspect.isawaitable(result):
                    close_awaitable(result)
                    raise TypeError("AsyncTaskScope on_error callback must be synchronous")
                return
            except BaseException as callback_error:
                error = BaseExceptionGroup(
                    "Task and task error observer both failed", [error, callback_error]
                )
        loop.call_exception_handler(
            {
                "message": f"Unhandled failure in EventFlow task {handle.name!r}",
                "exception": error,
                "task": task,
            }
        )

    def cancel(self, handle: TaskHandle[Any] | None = None) -> int:
        """Request cancellation of one task or every task in the scope."""
        self._bind_loop()
        if handle is not None:
            if handle.task not in self._tasks:
                return 0
            return int(handle.cancel())
        return sum(int(task.cancel()) for task in tuple(self._tasks))

    async def join(self, timeout: float | None = None) -> None:
        """Wait for all tasks without changing their cancellation policy."""
        self._bind_loop()
        if asyncio.current_task() in self._tasks:
            raise RuntimeError("An owned task cannot join its own scope")
        timeout = validate_nonnegative_timeout(timeout, name="task scope join timeout")
        pending = tuple(self._tasks)
        if not pending:
            return
        _, unfinished = await asyncio.wait(pending, timeout=timeout)
        if unfinished:
            raise TimeoutError(f"{len(unfinished)} EventFlow task(s) are still running")

    async def _close_tasks(self, timeout: float | None) -> None:
        records = tuple(self._tasks.items())
        for task, (_, policy, _started) in records:
            if policy == "cancel" and not task.done():
                task.cancel()
        tasks = tuple(task for task, _ in records if not task.done())
        if tasks:
            _, unfinished = await asyncio.wait(tasks, timeout=timeout)
            if unfinished:
                raise TimeoutError(
                    f"{len(unfinished)} EventFlow task(s) remain after shutdown request"
                )

    async def aclose(self, timeout: float | None = None) -> None:
        """Stop accepting tasks and wait for cancel/drain policies to complete.

        A timeout reports tasks that are still running and leaves them owned so a
        later call can retry. Cancellation of this caller is delayed until the
        shared cleanup task finishes, matching the resource-scope contract.
        """
        self._bind_loop()
        timeout = validate_nonnegative_timeout(timeout, name="task scope close timeout")
        current = asyncio.current_task()
        if current is self._closing or current in self._tasks:
            raise RuntimeError("An owned task cannot close its own scope")
        self._closed = True
        if self._closing is None or (
            self._closing.done()
            and (self._tasks or self._closing.cancelled() or self._closing.exception() is not None)
        ):
            self._closing = asyncio.create_task(
                self._close_tasks(timeout), name="EventFlowTaskScopeClose"
            )
        await finish_cleanup(self._closing)

    def snapshot(self) -> TaskScopeSnapshot:
        self._bind_loop()
        return TaskScopeSnapshot(
            len(self._tasks), self._completed, self._failed, self._cancelled, self._closed
        )

    async def __aenter__(self) -> AsyncTaskScope:
        self._bind_loop()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()


@dataclass(frozen=True, slots=True)
class LatestTaskSnapshot:
    active: Mapping[Hashable, int]
    completed: int
    discarded_results: int


class LatestTaskCoordinator(Generic[K]):
    """Keep only the newest result applicable for each key."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._versions: dict[K, int] = {}
        self._tasks: dict[K, asyncio.Task[Any]] = {}
        self._outstanding: dict[K, set[asyncio.Task[Any]]] = {}
        self._closed = False
        self._completed = 0
        self._discarded = 0

    def _bind_loop(self) -> asyncio.AbstractEventLoop:
        loop = bind_running_loop(self._loop, "LatestTaskCoordinator")
        self._loop = loop
        return loop

    def submit(
        self,
        key: K,
        factory: Callable[[], Awaitable[T]],
        on_result: Callable[[T], object],
        *,
        cancel_previous: bool = True,
        on_error: Callable[[BaseException], object] | None = None,
        name: str | None = None,
    ) -> asyncio.Task[None]:
        """Schedule keyed work and apply its result only while its version is current."""
        loop = self._bind_loop()
        if self._closed:
            raise RuntimeError("Cannot submit work to a closed coordinator")
        if not callable(factory) or not callable(on_result):
            raise TypeError("factory and on_result must be callable")
        if on_error is not None and not callable(on_error):
            raise TypeError("on_error must be callable")
        if not isinstance(cancel_previous, bool):
            raise TypeError("cancel_previous must be a bool")
        previous = self._tasks.get(key)
        version = self._versions.get(key, 0) + 1
        self._versions[key] = version
        if previous is not None and cancel_previous:
            previous.cancel()

        async def run() -> None:
            try:
                value = await factory()
                if self._versions.get(key) != version or self._closed:
                    self._discarded += 1
                    return
                applied = on_result(value)
                if inspect.isawaitable(applied):
                    await applied
                self._completed += 1
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if on_error is not None:
                    try:
                        observed = on_error(error)
                        if inspect.isawaitable(observed):
                            await observed
                    except Exception as observer_error:
                        raise ExceptionGroup(
                            "Latest task and error observer both failed",
                            [error, observer_error],
                        ) from error
                else:
                    raise
            finally:
                current = asyncio.current_task()
                if self._tasks.get(key) is current:
                    self._tasks.pop(key, None)
                outstanding = self._outstanding.get(key)
                if outstanding is not None:
                    outstanding.discard(cast(asyncio.Task[Any], current))
                    if not outstanding:
                        self._outstanding.pop(key, None)
                        self._versions.pop(key, None)

        task = loop.create_task(run(), name=name or f"EventFlowLatest:{key!s}")
        self._tasks[key] = task
        self._outstanding.setdefault(key, set()).add(task)
        return task

    def cancel(self, key: K) -> bool:
        self._bind_loop()
        task = self._tasks.get(key)
        if task is None:
            return False
        self._versions[key] = self._versions.get(key, 0) + 1
        return task.cancel()

    async def aclose(self, timeout: float | None = None) -> None:
        self._bind_loop()
        current = asyncio.current_task()
        if any(current in tasks for tasks in self._outstanding.values()):
            raise RuntimeError("An owned task cannot close its own coordinator")
        timeout = validate_nonnegative_timeout(timeout, name="coordinator close timeout")
        self._closed = True
        tasks = tuple(task for grouped in self._outstanding.values() for task in grouped)
        for task in tasks:
            task.cancel()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=timeout)
            if pending:
                raise TimeoutError(f"{len(pending)} latest task(s) remain after cancellation")

    def snapshot(self) -> LatestTaskSnapshot:
        self._bind_loop()
        active = {key: self._versions[key] for key in self._tasks if key in self._versions}
        return LatestTaskSnapshot(MappingProxyType(active), self._completed, self._discarded)
