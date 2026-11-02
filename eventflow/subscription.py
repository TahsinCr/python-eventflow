"""Subscription ownership for synchronous and asynchronous event buses."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable


class Subscription:
    def __init__(self, subscription_id: int, cancel: Callable[[], None]) -> None:
        self._id = subscription_id
        self._cancel = cancel
        self._active = True
        self._lock = threading.Lock()
        self._cancel_condition = threading.Condition(self._lock)
        self._cancel_owner: int | None = None

    @property
    def id(self) -> int:
        return self._id

    @property
    def active(self) -> bool:
        with self._lock:
            return self._active

    def _deactivate(self) -> bool:
        with self._lock:
            if not self._active:
                return False
            self._active = False
            return True

    def cancel(self) -> None:
        self._consume()

    def _consume(self) -> bool:
        current_thread = threading.get_ident()
        with self._cancel_condition:
            while self._cancel_owner is not None:
                if self._cancel_owner == current_thread:
                    return False
                self._cancel_condition.wait()
            if not self._active:
                return False
            self._active = False
            self._cancel_owner = current_thread
        try:
            self._cancel()
        except BaseException:
            with self._cancel_condition:
                self._active = True
                self._cancel_owner = None
                self._cancel_condition.notify_all()
            raise
        with self._cancel_condition:
            self._cancel_owner = None
            self._cancel_condition.notify_all()
        return True

    def __enter__(self) -> "Subscription":
        return self

    def __exit__(self, *_args: object) -> None:
        self.cancel()


class AsyncSubscription:
    def __init__(
        self,
        subscription_id: int,
        cancel: Callable[[], Awaitable[None]],
    ) -> None:
        self._id = subscription_id
        self._cancel = cancel
        self._active = True
        self._cancel_completion: asyncio.Future[BaseException | None] | None = None
        self._cancel_owner: asyncio.Task | None = None

    @property
    def id(self) -> int:
        return self._id

    @property
    def active(self) -> bool:
        return self._active

    def _deactivate(self) -> bool:
        if not self._active:
            return False
        self._active = False
        return True

    async def cancel(self) -> None:
        await self._consume()

    async def _consume(self) -> bool:
        completion = self._cancel_completion
        if not self._active:
            if completion is None:
                return False
            if self._cancel_owner is asyncio.current_task():
                return False
            error = await asyncio.shield(completion)
            if error is not None:
                raise error
            return False

        self._active = False
        completion = asyncio.get_running_loop().create_future()
        self._cancel_completion = completion
        self._cancel_owner = asyncio.current_task()
        try:
            await self._cancel()
        except BaseException as error:
            self._active = True
            self._cancel_completion = None
            self._cancel_owner = None
            completion.set_result(error)
            raise
        self._cancel_completion = None
        self._cancel_owner = None
        completion.set_result(None)
        return True

    async def __aenter__(self) -> "AsyncSubscription":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.cancel()
