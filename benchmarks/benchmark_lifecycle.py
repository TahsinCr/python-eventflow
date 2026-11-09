"""Benchmark callbacks, scopes, task ownership, single-flight and work submission."""

from __future__ import annotations

import asyncio

from eventflow.callback import CallbackSubscription
from eventflow.concurrency import SingleFlight
from eventflow.executors import BoundedWorkPool
from eventflow.scopes import AsyncResourceScope, ResourceScope, SubscriptionScope
from eventflow.tasks import AsyncTaskScope

from ._runner import bench_async, bench_sync, create_runner


def _identity(value: int) -> int:
    return value


async def _async_identity(value: int) -> int:
    return value


def main() -> None:
    runner = create_runner(__doc__ or "EventFlow lifecycle benchmarks")
    runner.parse_args()

    callback = CallbackSubscription(1, lambda: None, _identity)
    bench_sync(runner, "callback/plain-invoke", lambda: callback.invoke(1))

    hooked_callback = CallbackSubscription(
        2,
        lambda: None,
        _identity,
        predicate=lambda event: event > 0,
        on_before=lambda _context: None,
        on_success=lambda _context: None,
        on_after=lambda _context: None,
    )
    bench_sync(runner, "callback/predicate-hooks-invoke", lambda: hooked_callback.invoke(1))

    async_callback = CallbackSubscription(3, lambda: None, _async_identity, asynchronous=True)
    bench_async(runner, "callback/async-invoke", lambda: async_callback.invoke_async(1))

    def resource_scope() -> int:
        with ResourceScope() as scope:
            scope.defer(lambda: None)
            return scope.pending_cleanup()

    bench_sync(runner, "resource-scope/defer-close", resource_scope)

    def subscription_scope() -> int:
        with SubscriptionScope() as scope:
            scope.add(CallbackSubscription(4, lambda: None, _identity))
            return 1

    bench_sync(runner, "subscription-scope/add-close", subscription_scope)

    async def async_resource_scope() -> int:
        async with AsyncResourceScope() as scope:
            scope.defer(lambda: None)
            return scope.pending_cleanup()

    bench_async(runner, "async-resource-scope/defer-close", async_resource_scope)

    with BoundedWorkPool(max_workers=1, max_pending=1) as pool:
        bench_sync(
            runner,
            "bounded-work-pool/submit-result",
            lambda: pool.submit(_identity, 1).result(),
        )

    async def task_scope_roundtrip() -> int:
        async with AsyncTaskScope() as scope:
            handle = scope.start(lambda: _async_identity(1), shutdown="drain")
            return await handle.join()

    bench_async(runner, "async-task-scope/start-join-close", task_scope_roundtrip)

    async def singleflight_burst() -> tuple[int, ...]:
        singleflight = SingleFlight[str]()

        async def factory() -> int:
            await asyncio.sleep(0)
            return 1

        try:
            return tuple(
                await asyncio.gather(*(singleflight.run("shared", factory) for _ in range(8)))
            )
        finally:
            await singleflight.aclose()

    bench_async(
        runner,
        "single-flight/same-key-eight-waiters",
        singleflight_burst,
        metadata={"waiters": 8},
    )


if __name__ == "__main__":
    main()
