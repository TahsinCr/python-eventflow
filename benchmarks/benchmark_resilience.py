"""Benchmark retry, cooldown and circuit-breaker hot paths."""

from __future__ import annotations

from eventflow.resilience import CircuitBreaker, CooldownRegistry, RetryPolicy

from ._runner import bench_async, bench_sync, create_runner


def main() -> None:
    runner = create_runner(__doc__ or "EventFlow resilience benchmarks")
    runner.parse_args()

    retry = RetryPolicy(max_attempts=1, initial_delay=0)
    bench_sync(runner, "retry/sync-success", lambda: retry.run(lambda: 1))

    retry_twice = RetryPolicy(max_attempts=2, initial_delay=0)

    def sync_retry_once() -> int:
        attempts = 0

        def operation() -> int:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ValueError("retry benchmark")
            return attempts

        return retry_twice.run(operation, should_retry=lambda error: isinstance(error, ValueError))

    bench_sync(runner, "retry/sync-one-retry", sync_retry_once)

    cooldown = CooldownRegistry[str]()
    cooldown.block("active", 3600)
    bench_sync(runner, "cooldown/remaining", lambda: cooldown.remaining("active"))

    def block_and_clear() -> bool:
        cooldown.block("transient", 3600)
        return cooldown.clear("transient")

    bench_sync(runner, "cooldown/block-clear", block_and_clear)

    breaker = CircuitBreaker(failure_threshold=100)
    bench_sync(runner, "circuit-breaker/sync-success", lambda: breaker.call(lambda: 1))

    async_retry = RetryPolicy(max_attempts=1, initial_delay=0)

    async def async_value() -> int:
        return 1

    bench_async(runner, "retry/async-success", lambda: async_retry.run_async(async_value))

    async_retry_twice = RetryPolicy(max_attempts=2, initial_delay=0)

    async def async_retry_once() -> int:
        attempts = 0

        async def operation() -> int:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ValueError("retry benchmark")
            return attempts

        return await async_retry_twice.run_async(
            operation,
            should_retry=lambda error: isinstance(error, ValueError),
        )

    bench_async(runner, "retry/async-one-retry", async_retry_once)

    async_breaker = CircuitBreaker(failure_threshold=100)

    async def breaker_call() -> int:
        return await async_breaker.call_async(async_value)

    bench_async(runner, "circuit-breaker/async-success", breaker_call)


if __name__ == "__main__":
    main()
