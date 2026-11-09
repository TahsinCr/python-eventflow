"""Benchmark EventFlow cache, state, context, registry, policy and metrics paths."""

from __future__ import annotations

from eventflow.cache import VersionedCache
from eventflow.context import EventEnvelope, correlation_context, current_correlation_id
from eventflow.observability import OperationStats
from eventflow.policy import PolicyEvaluator
from eventflow.registry import ServiceRegistry
from eventflow.state import StateMachine, VersionedValue

from ._runner import bench_sync, create_runner


def main() -> None:
    runner = create_runner(__doc__ or "EventFlow component benchmarks")
    runner.parse_args()

    cache = VersionedCache[str, int](max_entries=128)
    cache.put("hot", 42, version=1)
    bench_sync(runner, "cache/hit", lambda: cache.get("hot"))
    bench_sync(runner, "cache/miss", lambda: cache.get("missing"))

    cache_version = 0

    def cache_update() -> bool:
        nonlocal cache_version
        cache_version += 1
        return cache.put("hot", cache_version, version=cache_version)

    bench_sync(runner, "cache/update", cache_update)

    value = VersionedValue(0)
    bench_sync(runner, "versioned-value/snapshot", value.snapshot)

    def compare_and_set() -> bool:
        snapshot = value.snapshot()
        return value.compare_and_set(snapshot.version, snapshot.value + 1)

    bench_sync(runner, "versioned-value/compare-and-set", compare_and_set)

    machine = StateMachine("idle", {"idle": {"running"}, "running": {"idle"}})

    def transition() -> object:
        snapshot = machine.snapshot()
        target = "running" if snapshot.state == "idle" else "idle"
        return machine.compare_and_transition(snapshot.version, target)

    bench_sync(runner, "state-machine/compare-and-transition", transition)

    registry = ServiceRegistry()
    registry.register("primary", object(), owner="benchmark")
    bench_sync(runner, "service-registry/resolve", lambda: registry.resolve("primary"))

    for rule_count in (0, 1, 16):
        policy = PolicyEvaluator()
        for index in range(rule_count):
            policy.add(f"allow-{index}", lambda _subject, _action, _resource, _context: True)
        bench_sync(
            runner,
            f"policy/evaluate-rules-{rule_count}",
            lambda policy=policy: policy.evaluate(subject="user", action="read", resource="item"),
            metadata={"rules": rule_count},
        )

    bench_sync(runner, "event-envelope/create", lambda: EventEnvelope(42))

    metrics = OperationStats(max_operations=16)
    bench_sync(runner, "operation-stats/record", lambda: metrics.record("publish"))

    def read_correlation_context() -> str | None:
        with correlation_context("benchmark"):
            return current_correlation_id()

    bench_sync(runner, "context/correlation-bind-read-reset", read_correlation_context)


if __name__ == "__main__":
    main()
