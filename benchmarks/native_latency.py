"""Collect raw routing latency, lock metrics and batch throughput separately."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import socket
import statistics
import sys
import sysconfig
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Condition, Lock, get_ident, local
from time import perf_counter_ns, process_time_ns
from typing import Any, cast

from benchmarks._runner import benchmark_metadata
from benchmarks._support import positive_int
from benchmarks.benchmark_native import RoutingEvent, _kernels, _subscribe_fixture
from benchmarks.native._contracts import MergeKernel
from benchmarks.native.provenance import native_build_metadata
from eventflow.event_bus import EventBus

_PROFILES = {
    "all_callbacks_7keys_20pct_wildcard": (7, 20, False),
    "filter_reject_7keys_20pct_wildcard": (7, 20, True),
    "all_callbacks_31keys_50pct_wildcard": (31, 50, False),
}
_PUBLISHER_COUNTS = (1, 4, 8)
_PERCENTILES = (50, 95, 99)


class _LockMetricsRecorder:
    """RLock proxy that captures per-thread acquisition wait and hold times."""

    def __init__(self, lock: Any) -> None:
        self._lock = lock
        self._local = local()
        self._samples_lock = Lock()
        self._generation = 0
        self._thread_samples: dict[int, list[tuple[int, int]]] = {}

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        started = perf_counter_ns()
        if timeout == -1:
            acquired = bool(self._lock.acquire(blocking))
        else:
            acquired = bool(self._lock.acquire(blocking, timeout))
        if acquired:
            acquired_at = perf_counter_ns()
            stack = getattr(self._local, "acquisitions", None)
            if stack is None:
                stack = []
                self._local.acquisitions = stack
            stack.append((acquired_at - started, acquired_at))
        return acquired

    def _record_release(self, release_lock: bool) -> None:
        stack = getattr(self._local, "acquisitions", None)
        if not stack:
            if release_lock:
                self._lock.release()
            return
        released_at = perf_counter_ns()
        wait_ns, acquired_at = stack.pop()
        if release_lock:
            self._lock.release()
        sample = (wait_ns, released_at - acquired_at)
        thread_id = get_ident()
        state = getattr(self._local, "sample_buffer", None)
        if state is None or state[0] != self._generation:
            buffer: list[tuple[int, int]] = []
            with self._samples_lock:
                self._thread_samples[thread_id] = buffer
                self._local.sample_buffer = (self._generation, buffer)
        else:
            buffer = state[1]
        buffer.append(sample)

    def release(self) -> None:
        self._record_release(release_lock=True)

    def __enter__(self) -> _LockMetricsRecorder:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()

    def _is_owned(self) -> bool:
        return bool(self._lock._is_owned())

    def locked(self) -> bool:
        try:
            return bool(self._lock.locked())
        except AttributeError:
            return self._is_owned()

    def _release_save(self) -> object:
        stack = getattr(self._local, "acquisitions", None)
        if stack:
            self._record_release(release_lock=False)
        return self._lock._release_save()

    def _acquire_restore(self, state: object) -> None:
        started = perf_counter_ns()
        self._lock._acquire_restore(state)
        acquired_at = perf_counter_ns()
        stack = getattr(self._local, "acquisitions", None)
        if stack is None:
            stack = []
            self._local.acquisitions = stack
        stack.append((acquired_at - started, acquired_at))

    def take_samples(self) -> dict[int, list[tuple[int, int]]]:
        with self._samples_lock:
            samples = {thread_id: list(values) for thread_id, values in self._thread_samples.items()}
            self._thread_samples.clear()
            self._generation += 1
        return samples


def _instrument_bus_locks(
    bus: EventBus[RoutingEvent],
) -> tuple[_LockMetricsRecorder, _LockMetricsRecorder]:
    underlying = cast(Any, bus._lock)
    bus_lock = _LockMetricsRecorder(underlying)
    stats_lock = _LockMetricsRecorder(underlying)
    cast(Any, bus)._lock = bus_lock
    cast(Any, bus)._condition = Condition(stats_lock)
    return bus_lock, stats_lock


def _nearest_rank(samples: Sequence[int], percentile: int) -> int:
    if not samples or percentile not in _PERCENTILES:
        raise ValueError("percentile requires samples and a 50, 95 or 99 percentile")
    ordered = sorted(samples)
    rank = math.ceil(percentile * len(ordered) / 100)
    return ordered[rank - 1]


def _distribution(samples: Sequence[int]) -> dict[str, int | float]:
    if not samples:
        raise ValueError("cannot summarize an empty sample group")
    ordered = sorted(samples)
    mean = statistics.fmean(ordered)
    stdev = statistics.stdev(ordered) if len(ordered) > 1 else 0.0
    return {
        "sample_count": len(ordered),
        "mean_ns": mean,
        "median_ns": statistics.median(ordered),
        "p50_ns": _nearest_rank(ordered, 50),
        "p95_ns": _nearest_rank(ordered, 95),
        "p99_ns": _nearest_rank(ordered, 99),
        "max_ns": ordered[-1],
        "stdev_ns": stdev,
        "cv_percent": stdev / mean * 100 if mean else 0.0,
    }


def _cpu_model() -> str:
    if sys.platform.startswith("linux"):
        try:
            for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
                if line.startswith(("model name", "Hardware")):
                    return line.partition(":")[2].strip()
        except OSError:
            pass
    return platform.processor() or platform.machine() or "unknown"


def _cpu_affinity() -> str:
    get_affinity = getattr(os, "sched_getaffinity", None)
    if get_affinity is None:
        return "unavailable"
    try:
        return ",".join(str(cpu) for cpu in sorted(get_affinity(0)))
    except OSError:
        return "unavailable"


def _run_publish_batch(
    bus: EventBus[RoutingEvent],
    event: RoutingEvent,
    loops: int,
    publishers: int,
    *,
    measure_latency: bool,
) -> dict[str, Any]:
    barrier = Barrier(publishers + 1)

    def publish_many(worker: int) -> tuple[int, int, list[int]]:
        thread_id = get_ident()
        samples: list[int] = []
        barrier.wait(timeout=30)
        for _ in range(loops):
            if measure_latency:
                started = perf_counter_ns()
                bus.publish(event)
                samples.append(perf_counter_ns() - started)
            else:
                bus.publish(event)
        return worker, thread_id, samples

    with ThreadPoolExecutor(max_workers=publishers) as executor:
        futures = [executor.submit(publish_many, worker) for worker in range(publishers)]
        started_wall = perf_counter_ns()
        started_cpu = process_time_ns()
        barrier.wait(timeout=30)
        workers = [future.result() for future in futures]
        wall_ns = perf_counter_ns() - started_wall
        process_cpu_ns = process_time_ns() - started_cpu
    return {
        "workers": workers,
        "publish_count": loops * publishers,
        "wall_ns": wall_ns,
        "process_cpu_ns": process_cpu_ns,
    }


def _worker_samples(
    samples_by_thread: dict[int, list[tuple[int, int]]],
    workers: Sequence[tuple[int, int, list[int]]],
) -> dict[str, list[dict[str, int]]]:
    worker_indexes = {thread_id: worker for worker, thread_id, _ in workers}
    result: dict[str, list[dict[str, int]]] = {}
    for thread_id, samples in samples_by_thread.items():
        try:
            worker = worker_indexes[thread_id]
        except KeyError as exc:
            raise RuntimeError("lock samples were recorded by an unknown worker thread") from exc
        result[str(worker)] = [
            {"wait_ns": wait_ns, "hold_ns": hold_ns} for wait_ns, hold_ns in samples
        ]
    return dict(sorted(result.items(), key=lambda item: int(item[0])))


def _measure_case(
    *,
    candidate_implementation: str,
    algorithm: str,
    merge: MergeKernel | None,
    profile: str,
    key_count: int,
    wildcard_percent: int,
    filtered: bool,
    subscriptions: int,
    loops: int,
    publishers: int,
) -> dict[str, Any]:
    bus = _subscribe_fixture(
        subscriptions,
        merge=merge,
        key_count=key_count,
        wildcard_percent=wildcard_percent,
        filtered=filtered,
    )
    event = RoutingEvent(0, 17)
    try:
        selected = len(bus._subscriber_snapshot(event.key))
        check = bus.publish(event)
        if filtered and (check.matched or check.delivered):
            raise AssertionError("filter-rejection fixture delivered an event")
        if not filtered and (check.matched != selected or check.delivered != selected):
            raise AssertionError("routing fixture did not deliver every selected callback")

        # Measure throughput without per-publish timers or lock instrumentation.
        throughput_batch = _run_publish_batch(
            bus, event, loops, publishers, measure_latency=False
        )
        # Derive every latency percentile from this one retained raw sample group.
        latency_batch = _run_publish_batch(
            bus, event, loops, publishers, measure_latency=True
        )
        publish_latency = {
            str(worker): samples
            for worker, _thread_id, samples in latency_batch["workers"]
        }

        bus_lock, stats_lock = _instrument_bus_locks(bus)
        lock_batch = _run_publish_batch(bus, event, loops, publishers, measure_latency=False)
        workers = lock_batch["workers"]
        lock_metrics = {
            "bus_lock": _worker_samples(bus_lock.take_samples(), workers),
            "stats_lock": _worker_samples(stats_lock.take_samples(), workers),
        }
        expected_workers = {str(index) for index in range(publishers)}
        for phase, samples_by_worker in lock_metrics.items():
            expected = loops * (2 if phase == "bus_lock" else 1)
            if set(samples_by_worker) != expected_workers or any(
                len(samples) != expected for samples in samples_by_worker.values()
            ):
                raise AssertionError(f"{phase} recorded an unexpected count per publisher")

        name = f"routing_latency/{algorithm}/{profile}/{publishers}"
        return {
            "benchmark": name,
            "candidate_implementation": candidate_implementation,
            "candidate_algorithm": algorithm,
            "latency_profile": profile,
            "filter_mode": "reject_all" if filtered else "deliver_all",
            "key_count": key_count,
            "wildcard_percent": wildcard_percent,
            "subscriptions": subscriptions,
            "selected_subscriptions": selected,
            "publishers": publishers,
            "loops_per_publisher": loops,
            "throughput": {
                "publish_count": throughput_batch["publish_count"],
                "wall_ns": throughput_batch["wall_ns"],
                "process_cpu_ns": throughput_batch["process_cpu_ns"],
            },
            "publish_latency_ns": publish_latency,
            "latency_summary_ns": {
                "aggregate": _distribution(
                    [sample for samples in publish_latency.values() for sample in samples]
                ),
                **{
                    f"worker_{worker}": _distribution(samples)
                    for worker, samples in publish_latency.items()
                },
            },
            "lock_metrics": lock_metrics,
            "lock_metric_summary_ns": {
                phase: {
                    metric: {
                        "aggregate": _distribution(
                            [
                                sample[metric]
                                for samples in samples_by_worker.values()
                                for sample in samples
                            ]
                        ),
                        **{
                            f"worker_{worker}": _distribution(
                                [sample[metric] for sample in samples]
                            )
                            for worker, samples in samples_by_worker.items()
                        },
                    }
                    for metric in ("wait_ns", "hold_ns")
                }
                for phase, samples_by_worker in lock_metrics.items()
            },
            "latency_timer": "perf_counter_ns around EventBus.publish; timer overhead included",
            "lock_timer": "perf_counter_ns around each underlying RLock acquisition and hold",
            "throughput_instrumented": False,
            "latency_lock_instrumented": False,
            "lock_batch_instrumented": True,
        }
    finally:
        bus.close()


def _candidates(
    implementation: str,
    merge_kernels: Sequence[tuple[str, MergeKernel]],
) -> list[tuple[str, str, MergeKernel | None]]:
    if implementation == "python":
        selected = [
            (name, implementation, merge)
            for name, merge in merge_kernels
            if name == "python_two_pointer"
        ]
        selected.append(("production_two_pointer", "production_python", None))
        return selected
    return [(name, implementation, merge) for name, merge in merge_kernels]


def run_experiment(
    *,
    implementation: str,
    profiles: Sequence[str],
    publishers: Sequence[int],
    loops: int,
    subscriptions: int,
) -> dict[str, Any]:
    _filter, merge_kernels, _tuple_filters = _kernels(implementation)
    candidates = _candidates(implementation, merge_kernels)
    results = []
    for algorithm, candidate_implementation, merge in candidates:
        for profile in profiles:
            key_count, wildcard_percent, filtered = _PROFILES[profile]
            for publisher_count in publishers:
                results.append(
                    _measure_case(
                        candidate_implementation=candidate_implementation,
                        algorithm=algorithm,
                        merge=merge,
                        profile=profile,
                        key_count=key_count,
                        wildcard_percent=wildcard_percent,
                        filtered=filtered,
                        subscriptions=subscriptions,
                        loops=loops,
                        publishers=publisher_count,
                    )
                )
    metadata = benchmark_metadata()
    metadata.update(
        {
            "eventflow_native_matrix": "raw_latency",
            "python_version": sys.version,
            "python_executable": sys.executable,
            "platform": platform.platform(),
            "hostname": socket.gethostname(),
            "cpu_count": os.cpu_count() or "unknown",
            "cpu_model": _cpu_model(),
            "cpu_affinity": _cpu_affinity(),
            "python_compiler": platform.python_compiler(),
            "python_cc": sysconfig.get_config_var("CC") or "unknown",
            "python_cflags": sysconfig.get_config_var("CFLAGS") or "unknown",
        }
    )
    if implementation == "python":
        metadata["eventflow_native_backend"] = "python"
    else:
        metadata.update(native_build_metadata(implementation))
    return {
        "schema": "eventflow.native-latency.v1",
        "metadata": metadata,
        "experiment": {
            "implementation": implementation,
            "loops_per_publisher": loops,
            "publishers": list(publishers),
            "subscriptions": subscriptions,
            "profiles": list(profiles),
            "timer_resolution": "nanoseconds from time.perf_counter_ns",
            "process_cpu_timer": "time.process_time_ns",
            "independent_repetition": "rerun this command in a fresh process",
            "decision_protocol": {
                "pilot": "one fresh process with 500 observations per publisher",
                "initial_stage": {
                    "independent_processes_per_case": 5,
                    "observations_per_publisher": 2000,
                },
                "escalation": {
                    "trigger": "any matched P99 across-process CV exceeds 10% after initial stage",
                    "independent_processes_per_case": 5,
                    "observations_per_publisher": 5000,
                    "scope": "repeat every candidate in the matched comparison",
                },
                "stop": (
                    "stop after the initial stage when all matched P99 CV values are at most 10%; "
                    "otherwise stop after escalation and label remaining high-variance results "
                    "inconclusive"
                ),
            },
        },
        "results": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--implementation", choices=("python", "cython", "c"), default="python")
    parser.add_argument("--profile", choices=tuple(_PROFILES), action="append")
    parser.add_argument("--publishers", type=positive_int, action="append")
    parser.add_argument("--loops", type=positive_int, default=500)
    parser.add_argument("--subscriptions", type=positive_int, default=2048)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = run_experiment(
        implementation=args.implementation,
        profiles=args.profile or tuple(_PROFILES),
        publishers=args.publishers or _PUBLISHER_COUNTS,
        loops=args.loops,
        subscriptions=args.subscriptions,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
