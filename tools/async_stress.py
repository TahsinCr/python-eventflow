from __future__ import annotations

import argparse
import os
import sys
import unittest
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"
SCENARIOS = (
    "test_async_bus.AsyncEventBusTests.test_async_close_can_resume_after_caller_cancellation",
    "test_async_bus.AsyncEventBusTests.test_async_wait_for_cleanup_when_clear_or_close_races_with_cancellation",
    "test_async_bus.AsyncEventBusTests.test_async_live_publish_waits_behind_subscription_replay",
    "test_async_bus.AsyncEventBusTests.test_cancelled_periodic_task_stays_owned_until_it_exits",
    "test_async_primitives.AsyncPrimitiveTests.test_single_flight_shares_work_without_coupling_waiter_cancellation",
    "test_async_primitives.AsyncPrimitiveTests.test_keyed_rate_limiter_waits_cancellably_and_cleans_waiters",
    "test_async_primitives.AsyncPrimitiveTests.test_circuit_breaker_ignores_stale_half_open_probe_results",
    "test_bridges.AsyncCoordinationEdgeTests.test_event_queue_recovers_when_publish_races_with_waiter_cancellation",
    "test_bridges.AsyncCoordinationEdgeTests.test_event_queue_wakes_waiter_from_publisher_thread",
    "test_stream.StreamTests.test_repeated_cancel_waits_for_producer_cleanup",
    "test_async_primitives.AsyncPrimitiveTests.test_async_stream_batch_within_flushes_by_time_and_preserves_final_partial",
)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Repeat deterministic async lifecycle regressions. Set EVENTFLOW_STRESS=1 "
            "to opt in to this longer run."
        )
    )
    parser.add_argument("--repeat", type=_positive_int, default=10)
    args = parser.parse_args()
    if os.environ.get("EVENTFLOW_STRESS") != "1":
        parser.error("set EVENTFLOW_STRESS=1 to run the opt-in stress suite")

    os.environ["PYTHONASYNCIODEBUG"] = "1"
    warnings.simplefilter("error")
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(TESTS))

    from eventflow import _native

    loader = unittest.defaultTestLoader
    suite = unittest.TestSuite()
    for _ in range(args.repeat):
        for scenario in SCENARIOS:
            test = loader.loadTestsFromName(scenario)
            if test.countTestCases() != 1:
                raise RuntimeError(f"Expected one test for {scenario}")
            suite.addTests(test)

    backend = "native" if _native.CORE is not None else "pure Python"
    print(
        f"Running {len(SCENARIOS)} async scenarios x {args.repeat} repeats "
        f"(backend={backend}, asyncio_debug=on, warnings=error)."
    )
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    print(
        f"Stress summary: scenarios={len(SCENARIOS)}, repeats={args.repeat}, "
        f"tests={result.testsRun}, failures={len(result.failures)}, "
        f"errors={len(result.errors)}, skipped={len(result.skipped)}."
    )
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
