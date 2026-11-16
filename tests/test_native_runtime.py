"""Tests for loading and running the optional eventflow._core C extension."""

from __future__ import annotations

import os
import subprocess
import sys
import sysconfig
import textwrap
import unittest
from collections.abc import Hashable
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from eventflow import EventBus, MemoryEventHistory, _native
from eventflow.observability import OperationStats


class NativeRuntimeTests(unittest.TestCase):
    @unittest.skipUnless(_native.CORE is not None, "native core required")
    @unittest.skipIf(sysconfig.get_config_var("Py_GIL_DISABLED"), "allocator hooks require GIL")
    def test_metrics_growth_recovers_from_allocator_failure(self):
        try:
            import _testcapi
        except ImportError:
            self.skipTest("CPython allocator test hooks unavailable")
        if not hasattr(_testcapi, "set_nomemory"):
            self.skipTest("CPython allocator failure hook unavailable")
        code = textwrap.dedent("""
            import _testcapi
            from eventflow import _native
            import eventflow.observability
            failures = 0
            for offset in range(4):
                stats = _native.CORE.OperationStatsCore(64)
                args = ("growth", "success", 0.0, None, 0, 0, 0, 0, 0.0)
                for index in range(8):
                    stats.record(str(index), *args[1:])
                record = stats.record
                _testcapi.set_nomemory(offset, offset + 1)
                try:
                    record(*args)
                except MemoryError:
                    failures += 1
                finally:
                    _testcapi.remove_mem_hooks()
                for index in range(40):
                    stats.record(str(index), *args[1:])
                stats.snapshot()
                stats.reset()
                record(*args)
                assert stats.snapshot().operations["growth"].calls == 1
            assert failures >= 2, failures
        """)
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=30
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_metrics_record_snapshot_reset_are_thread_safe_in_both_backends(self):
        for backend in (_native.CORE, None):
            with (
                self.subTest(native=backend is not None),
                patch("eventflow.observability.CORE", backend),
            ):
                stats = OperationStats(max_operations=32)

                def exercise(worker):
                    for index in range(200):
                        stats.record(str(worker), dropped=2**100)
                        if index % 7 == 0:
                            stats.snapshot()
                        if index % 31 == 0:
                            stats.reset()

                with ThreadPoolExecutor(max_workers=4) as pool:
                    list(pool.map(exercise, range(4)))
                stats.reset()
                stats.record("final", dropped=2**100)
                self.assertEqual(stats.snapshot().operations["final"].dropped, 2**100)

    def test_metrics_numeric_validation_matches_backends(self):
        class Counter(int):
            pass

        values = (
            0,
            -1,
            True,
            Counter(3),
            2**63 - 1,
            2**63,
            2**100,
            float("nan"),
            float("inf"),
            object(),
        )
        for field in ("queue_depth", "dropped", "retries", "rejected", "timeouts"):
            for value in values:
                outcomes = []
                for backend in (_native.CORE, None):
                    with patch("eventflow.observability.CORE", backend):
                        stats = OperationStats()
                    try:
                        stats.record("sample", **{field: value})
                    except (TypeError, ValueError) as error:
                        outcomes.append(type(error))
                    else:
                        outcomes.append(getattr(stats.snapshot().operations["sample"], field))
                with self.subTest(field=field, value=value):
                    self.assertEqual(outcomes[0], outcomes[1])

    def test_load_core_respects_no_extensions_env(self) -> None:
        with patch.dict(os.environ, {"EVENTFLOW_NO_EXTENSIONS": "1"}):
            self.assertIsNone(_native._load_core())

    def test_load_core_returns_none_when_extension_missing(self) -> None:
        missing_module = ModuleNotFoundError("No module named 'eventflow._core'")
        with (
            patch.dict(os.environ, {"EVENTFLOW_NO_EXTENSIONS": "0"}),
            patch.object(_native, "import_module", side_effect=missing_module),
        ):
            self.assertIsNone(_native._load_core())

    def test_native_import_that_enables_free_threaded_gil_fails_closed(self) -> None:
        with (
            patch.dict(os.environ, {"EVENTFLOW_NO_EXTENSIONS": "0"}),
            patch.object(_native.sys, "_is_gil_enabled", side_effect=(False, True), create=True),
            patch.object(_native, "import_module", return_value=SimpleNamespace()),
            self.assertRaisesRegex(RuntimeError, "enabled the free-threaded GIL"),
        ):
            _native._load_core()

    @unittest.skipUnless(_native.CORE is not None, "eventflow._core extension is not compiled")
    def test_core_extension_powers_event_bus_and_operation_stats(self) -> None:
        core = _native.CORE
        self.assertEqual(core.__name__, "eventflow._core")

        seen: list[str] = []
        with EventBus[str](routing_key=lambda event: event) as bus:
            self.assertIsInstance(bus._core, core.BusCore)
            bus.subscribe(lambda _event: seen.append("wildcard-1"))
            bus.subscribe(lambda _event: seen.append("keyed-1"), routing_key="a")
            bus.subscribe(lambda _event: seen.append("wildcard-2"))
            bus.subscribe(lambda _event: seen.append("keyed-2"), routing_key="a", once=True)
            bus.subscribe(lambda _event: seen.append("other-key"), routing_key="b")

            first = bus.publish("a")
            second = bus.publish("a")

        self.assertEqual(
            seen,
            [
                "wildcard-1",
                "keyed-1",
                "wildcard-2",
                "keyed-2",
                "wildcard-1",
                "keyed-1",
                "wildcard-2",
            ],
        )
        self.assertEqual((first.matched, first.delivered), (4, 4))
        self.assertEqual((second.matched, second.delivered), (3, 3))
        self.assertEqual((bus.stats.published, bus.stats.delivered), (2, 7))

        stats = OperationStats(max_operations=2)
        self.assertIsInstance(stats._core, core.OperationStatsCore)
        stats.record("bus.publish", elapsed=0.25, queue_depth=3, dropped=1)
        stats.record("task.run", status="error", retries=2, timeouts=1)
        stats.record("unbounded-user-key-123", status="cancelled")

        snapshot = stats.snapshot()
        self.assertEqual(set(snapshot.operations), {"bus.publish", "other"})
        self.assertEqual(snapshot.operations["bus.publish"].max_queue_depth, 3)
        self.assertEqual(snapshot.operations["other"].cancelled, 1)

        stats.reset()
        self.assertEqual(stats.snapshot().operations, {})

        maximum = (1 << 63) - 1
        oversized = 1 << 100
        stats.record(
            "overflow",
            dropped=maximum,
            retries=maximum,
            rejected=maximum,
            timeouts=maximum,
            queue_depth=oversized,
        )
        stats.record(
            "overflow", dropped=5, retries=oversized, rejected=7, timeouts=9, queue_depth=9
        )
        metrics = stats.snapshot().operations["overflow"]
        self.assertEqual(metrics.dropped, maximum + 5)
        self.assertEqual(metrics.retries, maximum + oversized)
        self.assertEqual(metrics.rejected, maximum + 7)
        self.assertEqual(metrics.timeouts, maximum + 9)
        self.assertEqual(metrics.queue_depth, 9)
        self.assertEqual(metrics.max_queue_depth, oversized)

        stats.record("overflow")
        metrics = stats.snapshot().operations["overflow"]
        self.assertEqual(metrics.dropped, maximum + 5)
        self.assertEqual(metrics.retries, maximum + oversized)
        self.assertEqual(metrics.rejected, maximum + 7)
        self.assertEqual(metrics.timeouts, maximum + 9)

        stats.reset()
        stats.record("zero-delta", dropped=2, retries=3, rejected=4, timeouts=5)
        stats.record("zero-delta")
        metrics = stats.snapshot().operations["zero-delta"]
        self.assertEqual(
            (metrics.dropped, metrics.retries, metrics.rejected, metrics.timeouts),
            (2, 3, 4, 5),
        )

        stats.reset()
        self.assertEqual(stats.snapshot().operations, {})

        class Counter(int):
            normalized: list[int] = []

            def __int__(self) -> int:
                value = int.__int__(self)
                self.normalized.append(value)
                return value

        stats.record(
            "subclass",
            dropped=Counter(5),
            retries=Counter(oversized),
            queue_depth=Counter(7),
        )
        metrics = stats.snapshot().operations["subclass"]
        self.assertEqual(metrics.dropped, 5)
        self.assertEqual(metrics.retries, oversized)
        self.assertEqual(metrics.queue_depth, 7)
        self.assertIs(type(metrics.retries), int)
        self.assertEqual(Counter.normalized, [5, oversized, 7])

    @unittest.skipUnless(_native.CORE is not None, "eventflow._core extension is not compiled")
    def test_native_publish_falls_back_before_recording_side_effects(self) -> None:
        class MalformedSnapshotBus(EventBus[str]):
            malformed_next_snapshot = False

            def _subscriber_snapshot(self, event_key: Hashable | None) -> tuple[Any, ...]:
                if self.malformed_next_snapshot:
                    self.malformed_next_snapshot = False
                    return (object(),)
                return super()._subscriber_snapshot(event_key)

        history = MemoryEventHistory[str]()
        received: list[str] = []
        with MalformedSnapshotBus(history=history) as bus:
            self.assertIsNotNone(bus._core)
            bus.subscribe(received.append)
            bus.malformed_next_snapshot = True

            result = bus.publish("event")

        self.assertEqual(result.delivered, 1)
        self.assertEqual(received, ["event"])
        self.assertEqual(history.query(), ("event",))
        self.assertEqual(bus.stats.published, 1)
        self.assertEqual(bus.stats.delivered, 1)

    def test_native_publish_respects_subclass_subscribe_overrides(self) -> None:
        class CustomSubscribeBus(EventBus[str]):
            snapshot_calls = 0

            def subscribe(self, *args: Any, **kwargs: Any) -> Any:
                return super().subscribe(*args, **kwargs)

            def _subscriber_snapshot(self, event_key: Hashable | None) -> tuple[Any, ...]:
                self.snapshot_calls += 1
                return super()._subscriber_snapshot(event_key)

        received: list[str] = []
        with CustomSubscribeBus() as bus:
            bus.subscribe(received.append)
            result = bus.publish("event")

        self.assertEqual(received, ["event"])
        self.assertEqual((result.matched, result.delivered), (1, 1))
        self.assertEqual(bus.snapshot_calls, 1)


if __name__ == "__main__":
    unittest.main()
