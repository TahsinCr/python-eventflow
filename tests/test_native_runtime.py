"""Tests for loading and running the optional eventflow._core C extension."""

from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from eventflow import EventBus, _native
from eventflow.observability import OperationStats


class NativeRuntimeTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
