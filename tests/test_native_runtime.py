"""Checks that requested experimental kernels execute in the EventFlow paths."""

from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import eventflow.event_bus as event_bus_module
import eventflow.history as history_module
from eventflow import EventBus, EventFilter, _native


class NativeRuntimeTests(unittest.TestCase):
    def test_native_import_that_enables_free_threaded_gil_fails_closed(self) -> None:
        module = SimpleNamespace(merge_subscribers=lambda *_args: ())
        with (
            patch.dict(os.environ, {"TEST_BACKEND": "c"}),
            patch.object(_native.sys, "_is_gil_enabled", side_effect=(False, True), create=True),
            patch.object(_native, "import_module", return_value=module),
        ):
            with self.assertRaisesRegex(RuntimeError, "enabled the free-threaded GIL"):
                _native._load_kernel("TEST_BACKEND", "c", "native_fixture", "merge_subscribers")

    def test_forced_native_backend_fails_when_extension_is_missing(self) -> None:
        missing_module = ModuleNotFoundError("No module named 'eventflow_native_cython'")
        with (
            patch.dict(os.environ, {"TEST_BACKEND": "cython"}),
            patch.object(_native, "import_module", side_effect=missing_module),
            self.assertRaisesRegex(
                RuntimeError,
                "requires the optional eventflow_native_cython extension",
            ),
        ):
            _native._load_kernel(
                "TEST_BACKEND", "cython", "eventflow_native_cython", "filter_by_type"
            )

    @unittest.skipUnless(
        _native.HISTORY_BACKEND == "cython" and _native.ROUTING_BACKEND == "c",
        "run with both experimental native backends enabled",
    )
    def test_native_kernels_run_through_history_and_event_bus(self) -> None:
        self.assertEqual(_native.HISTORY_TYPE_FILTER.__module__, "eventflow_native_cython")
        self.assertEqual(_native.ROUTING_MERGE.__module__, "eventflow_native_c")

        class Chosen:
            pass

        history = history_module.MemoryEventHistory[object](capacity=3)
        values = (Chosen(), object(), Chosen())
        for value in values:
            history.append(value)

        with patch.object(
            history_module, "HISTORY_TYPE_FILTER", wraps=history_module.HISTORY_TYPE_FILTER
        ) as native_filter:
            self.assertEqual(
                history.query(EventFilter[object](event_type=Chosen)),
                (values[0], values[2]),
            )
            self.assertEqual(
                history.query(EventFilter[object](event_type=Chosen), limit=1), (values[2],)
            )
            native_filter.assert_called_once()

        seen: list[str] = []
        with patch.object(
            event_bus_module, "ROUTING_MERGE", wraps=event_bus_module.ROUTING_MERGE
        ) as native_merge:
            with EventBus[str](routing_key=lambda event: event) as bus:
                bus.subscribe(lambda _event: seen.append("wildcard-1"))
                bus.subscribe(lambda _event: seen.append("keyed-1"), routing_key="a")
                bus.subscribe(lambda _event: seen.append("wildcard-2"))
                bus.subscribe(lambda _event: seen.append("keyed-2"), routing_key="a")
                result = bus.publish("a")

        self.assertEqual(seen, ["wildcard-1", "keyed-1", "wildcard-2", "keyed-2"])
        self.assertEqual((result.matched, result.delivered), (4, 4))
        native_merge.assert_called_once()


if __name__ == "__main__":
    unittest.main()
