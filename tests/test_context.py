import asyncio
import copy
import pickle
import threading
import unittest
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import datetime
from types import MappingProxyType

from eventflow.context import EventEnvelope, correlation_context, current_correlation_id


class ContextTests(unittest.TestCase):
    def test_envelope_is_opt_in_and_shallowly_immutable(self):
        metadata = {"request": 1}
        envelope = EventEnvelope[int](1, metadata=metadata)
        metadata["request"] = 2
        self.assertEqual(envelope.event, 1)
        self.assertEqual(envelope.metadata["request"], 1)
        self.assertEqual(envelope.correlation_id, envelope.id)
        self.assertIsNotNone(envelope.timestamp.utcoffset())
        with self.assertRaises(FrozenInstanceError):
            envelope.source = "changed"
        with self.assertRaises(TypeError):
            envelope.metadata["changed"] = True
        for kwargs in ({"id": ""}, {"correlation_id": ""}, {"timestamp": datetime.now()}):
            with self.assertRaises(ValueError):
                EventEnvelope(1, **kwargs)

    def test_envelope_metadata_supports_asdict_and_pickle(self):
        envelope = EventEnvelope[int](1, metadata={"request": "r-1"})

        self.assertEqual(asdict(envelope)["metadata"], {"request": "r-1"})
        restored = pickle.loads(pickle.dumps(envelope))
        self.assertEqual(restored, envelope)
        with self.assertRaises(TypeError):
            restored.metadata["other"] = "value"

    def test_metadata_copy_replace_and_pickle_protocols_preserve_contract(self):
        nested = {"items": [1]}
        envelope = EventEnvelope[int](
            1,
            metadata=MappingProxyType({"nested": nested}),
        )

        copied = copy.deepcopy(envelope)
        replaced = replace(envelope, source="test")
        self.assertEqual(asdict(envelope)["metadata"], {"nested": {"items": [1]}})
        self.assertEqual(copied.metadata["nested"], {"items": [1]})
        self.assertIsNot(copied.metadata["nested"], nested)
        self.assertEqual(replaced.metadata["nested"], {"items": [1]})

        nested["items"].append(2)
        self.assertEqual(envelope.metadata["nested"]["items"], [1, 2])
        self.assertEqual(copied.metadata["nested"]["items"], [1])

        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            with self.subTest(protocol=protocol):
                restored = pickle.loads(pickle.dumps(envelope, protocol=protocol))
                self.assertEqual(restored.metadata["nested"]["items"], [1, 2])
                with self.assertRaises(TypeError):
                    restored.metadata["other"] = "value"

    def test_non_pickleable_metadata_is_rejected_by_pickle(self):
        envelope = EventEnvelope[int](1, metadata={"lock": threading.Lock()})

        with self.assertRaises(TypeError):
            pickle.dumps(envelope)

    def test_nested_correlation_is_reset_after_exception(self):
        self.assertIsNone(current_correlation_id())
        with correlation_context("outer"):
            with self.assertRaises(ValueError), correlation_context("inner"):
                self.assertEqual(EventEnvelope(1).correlation_id, "inner")
                raise ValueError
            self.assertEqual(current_correlation_id(), "outer")
        self.assertIsNone(current_correlation_id())
        with self.assertRaises(ValueError), correlation_context(""):
            pass


class AsyncContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_task_correlation_isolation(self):
        async def run(identifier):
            with correlation_context(identifier):
                await asyncio.sleep(0)
                return EventEnvelope(1).correlation_id

        self.assertEqual(await asyncio.gather(run("a"), run("b")), ["a", "b"])
        self.assertIsNone(current_correlation_id())
