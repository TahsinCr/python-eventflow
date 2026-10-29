import asyncio
import pickle
import unittest
from dataclasses import FrozenInstanceError, asdict
from datetime import datetime

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
