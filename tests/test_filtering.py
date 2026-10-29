from __future__ import annotations

import unittest

from eventflow.filtering import EventFilter
from eventflow.stream import AsyncStream


class EventFilterTests(unittest.TestCase):
    def test_predicate_must_return_bool(self) -> None:
        for result in (0, "false", None):
            event_filter = EventFilter(predicate=lambda _event: result)
            with self.subTest(result=result), self.assertRaisesRegex(TypeError, "return bool"):
                event_filter.matches(1)


class StreamFilterTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_predicate_must_return_bool(self) -> None:
        async def source():
            yield 1

        stream = AsyncStream(source()).filter(lambda _event: "true")  # type: ignore[arg-type]
        with self.assertRaisesRegex(TypeError, "return bool"):
            await stream.__anext__()

    async def test_async_predicate_must_return_bool(self) -> None:
        async def source():
            yield 1

        async def predicate(_event: int) -> bool:
            return "false"  # type: ignore[return-value]

        stream = AsyncStream(source()).filter(predicate)
        with self.assertRaisesRegex(TypeError, "return bool"):
            await stream.__anext__()
