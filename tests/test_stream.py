import asyncio
import unittest

from eventflow.stream import AsyncStream


async def numbers(count=5):
    for value in range(count):
        yield value


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_lazy_sync_async_operators_and_final_partial_batch(self):
        calls = []

        async def mapper(value):
            calls.append(value)
            return value * 2

        async def predicate(value):
            return value % 4 == 0

        stream = AsyncStream(numbers()).map(mapper).filter(predicate).batch(2)
        self.assertEqual(calls, [])
        self.assertEqual([batch async for batch in stream], [(0, 4), (8,)])
        self.assertEqual(calls, list(range(5)))
        self.assertTrue(stream.closed)
        with self.assertRaises(RuntimeError):
            stream.__aiter__()
        stream = AsyncStream(numbers()).map(lambda value: value + 1).filter(lambda value: value > 1)
        self.assertEqual([item async for item in stream.batch(3, partial=False)], [(2, 3, 4)])

    async def test_validation_and_unstarted_close_propagates_to_source(self):
        class Source:
            closed = False

            def __aiter__(self):
                return numbers()

            async def aclose(self):
                self.closed = True

        source = Source()
        stream = AsyncStream(source).map(lambda value: value).buffer(1)
        await stream.aclose()
        await stream.aclose()
        self.assertTrue(source.closed)
        for invalid in (0, -1, True, 0.5, float("inf")):
            with self.assertRaises(ValueError):
                AsyncStream(numbers()).buffer(invalid)
            with self.assertRaises(ValueError):
                AsyncStream(numbers()).batch(invalid)
        with self.assertRaises(TypeError):
            AsyncStream([])
        with self.assertRaises(TypeError):
            AsyncStream(numbers()).map(None)
        with self.assertRaises(TypeError):
            AsyncStream(numbers()).filter(None)
        with self.assertRaises(TypeError):
            AsyncStream(numbers()).batch(2, partial=1)

    async def test_full_buffer_has_backpressure_and_early_break_cleans_source(self):
        produced = []
        third, closed = asyncio.Event(), asyncio.Event()

        async def source():
            try:
                for value in range(100):
                    produced.append(value)
                    if value == 2:
                        third.set()
                    yield value
            finally:
                closed.set()

        stream = AsyncStream(source()).buffer(1)
        async with stream:
            async for value in stream:
                self.assertEqual(value, 0)
                await third.wait()
                self.assertEqual(produced, [0, 1, 2])
                break
        self.assertTrue(closed.is_set())
        self.assert_no_stream_tasks()

    async def test_producer_error_is_forwarded_after_queued_values(self):
        original = ValueError("source failed")

        async def source():
            yield 1
            yield 2
            raise original

        stream = AsyncStream(source()).buffer(1)
        seen = []
        with self.assertRaises(ValueError) as caught:
            async for item in stream:
                seen.append(item)
        self.assertIs(caught.exception, original)
        self.assertEqual(seen, [1, 2])
        self.assert_no_stream_tasks()

    async def test_source_error_is_preserved_when_close_fails(self):
        source_error = ValueError("source failed")
        cleanup_error = RuntimeError("source close failed")

        class Source:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise source_error

            async def aclose(self):
                raise cleanup_error

        stream = AsyncStream(Source())
        with self.assertRaises(ValueError) as caught:
            await stream.__anext__()
        self.assertIs(caught.exception, source_error)
        self.assertIs(caught.exception.__cause__, cleanup_error)

    async def test_natural_exhaustion_reports_close_failure(self):
        cleanup_error = RuntimeError("source close failed")

        class Source:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            async def aclose(self):
                raise cleanup_error

        with self.assertRaises(RuntimeError) as caught:
            await AsyncStream(Source()).__anext__()
        self.assertIs(caught.exception, cleanup_error)

    async def test_cleanup_cancellation_is_not_masked_by_source_error(self):
        source_error = ValueError("source failed")
        cleanup_error = asyncio.CancelledError("source close cancelled")

        class Source:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise source_error

            async def aclose(self):
                raise cleanup_error

        with self.assertRaises(asyncio.CancelledError) as caught:
            await AsyncStream(Source()).__anext__()
        self.assertIs(caught.exception.__cause__, source_error)

    async def test_close_attempts_upstream_and_chains_both_failures(self):
        source_error = ValueError("stream close failed")
        upstream_error = OSError("upstream close failed")

        class Source:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            async def aclose(self):
                raise source_error

        class Upstream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            async def aclose(self):
                raise upstream_error

        stream = AsyncStream(Source(), _upstream=AsyncStream(Upstream()))
        with self.assertRaises(ValueError) as caught:
            await stream.aclose()
        self.assertIs(caught.exception, source_error)
        self.assertIs(caught.exception.__cause__, upstream_error)

    async def test_source_owned_cancellation_propagates(self):
        async def source():
            raise asyncio.CancelledError
            yield 1

        with self.assertRaises(asyncio.CancelledError):
            async for _ in AsyncStream(source()).buffer(1):
                pass
        self.assert_no_stream_tasks()

    async def test_consumer_error_and_mapper_error_close_source(self):
        closed = asyncio.Event()

        async def source():
            try:
                yield 1
                yield 2
            finally:
                closed.set()

        stream = AsyncStream(source()).buffer(1)
        with self.assertRaises(ValueError):
            async with stream:
                async for _ in stream:
                    raise ValueError("consumer")
        self.assertTrue(closed.is_set())
        closed.clear()
        stream = AsyncStream(source()).map(lambda value: 1 / 0)
        with self.assertRaises(ZeroDivisionError):
            async for _ in stream:
                pass
        self.assertTrue(closed.is_set())
        self.assert_no_stream_tasks()

    async def test_repeated_cancel_waits_for_producer_cleanup(self):
        entered, cleaning, release, closed = (asyncio.Event() for _ in range(4))

        async def source():
            try:
                entered.set()
                await asyncio.Event().wait()
                yield 1
            finally:
                cleaning.set()
                await release.wait()
                closed.set()

        stream = AsyncStream(source()).buffer(1)

        async def consume():
            async with stream:
                async for _ in stream:
                    pass

        consumer = asyncio.create_task(consume())
        await entered.wait()
        consumer.cancel()
        await cleaning.wait()
        consumer.cancel()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await consumer
        self.assertTrue(closed.is_set())
        self.assert_no_stream_tasks()

    async def test_external_close_of_pending_read_inside_context_does_not_deadlock(self):
        entered, closed = asyncio.Event(), asyncio.Event()

        async def source():
            try:
                entered.set()
                await asyncio.Event().wait()
                yield 1
            finally:
                closed.set()

        stream = AsyncStream(source()).buffer(1)

        async def consume():
            async with stream:
                async for _ in stream:
                    pass

        consumer = asyncio.create_task(consume())
        await entered.wait()
        await asyncio.wait_for(stream.aclose(), 1)
        with self.assertRaises(asyncio.CancelledError):
            await consumer
        self.assertTrue(closed.is_set())
        self.assert_no_stream_tasks()

    async def test_concurrent_reads_are_rejected(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def source():
            entered.set()
            await release.wait()
            yield 1

        stream = AsyncStream(source())
        reader = asyncio.create_task(stream.__anext__())
        await entered.wait()
        with self.assertRaises(RuntimeError):
            await stream.__anext__()
        release.set()
        self.assertEqual(await reader, 1)
        await stream.aclose()

    async def test_one_task_owns_stream_even_between_source_reads(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def mapper(value):
            entered.set()
            await release.wait()
            return value

        source = AsyncStream(numbers())
        stream = source.map(mapper)
        reader = asyncio.create_task(stream.__anext__())
        await entered.wait()
        with self.assertRaisesRegex(RuntimeError, "one task"):
            await source.__anext__()
        release.set()
        self.assertEqual(await reader, 0)
        await stream.aclose()

    async def test_nested_buffers_close_without_producer_leaks(self):
        closed = asyncio.Event()

        async def source():
            try:
                for value in range(100):
                    yield value
            finally:
                closed.set()

        stream = AsyncStream(source()).buffer(1).map(lambda value: value).buffer(1)
        async with stream:
            async for value in stream:
                self.assertEqual(value, 0)
                break
        self.assertTrue(closed.is_set())
        self.assert_no_stream_tasks()

    def assert_no_stream_tasks(self):
        self.assertEqual(
            [
                task.get_name()
                for task in asyncio.all_tasks()
                if task.get_name().startswith("EventFlowStream") and not task.done()
            ],
            [],
        )
