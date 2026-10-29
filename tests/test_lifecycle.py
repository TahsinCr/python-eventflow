import asyncio
import unittest

from eventflow._lifecycle import finish_cleanup


class FinishCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_awaiting_the_current_task(self):
        task = asyncio.current_task()
        self.assertIsNotNone(task)
        with self.assertRaisesRegex(RuntimeError, "cannot await itself"):
            await finish_cleanup(task)

    async def test_propagates_cleanup_task_cancellation(self):
        async def cancelled():
            raise asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await finish_cleanup(cancelled())

    async def test_propagates_cleanup_failure(self):
        async def failed():
            raise ValueError("cleanup failed")

        with self.assertRaisesRegex(ValueError, "cleanup failed"):
            await finish_cleanup(failed())

    async def test_caller_cancellation_waits_and_chains_cleanup_failure(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def failed_cleanup():
            entered.set()
            await release.wait()
            raise ValueError("cleanup failed")

        cleanup = asyncio.create_task(failed_cleanup())
        caller = asyncio.current_task()
        self.assertIsNotNone(caller)

        async def cancel_caller():
            await entered.wait()
            caller.cancel()
            await asyncio.sleep(0)
            release.set()

        canceller = asyncio.create_task(cancel_caller())
        with self.assertRaises(asyncio.CancelledError) as caught:
            await finish_cleanup(cleanup)
        await canceller
        self.assertIsInstance(caught.exception.__cause__, ValueError)
