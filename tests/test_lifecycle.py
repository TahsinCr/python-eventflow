import asyncio
import unittest
from unittest.mock import patch

from eventflow._lifecycle import finish_cleanup


class FinishCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_closes_coroutine_when_task_creation_is_rejected(self):
        loop = asyncio.get_running_loop()
        rejected = []

        def reject_task(coroutine, *, name=None, context=None):
            rejected.append(coroutine)
            raise RuntimeError("task creation rejected")

        async def cleanup():
            return None

        operation = cleanup()
        with patch.object(loop, "create_task", side_effect=reject_task):
            with self.assertRaisesRegex(RuntimeError, "creation rejected"):
                await finish_cleanup(operation)

        self.assertEqual(len(rejected), 1)
        self.assertIsNone(rejected[0].cr_frame)
        self.assertIsNone(operation.cr_frame)

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
