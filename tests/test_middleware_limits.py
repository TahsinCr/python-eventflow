from __future__ import annotations

import unittest

from eventflow import AsyncPipeline, Pipeline


class MiddlewareLimitTests(unittest.TestCase):
    def test_sync_pipeline_rejects_middleware_beyond_supported_depth(self) -> None:
        pipeline = Pipeline[int]()

        def identity(value: int, next_):
            return next_(value)

        for index in range(64):
            pipeline.add_middleware(str(index), identity)

        pipeline.disable_middleware("0")
        self.assertEqual(pipeline.run(1), 1)
        with self.assertRaisesRegex(ValueError, "at most 64"):
            pipeline.add_middleware("overflow", identity)
        self.assertEqual(len(pipeline.middleware_names), 64)

    def test_async_pipeline_rejects_middleware_beyond_supported_depth(self) -> None:
        import asyncio

        async def run() -> None:
            pipeline = AsyncPipeline[int]()

            async def identity(value: int, next_):
                return await next_(value)

            for index in range(64):
                pipeline.add_middleware(str(index), identity)

            pipeline.disable_middleware("0")
            self.assertEqual(await pipeline.run(1), 1)
            with self.assertRaisesRegex(ValueError, "at most 64"):
                pipeline.add_middleware("overflow", identity)
            self.assertEqual(len(pipeline.middleware_names), 64)

        asyncio.run(run())
