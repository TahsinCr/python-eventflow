"""Frozen v1.1 public signatures and real decorator/context-manager contracts."""

import ast
import inspect
import json
import re
import subprocess
import sys
import unittest
from pathlib import Path

import eventflow
from eventflow import (
    AsyncEventBus,
    AsyncEventEngine,
    CallbackSubscription,
    EventBus,
    Subscription,
)


class PublicContractTests(unittest.TestCase):
    def test_legacy_readme_examples_execute(self):
        root = Path(__file__).resolve().parents[1]
        for name in ("README.md", "README-TR.md"):
            legacy = (
                (root / name)
                .read_text(encoding="utf-8")
                .split("<!-- integrated execution documentation -->")[0]
            )
            blocks = re.findall(r"~~~python\n(.*?)~~~", legacy, re.S)
            subprocess.run(
                [sys.executable, "-c", "\n".join(blocks)],
                check=True,
                cwd=root,
                capture_output=True,
                timeout=20,
            )

    def test_v1_1_exports_and_signatures_are_preserved(self):
        contract = json.loads(Path(__file__).with_name("api_v1_1.json").read_text())
        for name, methods in contract.items():
            self.assertIn(name, eventflow.__all__)
            item = getattr(eventflow, name)
            for method, signature in methods.items():
                actual = inspect.signature(getattr(item, method))
                if (name, method) in {
                    ("AsyncEventBus", "__init__"),
                    ("AsyncEventEngine", "__init__"),
                }:
                    actual = actual.replace(
                        parameters=[
                            parameter
                            for parameter in actual.parameters.values()
                            if parameter.name != "max_concurrency"
                        ]
                    )
                self.assertEqual(str(actual), signature, f"{name}.{method}")

    def test_async_delivery_limit_is_keyword_only_and_opt_in(self):
        for item in (AsyncEventBus, AsyncEventEngine):
            parameter = inspect.signature(item).parameters["max_concurrency"]
            self.assertEqual(parameter.kind, inspect.Parameter.KEYWORD_ONLY)
            self.assertIsNone(parameter.default)

    def test_callback_decorators_keep_original_function_and_policy(self):
        calls = []
        callback = CallbackSubscription(
            1, lambda: calls.append("cancel"), lambda event: calls.append(event), once=True
        )

        @callback.on_before
        def before(context):
            calls.append(("before", context.event))

        self.assertEqual(before.__name__, "before")
        callback("direct")
        callback.invoke("policy")
        callback.invoke("ignored")
        self.assertEqual(calls, ["direct", "cancel", ("before", "policy"), "policy"])

    def test_subscribe_returns_handle_and_old_context_managers_work(self):
        seen = []
        with EventBus[int]() as bus:
            with bus.subscribe(seen.append) as handle:
                self.assertIsInstance(handle, Subscription)
                bus.publish(1)
            bus.publish(2)
        self.assertEqual(seen, [1])
        self.assertTrue(bus.closed)

    def test_all_readme_python_examples_compile(self):
        root = Path(__file__).resolve().parents[1]
        for name in ("README.md", "README-TR.md"):
            text = (root / name).read_text(encoding="utf-8")
            for index, source in enumerate(re.findall(r"~~~python\n(.*?)~~~", text, re.S)):
                compile(source, f"{name}:{index}", "exec", ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)


class AsyncPublicContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_subscription_manager_and_raw_publish(self):
        seen = []

        async def receive(event):
            seen.append(event)

        async with AsyncEventBus[int]() as bus:
            async with await bus.subscribe(receive):
                await bus.publish(1)
            await bus.publish(2)
        self.assertEqual(seen, [1])
        self.assertTrue(bus.closed)
