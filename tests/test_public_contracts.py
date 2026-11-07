"""Frozen public signatures and real decorator/context-manager contracts."""

import ast
from importlib import import_module
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
    def test_readme_examples_before_integrated_section_execute(self):
        root = Path(__file__).resolve().parents[1]
        for name in ("README.md", "README-TR.md"):
            content = (
                (root / name)
                .read_text(encoding="utf-8")
                .split("<!-- integrated execution documentation -->")[0]
            )
            blocks = re.findall(r"~~~python\n(.*?)~~~", content, re.S)
            for index, block in enumerate(blocks):
                with self.subTest(readme=name, block=index):
                    subprocess.run(
                        [sys.executable, "-c", block],
                        check=True,
                        cwd=root,
                        capture_output=True,
                        timeout=20,
                    )

    def test_released_execution_apis_match_the_reviewed_contract(self):
        contract = json.loads(
            Path(__file__).with_name("api_v1_3_1.json").read_text(encoding="utf-8")
        )
        for name in contract["exports"]:
            self.assertIn(name, eventflow.__all__)
            self.assertIsNotNone(getattr(eventflow, name))
        for name, definition in contract["signatures"].items():
            item = getattr(eventflow, name)
            if "$call" in definition:
                self.assertEqual(str(inspect.signature(item)), definition["$call"], name)
            for method, signature in definition["methods"].items():
                self.assertEqual(
                    str(inspect.signature(getattr(item, method))),
                    signature,
                    f"{name}.{method}",
                )

    def test_public_exports_match_the_lazy_module_inventory(self):
        exports = eventflow.__all__
        optional_exports = eventflow._OPTIONAL_EXPORTS

        self.assertEqual(len(exports), len(set(exports)))
        self.assertTrue(set(exports).issubset(dir(eventflow)))
        self.assertTrue(set(optional_exports).issubset(exports))

        module_exports = {}
        exports_by_module = {}
        for name, module_name in optional_exports.items():
            module = module_exports.get(module_name)
            if module is None:
                module = import_module(module_name, eventflow.__name__)
                module_exports[module_name] = module
            exports_by_module.setdefault(module_name, set()).add(name)

        for module_name, module in module_exports.items():
            module_public_exports = set(getattr(module, "__all__", ())) & set(exports)
            self.assertEqual(module_public_exports, exports_by_module[module_name])

        for name in exports:
            self.assertIsNotNone(getattr(eventflow, name), name)

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

    def test_subscribe_returns_handle_and_context_managers_work(self):
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
