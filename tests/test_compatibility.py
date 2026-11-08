"""Version-independent behavior of the supported standard-library shims."""

from __future__ import annotations

import unittest

from eventflow._compatibility import BaseExceptionGroup, ExceptionGroup, StrEnum


class CompatibilityTests(unittest.TestCase):
    def test_string_enum_and_exception_groups_match_supported_python_versions(self):
        class Status(StrEnum):
            READY = "ready"

        self.assertEqual(Status.READY, "ready")
        self.assertEqual(str(Status.READY), "ready")

        errors = ExceptionGroup("callback failures", [ValueError("callback")])
        self.assertIsInstance(errors, BaseExceptionGroup)
        self.assertEqual(errors.message, "callback failures")
        self.assertEqual(str(errors.exceptions[0]), "callback")

        base_errors = BaseExceptionGroup("process failures", [KeyboardInterrupt()])
        self.assertIsInstance(base_errors.exceptions[0], KeyboardInterrupt)


if __name__ == "__main__":
    unittest.main()
