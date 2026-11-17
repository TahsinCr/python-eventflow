from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = {
    "quickstart.py": "matched=1; received=1; sensor=north; celsius=21.5",
    "bus_stream.py": "[0, 2, 4, 6]",
    "coordination.py": "result=42; attempts=2",
    "integrated.py": "[4, 8, 12]",
}


class DocumentationExampleTests(unittest.TestCase):
    def test_linked_examples_run_as_standalone_programs(self) -> None:
        environment = os.environ.copy()
        pythonpath = [str(ROOT)]
        if environment.get("PYTHONPATH"):
            pythonpath.append(environment["PYTHONPATH"])
        environment["PYTHONPATH"] = os.pathsep.join(pythonpath)
        for filename, expected_output in EXAMPLES.items():
            with self.subTest(example=filename):
                result = subprocess.run(
                    [sys.executable, str(ROOT / "examples" / filename)],
                    cwd=ROOT,
                    env=environment,
                    text=True,
                    capture_output=True,
                    timeout=15,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(expected_output, result.stdout)


if __name__ == "__main__":
    unittest.main()
