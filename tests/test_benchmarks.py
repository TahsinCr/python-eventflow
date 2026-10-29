from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class StreamBenchmarkTests(unittest.TestCase):
    def test_stream_report_includes_equivalent_native_baseline(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "stream.json"
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "benchmarks.benchmark_stream",
                    "--iterations",
                    "1",
                    "--items",
                    "5",
                    "--output",
                    str(output),
                ],
                cwd=repository,
                check=True,
                capture_output=True,
                text=True,
            )

            results = json.loads(output.read_text(encoding="utf-8"))["results"]

        by_case = {result["case"]: result for result in results}
        self.assertEqual(
            set(by_case),
            {"direct-generator", "native-map-filter", "unbuffered", "buffer-16"},
        )
        for result in by_case.values():
            self.assertEqual(result["items_per_operation"], 5)
            self.assertGreater(result["items_per_second"], 0)


if __name__ == "__main__":
    unittest.main()
