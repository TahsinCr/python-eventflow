"""Small, shared benchmark harness; timings exclude memory tracing."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import platform
import subprocess
import sys
import tracemalloc
from collections.abc import Awaitable, Callable
from pathlib import Path, PurePosixPath
from statistics import median
from time import perf_counter_ns


def _result(label: str, timings: list[int], peak: int) -> dict[str, object]:
    ordered = sorted(timings)

    def percentile(fraction: float) -> float:
        return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))] / 1000

    return {
        "case": label,
        "iterations": len(timings),
        "ops_per_second": 1e9 / median(timings),
        "p50_us": percentile(0.50),
        "p95_us": percentile(0.95),
        "p99_us": percentile(0.99),
        "peak_traced_bytes": peak,
    }


def measure(label: str, operation: Callable[[], object], iterations: int) -> dict[str, object]:
    for _ in range(20):
        operation()
    timings = []
    for _ in range(iterations):
        started = perf_counter_ns()
        operation()
        timings.append(perf_counter_ns() - started)
    tracemalloc.start()
    try:
        for _ in range(min(iterations, 100)):
            operation()
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    return _result(label, timings, peak)


async def measure_async(
    label: str,
    operation: Callable[[], Awaitable[object]],
    iterations: int,
) -> dict[str, object]:
    for _ in range(20):
        await operation()
    timings = []
    for _ in range(iterations):
        started = perf_counter_ns()
        await operation()
        timings.append(perf_counter_ns() - started)
    tracemalloc.start()
    try:
        for _ in range(min(iterations, 100)):
            await operation()
        await asyncio.sleep(0)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    return _result(label, timings, peak)


def _is_source_path(relative: bytes | str) -> bool:
    path = PurePosixPath(os.fsdecode(relative) if isinstance(relative, bytes) else relative)
    if path == PurePosixPath("pyproject.toml"):
        return True
    if not path.parts or path.parts[0] == "benchmarks" and "results" in path.parts:
        return False
    return (path.parts[0] == "eventflow" and (path.suffix == ".py" or path.name == "py.typed")) or (
        path.parts[0] == "benchmarks" and path.suffix == ".py"
    )


def _source_files(root: Path) -> list[Path]:
    files = [root / "pyproject.toml"]
    for directory in (root / "eventflow", root / "benchmarks"):
        if directory.is_dir():
            files.extend(
                path
                for path in directory.rglob("*")
                if path.is_file() and _is_source_path(path.relative_to(root).as_posix())
            )
    return sorted(path for path in files if path.is_file())


def source_provenance(root: Path | None = None) -> dict[str, str | bool | None]:
    root = (root or Path(__file__).resolve().parents[1]).resolve()
    revision = None
    source_dirty = None
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            encoding="ascii",
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"],
            cwd=root,
            check=True,
            capture_output=True,
        ).stdout
        source_dirty = any(
            _is_source_path(entry[3:] if len(entry) >= 3 and entry[2:3] == b" " else entry)
            for entry in status.split(b"\0")
            if entry
        )
    except (OSError, subprocess.CalledProcessError, UnicodeDecodeError):
        pass

    source_sha256 = None
    try:
        digest = hashlib.sha256()
        for source in _source_files(root):
            relative = os.fsencode(source.relative_to(root).as_posix())
            digest.update(len(relative).to_bytes(8, "big"))
            digest.update(relative)
            with source.open("rb") as content:
                for chunk in iter(lambda: content.read(64 * 1024), b""):
                    digest.update(chunk)
        source_sha256 = digest.hexdigest()
    except OSError:
        pass

    return {
        "revision": revision,
        "source_dirty": source_dirty,
        "source_sha256": source_sha256,
    }


def benchmark_metadata(root: Path | None = None) -> dict[str, object]:
    return {
        "gil_enabled": getattr(sys, "_is_gil_enabled", lambda: None)(),
        "source": source_provenance(root),
    }


def report(results: list[dict[str, object]], output: str | None) -> None:
    document = {
        "python": sys.version,
        "platform": platform.platform(),
        **benchmark_metadata(),
        "results": results,
    }
    payload = json.dumps(document, indent=2)
    if output:
        Path(output).write_text(payload + "\n", encoding="utf-8")
    print(payload)
