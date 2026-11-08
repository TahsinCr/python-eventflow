"""Shared pyperf runner and source metadata for EventFlow benchmarks."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from argparse import ArgumentParser, Namespace
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path, PurePosixPath
from time import perf_counter
from typing import Protocol, TypedDict, cast


class SourceProvenance(TypedDict):
    revision: str | None
    source_dirty: bool | None
    source_sha256: str | None


class Runner(Protocol):
    argparser: ArgumentParser

    def parse_args(self) -> Namespace: ...

    def bench_time_func(
        self,
        name: str,
        time_func: Callable[..., float],
        *args: object,
        inner_loops: int | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> object: ...

    def bench_async_func(
        self,
        name: str,
        func: Callable[..., Awaitable[object]],
        *args: object,
        inner_loops: int | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> object: ...


def _is_source_path(relative: bytes | str) -> bool:
    path = PurePosixPath(os.fsdecode(relative) if isinstance(relative, bytes) else relative)
    if path == PurePosixPath("pyproject.toml"):
        return True
    if not path.parts or (path.parts[0] == "benchmarks" and "results" in path.parts):
        return False
    return (path.parts[0] == "eventflow" and (path.suffix == ".py" or path.name == "py.typed")) or (
        path.parts[0] == "benchmarks" and path.suffix == ".py"
    )


def source_provenance(root: Path | None = None) -> SourceProvenance:
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

    source_files = [root / "pyproject.toml"]
    for directory in (root / "eventflow", root / "benchmarks"):
        if directory.is_dir():
            source_files.extend(
                path
                for path in directory.rglob("*")
                if path.is_file() and _is_source_path(path.relative_to(root).as_posix())
            )

    digest = hashlib.sha256()
    try:
        for source in sorted(path for path in source_files if path.is_file()):
            relative = os.fsencode(source.relative_to(root).as_posix())
            digest.update(len(relative).to_bytes(8, "big"))
            digest.update(relative)
            with source.open("rb") as content:
                for chunk in iter(lambda: content.read(64 * 1024), b""):
                    digest.update(chunk)
        source_sha256 = digest.hexdigest()
    except OSError:
        source_sha256 = None

    return {
        "revision": revision,
        "source_dirty": source_dirty,
        "source_sha256": source_sha256,
    }


def benchmark_metadata(root: Path | None = None) -> dict[str, str]:
    gil_check = getattr(sys, "_is_gil_enabled", None)
    gil_enabled = "unavailable" if gil_check is None else ("enabled" if gil_check() else "disabled")
    source = source_provenance(root)
    return {
        "eventflow_implementation": "python",
        "eventflow_gil": gil_enabled,
        "eventflow_revision": source["revision"] or "unknown",
        "eventflow_source_dirty": (
            "unknown"
            if source["source_dirty"] is None
            else ("true" if source["source_dirty"] else "false")
        ),
        "eventflow_source_sha256": source["source_sha256"] or "unknown",
    }


def create_runner(
    description: str,
    *,
    add_cmdline_args: Callable[[list[str], Namespace], None] | None = None,
) -> Runner:
    import pyperf

    main_module = getattr(sys.modules.get("__main__"), "__spec__", None)
    program_args = ("-m", main_module.name) if main_module is not None else None
    runner = cast(
        Runner,
        pyperf.Runner(
            metadata=benchmark_metadata(),
            program_args=program_args,
            add_cmdline_args=add_cmdline_args,
        ),
    )
    runner.argparser.set_defaults(inherit_environ=("PYTHON_GIL",))
    runner.argparser.description = description
    return runner


def bench_sync(
    runner: Runner,
    name: str,
    operation: Callable[[], object],
    *,
    inner_loops: int = 1,
    metadata: dict[str, str | int | float] | None = None,
) -> None:
    bench_time_func(
        runner,
        name,
        _time_sync,
        operation,
        inner_loops=inner_loops,
        metadata=metadata,
    )


def bench_time_func(
    runner: Runner,
    name: str,
    time_func: Callable[..., float],
    *args: object,
    inner_loops: int = 1,
    metadata: dict[str, str | int | float] | None = None,
) -> None:
    runner.bench_time_func(
        name,
        time_func,
        *args,
        inner_loops=inner_loops,
        metadata=metadata,
    )


def bench_async(
    runner: Runner,
    name: str,
    operation: Callable[[], Awaitable[object]],
    *,
    inner_loops: int = 1,
    metadata: dict[str, str | int | float] | None = None,
) -> None:
    runner.bench_async_func(
        name,
        operation,
        inner_loops=inner_loops,
        metadata=metadata,
    )


def _time_sync(loops: int, operation: Callable[[], object]) -> float:
    started = perf_counter()
    for _ in range(loops):
        operation()
    return perf_counter() - started
