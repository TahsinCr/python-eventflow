from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import shutil
import subprocess
import sys
import sysconfig
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COPY_DIRECTORIES = ("eventflow", "tests", "examples", "benchmarks", "docs")
COPY_FILES = (
    "setup.py",
    "pyproject.toml",
    "run_tests.py",
    "README.md",
    "README-TR.md",
    "LICENSE",
    "CHANGELOG.md",
)
IGNORE = shutil.ignore_patterns(
    ".git",
    ".serena",
    ".venv",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "__pycache__",
    "*.pyc",
    "*.pyo",
    "*.so",
    "*.pyd",
    "build",
    "dist",
    "*.egg-info",
    "results",
)


def _run(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    log: Path | None = None,
) -> str:
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    output = result.stdout
    if log is not None:
        log.write_text(output, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: {' '.join(command)}\n"
            f"{output[-12000:]}"
        )
    return output


def _version(command: str) -> str:
    executable = shutil.which(command)
    if executable is None:
        raise RuntimeError(f"Required executable not found: {command}")
    output = subprocess.run(
        [executable, "--version"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True,
    ).stdout
    return output.splitlines()[0]


def _git_value(*args: str) -> str | None:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _copy_source(destination: Path) -> None:
    for name in COPY_DIRECTORIES:
        source = ROOT / name
        if source.is_dir():
            shutil.copytree(source, destination / name, ignore=IGNORE)
    for name in COPY_FILES:
        source = ROOT / name
        if source.is_file():
            shutil.copy2(source, destination / name)


def _coverage_summary(path: Path) -> dict[str, int | float | str]:
    report = json.loads(path.read_text(encoding="utf-8"))
    files = report.get("files", [])
    matching = [item for item in files if item.get("filename") == "eventflow/_core.c"]
    if len(matching) != 1 or len(files) != 1:
        raise RuntimeError(
            "Coverage must contain exactly eventflow/_core.c; "
            f"reported files: {[item.get('filename') for item in files]}"
        )

    item = matching[0]
    totals = {
        "line_total": item.get("line_total", 0),
        "line_covered": item.get("line_covered", 0),
        "line_percent": item.get("line_percent", 0.0),
        "function_total": item.get("function_total", 0),
        "function_covered": item.get("function_covered", 0),
        "function_percent": item.get("function_percent", 0.0),
        "branch_total": item.get("branch_total", 0),
        "branch_covered": item.get("branch_covered", 0),
        "branch_percent": item.get("branch_percent", 0.0),
    }
    if not all(totals[key] for key in ("line_total", "function_total", "branch_total")):
        raise RuntimeError(f"Coverage data is empty or incomplete: {totals}")
    if not all(totals[key] for key in ("line_covered", "function_covered", "branch_covered")):
        raise RuntimeError(f"No executed C coverage data was recorded: {totals}")
    return totals


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build an isolated GCC-instrumented EventFlow C extension and report C coverage."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "build" / "native-c-coverage",
        help="Directory for the machine-readable, HTML and text reports.",
    )
    args = parser.parse_args()

    if sys.platform != "linux":
        parser.error("Native C coverage currently requires Linux and GCC/gcov.")
    if sysconfig.get_config_var("Py_GIL_DISABLED"):
        parser.error("Use a regular GIL-enabled CPython build for gcov coverage.")
    if not callable(getattr(sys, "_is_gil_enabled", None)) or not sys._is_gil_enabled():
        parser.error("The coverage interpreter must report that the GIL is enabled.")

    gcc = shutil.which("gcc")
    gcov = shutil.which("gcov")
    gcovr = shutil.which("gcovr")
    if gcc is None or gcov is None or gcovr is None:
        parser.error("Install GCC, its matching gcov, and gcovr==8.8 before running coverage.")

    missing = [
        name
        for name in ("setuptools", "hypothesis", "pyperf")
        if not importlib.util.find_spec(name)
    ]
    if missing:
        parser.error(
            f"Missing coverage build/test dependencies: {', '.join(missing)}. Install .[dev] and setuptools>=77."
        )

    output_dir = args.output_dir if args.output_dir.is_absolute() else ROOT / args.output_dir
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    html_dir = output_dir / "html"
    if html_dir.exists():
        shutil.rmtree(html_dir)
    html_dir.mkdir()

    report_path = output_dir / "native-coverage.json"
    summary_path = output_dir / "native-coverage-summary.json"
    for path in (report_path, summary_path):
        path.unlink(missing_ok=True)

    with tempfile.TemporaryDirectory(prefix="eventflow-native-coverage-") as temp:
        project = Path(temp) / "source"
        project.mkdir()
        _copy_source(project)

        env = os.environ.copy()
        for name in ("EVENTFLOW_NO_EXTENSIONS", "EVENTFLOW_PURE_PYTHON", "PYTHONPATH"):
            env.pop(name, None)
        env.update(
            {
                "CC": gcc,
                "CFLAGS": "--coverage -O0 -g",
                "LDFLAGS": "--coverage",
            }
        )

        build_output = _run(
            [sys.executable, "setup.py", "build_ext", "--inplace", "--force"],
            cwd=project,
            env=env,
            log=output_dir / "build.log",
        )
        if "--coverage" not in build_output:
            raise RuntimeError("The extension build did not use GCC coverage instrumentation.")

        probe = _run(
            [
                sys.executable,
                "-c",
                "import json, sys; from pathlib import Path; import eventflow; "
                "from eventflow import _native; "
                "assert _native.CORE is not None, 'native extension did not load'; "
                "module = Path(_native.CORE.__file__).resolve(); "
                "assert module.is_relative_to(Path.cwd()), module; "
                "print(json.dumps({'version': eventflow.__version__, 'core': str(module), "
                "'gil_enabled': sys._is_gil_enabled()}))",
            ],
            cwd=project,
            env=env,
        )
        runtime = json.loads(probe.splitlines()[-1])

        test_env = env.copy()
        test_env["PYTHONPATH"] = str(project / "tests")
        _run(
            [sys.executable, "run_tests.py"],
            cwd=project,
            env=test_env,
            log=output_dir / "tests.log",
        )

        gcno_files = list(project.rglob("*.gcno"))
        gcda_files = list(project.rglob("*.gcda"))
        if not gcno_files or not gcda_files:
            raise RuntimeError(
                f"Expected both compiler notes and runtime data; found "
                f"{len(gcno_files)} .gcno and {len(gcda_files)} .gcda files."
            )

        _run(
            [
                gcovr,
                "--root",
                str(project),
                "--object-directory",
                str(project / "build"),
                "--gcov-executable",
                gcov,
                "--filter",
                r"eventflow/_core\.c$",
                "--json",
                str(report_path),
                "--json-summary",
                str(summary_path),
                "--html-details",
                str(html_dir / "index.html"),
                "--print-summary",
            ],
            cwd=project,
            env=env,
            log=output_dir / "gcovr.log",
        )

        totals = _coverage_summary(summary_path)
        source_digest = hashlib.sha256((ROOT / "eventflow" / "_core.c").read_bytes()).hexdigest()
        status = _git_value("status", "--porcelain", "--untracked-files=normal")
        metadata = {
            "git_revision": _git_value("rev-parse", "HEAD"),
            "git_dirty": bool(status),
            "eventflow_version": runtime["version"],
            "core_module": runtime["core"],
            "python": sys.version,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "gil_enabled": runtime["gil_enabled"],
            "python_build_gil_disabled": sysconfig.get_config_var("Py_GIL_DISABLED"),
            "gcc": _version("gcc"),
            "gcov": _version("gcov"),
            "gcovr": _version("gcovr"),
            "core_source_sha256": source_digest,
            "gcno_files": len(gcno_files),
            "gcda_files": len(gcda_files),
            "coverage": totals,
        }
        (output_dir / "environment.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        lines = [
            "# EventFlow native C coverage",
            "",
            f"Source SHA-256: `{source_digest}`",
            f"Revision: `{metadata['git_revision'] or 'unavailable'}` "
            f"(dirty: `{metadata['git_dirty']}`)",
            f"Runtime: {platform.platform()}, {platform.machine()}, Python {platform.python_version()}, "
            f"GIL enabled: `{runtime['gil_enabled']}`",
            f"Tools: `{metadata['gcc']}`, `{metadata['gcov']}`, `{metadata['gcovr']}`",
            "",
            "| C metric | Covered | Total | Coverage |",
            "| --- | ---: | ---: | ---: |",
            f"| Lines | {totals['line_covered']} | {totals['line_total']} | {totals['line_percent']}% |",
            f"| Functions | {totals['function_covered']} | {totals['function_total']} | {totals['function_percent']}% |",
            f"| Branches | {totals['branch_covered']} | {totals['branch_total']} | {totals['branch_percent']}% |",
            "",
            f"Instrumented test run: `{len(gcno_files)}` compiler-note and `{len(gcda_files)}` runtime-data files.",
            "The JSON report is restricted to `eventflow/_core.c`; empty reports fail this command.",
        ]
        (output_dir / "native-coverage.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

        test_lines = (output_dir / "tests.log").read_text(encoding="utf-8").splitlines()
        print("\n".join(test_lines[-12:]))
        print((output_dir / "native-coverage.md").read_text(encoding="utf-8"))
        print(f"HTML report: {html_dir / 'index.html'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
