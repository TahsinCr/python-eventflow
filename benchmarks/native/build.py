"""Build optional C and Cython benchmark extensions outside the source tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import cast


def _compiler_version(compiler: object) -> str:
    command = getattr(compiler, "compiler", None)
    if not isinstance(command, (list, tuple)) or not command:
        return "unknown"
    executable = str(command[0])
    for option in ("--version", "/Bv"):
        try:
            result = subprocess.run(
                [executable, option],
                capture_output=True,
                check=False,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        output = result.stdout or result.stderr
        if result.returncode == 0 and output.strip():
            return output.splitlines()[0].strip()
    return "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    args = parser.parse_args()

    from setuptools import Distribution, Extension

    cythonize = cast(
        Callable[..., list[Extension]],
        import_module("Cython.Build").cythonize,
    )

    root = Path(__file__).resolve().parents[2]
    output = args.build_dir.resolve()
    generated = output / "generated"
    output.mkdir(parents=True, exist_ok=True)
    extensions = [
        Extension(
            "eventflow_native_c",
            [str(root / "benchmarks/native/kernels.c")],
        ),
        Extension(
            "eventflow_native_cython",
            [str(root / "benchmarks/native/kernels.pyx")],
        ),
    ]
    extensions = cythonize(
        extensions,
        build_dir=str(generated),
        compiler_directives={"language_level": 3},
        quiet=True,
    )
    distribution = Distribution({"ext_modules": extensions})
    build_ext = distribution.get_command_obj("build_ext")
    build_ext.build_lib = str(output)
    build_ext.build_temp = str(output / "temp")
    build_ext.ensure_finalized()
    build_ext.run()

    compiler = build_ext.compiler
    artifacts = {}
    for extension in extensions:
        artifact = Path(build_ext.get_ext_fullpath(extension.name))
        digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
        artifacts[extension.name] = {
            "path": artifact.name,
            "sha256": digest,
        }
    manifest = {
        "python": platform.python_version(),
        "implementation": sys.implementation.name,
        "platform": platform.platform(),
        "cython": import_module("Cython").__version__,
        "setuptools": import_module("setuptools").__version__,
        "compiler_type": compiler.compiler_type,
        "compiler_version": _compiler_version(compiler),
        "compiler": getattr(compiler, "compiler", []),
        "compiler_so": getattr(compiler, "compiler_so", []),
        "linker_so": getattr(compiler, "linker_so", []),
        "compile_options": getattr(compiler, "compile_options", []),
        "link_options": getattr(compiler, "link_options", []),
        "environment": {
            name: os.environ.get(name, "") for name in ("CC", "CFLAGS", "CPPFLAGS", "LDFLAGS")
        },
        "artifacts": artifacts,
    }
    manifest_path = output / "native-build.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Built optional native candidates in {output}")
    print(f"Build details: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
