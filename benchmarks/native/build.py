"""Build optional C and Cython benchmark extensions outside the source tree."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import cast


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
    print(f"Built optional native candidates in {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
