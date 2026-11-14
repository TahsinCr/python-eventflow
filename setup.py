from __future__ import annotations

import os
import sys

from setuptools import Extension, setup

extensions: list[Extension] = []

pure_python = os.environ.get("EVENTFLOW_PURE_PYTHON", "0")
if pure_python not in {"0", "1"}:
    raise RuntimeError("EVENTFLOW_PURE_PYTHON must be '0' or '1'")

if pure_python == "0":
    extensions.append(
        Extension(
            "eventflow._core",
            sources=["eventflow/_core.c"],
            extra_compile_args=[] if sys.platform == "win32" else ["-Wall", "-Wextra"],
            optional=True,
        )
    )

setup(ext_modules=extensions)
