"""Build a pure Python wheel or an explicitly requested experimental native wheel."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from setuptools import setup

native_build = os.environ.get("EVENTFLOW_BUILD_NATIVE", "0")
if native_build not in {"0", "1"}:
    raise RuntimeError("EVENTFLOW_BUILD_NATIVE must be '0' or '1'")

extensions = []
build_options = {}
if native_build == "1":
    root = Path(__file__).resolve().parent
    build_root = Path(os.environ.get("EVENTFLOW_NATIVE_BUILD_DIR", root / "build")).resolve()
    sys.path.insert(0, str(root))
    from benchmarks.native.build import native_extensions

    extensions = native_extensions(root, build_root / "cython")
    build_options["build_ext"] = {"build_temp": str(build_root / "temp")}

setup(ext_modules=extensions, options=build_options)
