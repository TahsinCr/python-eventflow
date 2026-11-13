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
if native_build == "1":
    root = Path(__file__).resolve().parent
    sys.path.insert(0, str(root))
    from benchmarks.native.build import native_extensions

    extensions = native_extensions(root, root / "build" / "cython")

setup(ext_modules=extensions)
