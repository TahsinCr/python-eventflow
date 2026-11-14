"""Private loader for the optional compiled EventFlow C extension."""

from __future__ import annotations

import os
import sys
from importlib import import_module
from typing import Any


def _load_core() -> Any:
    if os.environ.get("EVENTFLOW_NO_EXTENSIONS") == "1":
        return None
    gil_check = getattr(sys, "_is_gil_enabled", None)
    gil_was_disabled = gil_check is not None and not gil_check()
    try:
        module = import_module("._core", __package__)
    except ImportError:
        return None
    if gil_was_disabled and gil_check is not None and gil_check():
        raise RuntimeError("importing eventflow._core enabled the free-threaded GIL")
    return module


CORE = _load_core()
