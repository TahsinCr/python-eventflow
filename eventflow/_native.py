"""Private, opt-in loading for experimental native runtime kernels."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from importlib import import_module
from typing import Any


def _load_kernel(
    variable: str,
    expected_backend: str,
    module_name: str,
    function_name: str,
) -> tuple[str, Callable[..., Any] | None]:
    backend = os.environ.get(variable, "python")
    if backend == "python":
        return backend, None
    if backend != expected_backend:
        raise RuntimeError(f"{variable} must be 'python' or '{expected_backend}', got {backend!r}")
    gil_check = getattr(sys, "_is_gil_enabled", None)
    gil_was_disabled = gil_check is not None and not gil_check()
    try:
        module = import_module(module_name)
    except ImportError as exc:
        raise RuntimeError(
            f"{variable}={backend!r} requires the optional {module_name} extension"
        ) from exc
    if gil_was_disabled and gil_check is not None and gil_check():
        raise RuntimeError(f"importing {module_name} enabled the free-threaded GIL")
    kernel = getattr(module, function_name, None)
    if not callable(kernel):
        raise RuntimeError(f"{module_name} does not provide {function_name}()")
    return backend, kernel


HISTORY_BACKEND, HISTORY_TYPE_FILTER = _load_kernel(
    "EVENTFLOW_EXPERIMENTAL_HISTORY_BACKEND",
    "cython",
    "eventflow_native_cython",
    "filter_by_type",
)
ROUTING_BACKEND, ROUTING_MERGE = _load_kernel(
    "EVENTFLOW_EXPERIMENTAL_ROUTING_BACKEND",
    "c",
    "eventflow_native_c",
    "merge_subscribers",
)
