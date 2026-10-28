"""Expose standard-library features across supported Python versions."""

from __future__ import annotations

import sys

if sys.version_info >= (3, 11):
    from builtins import BaseExceptionGroup, ExceptionGroup
    from enum import StrEnum
else:  # pragma: no cover - exercised by the Python 3.10 environment
    from backports.strenum import StrEnum
    from exceptiongroup import BaseExceptionGroup, ExceptionGroup

__all__ = ["BaseExceptionGroup", "ExceptionGroup", "StrEnum"]
