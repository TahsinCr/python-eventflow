"""Shared callback and argument helpers for benchmark workloads."""

from __future__ import annotations

import argparse
import asyncio


def positive_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a positive integer") from exc
    if result < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return result


def ignore_event(_event: object) -> None:
    pass


async def ignore_event_async(_event: object) -> None:
    pass


async def yield_once_async(_event: object) -> None:
    await asyncio.sleep(0)
