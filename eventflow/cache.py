"""Bounded in-memory version-aware LRU cache."""

from __future__ import annotations

import heapq
import itertools
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from typing import Generic, TypeVar

from ._callbacks import synchronous_result
from .base import validate_finite_duration, validate_positive_capacity

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")
__all__ = ["VersionedCache", "CacheStats"]


@dataclass(slots=True)
class _Entry(Generic[V]):
    value: V
    version: int
    size: int
    expires_at: float | None
    expiry_token: int


@dataclass(frozen=True, slots=True)
class CacheStats:
    entries: int
    bytes: int
    hits: int
    misses: int
    evictions: int
    stale_writes: int


class VersionedCache(Generic[K, V]):
    """Thread-safe bounded LRU; stale versions are rejected while entries remain cached."""

    def __init__(
        self,
        *,
        max_entries: int,
        max_bytes: int | None = None,
        ttl: float | None = None,
        getsizeof: Callable[[K, V], int] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_entries = validate_positive_capacity(max_entries, name="max_entries")
        if max_bytes is not None:
            validate_positive_capacity(max_bytes, name="max_bytes")
            if getsizeof is None:
                raise ValueError("getsizeof is required when max_bytes is configured")
        if ttl is not None:
            ttl = validate_finite_duration(ttl, name="ttl")
        if getsizeof is not None and not callable(getsizeof):
            raise TypeError("getsizeof must be callable")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._max_bytes = max_bytes
        self._ttl = ttl
        self._has_custom_getsizeof = getsizeof is not None
        self._getsizeof = getsizeof if getsizeof is not None else (lambda _key, _value: 1)
        self._clock = clock
        self._entries: OrderedDict[K, _Entry[V]] = OrderedDict()
        self._expiry_heap: list[tuple[float, int, K]] = []
        self._expiry_tokens = itertools.count()
        self._bytes = 0
        self._hits = self._misses = self._evictions = self._stale_writes = 0
        self._lock = threading.RLock()

    def _expire(self, now: float) -> None:
        heap = self._expiry_heap
        if not heap:
            return
        while heap and heap[0][0] <= now:
            expires_at, token, key = heapq.heappop(heap)
            entry = self._entries.get(key)
            if entry is not None and entry.expiry_token == token and entry.expires_at == expires_at:
                self._entries.pop(key)
                self._bytes -= entry.size
        if len(heap) > 64 and len(heap) > len(self._entries) * 2:
            self._expiry_heap = [
                (entry.expires_at, entry.expiry_token, key)
                for key, entry in self._entries.items()
                if entry.expires_at is not None
            ]
            heapq.heapify(self._expiry_heap)

    def put(self, key: K, value: V, *, version: int, ttl: float | None = None) -> bool:
        """Store a value; reject stale versions and values too large for the byte bound."""
        if type(version) is not int or version < 0:
            if isinstance(version, bool) or not isinstance(version, int) or version < 0:
                raise ValueError("version must be a non-negative integer")
        lifetime = self._ttl if ttl is None else validate_finite_duration(ttl, name="ttl")
        if self._has_custom_getsizeof:
            size = synchronous_result(self._getsizeof(key, value))
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                raise ValueError("getsizeof must return a non-negative integer")
        else:
            size = 1
        if self._max_bytes is not None and size > self._max_bytes:
            return False
        with self._lock:
            now = self._clock() if lifetime is not None or self._expiry_heap else 0.0
            if self._expiry_heap:
                self._expire(now)
            current = self._entries.get(key)
            if current is not None and version < current.version:
                self._stale_writes += 1
                return False
            if lifetime is None:
                expires_at = None
                expiry_token = -1
            else:
                expires_at = now + lifetime
                expiry_token = next(self._expiry_tokens)
            if current is not None:
                self._bytes += size - current.size
                current.value = value
                current.version = version
                current.size = size
                current.expires_at = expires_at
                current.expiry_token = expiry_token
                self._entries.move_to_end(key)
            else:
                self._entries[key] = _Entry(value, version, size, expires_at, expiry_token)
                self._bytes += size
            if expires_at is not None:
                heapq.heappush(self._expiry_heap, (expires_at, expiry_token, key))
                if expires_at <= now:
                    self._expire(now)
            while len(self._entries) > self._max_entries or (
                self._max_bytes is not None and self._bytes > self._max_bytes
            ):
                _old_key, old = self._entries.popitem(last=False)
                self._bytes -= old.size
                self._evictions += 1
            return True

    def get(self, key: K, *, version: int | None = None) -> V | None:
        """Return and promote a matching value; None means absent, expired, or stale."""
        if version is not None and (type(version) is not int or version < 0):
            if isinstance(version, bool) or not isinstance(version, int) or version < 0:
                raise ValueError("version must be a non-negative integer or None")
        with self._lock:
            if self._expiry_heap:
                self._expire(self._clock())
            entry = self._entries.get(key)
            if entry is None or (version is not None and entry.version != version):
                self._misses += 1
                return None
            self._entries.move_to_end(key)
            self._hits += 1
            return entry.value

    def remove(self, key: K) -> V | None:
        with self._lock:
            if self._expiry_heap:
                self._expire(self._clock())
            entry = self._entries.pop(key, None)
            if entry is None:
                return None
            self._bytes -= entry.size
            return entry.value

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._expiry_heap.clear()
            self._bytes = 0

    def prune(self) -> int:
        with self._lock:
            if not self._expiry_heap:
                return 0
            before = len(self._entries)
            self._expire(self._clock())
            return before - len(self._entries)

    def stats(self) -> CacheStats:
        with self._lock:
            if self._expiry_heap:
                self._expire(self._clock())
            return CacheStats(
                len(self._entries),
                self._bytes,
                self._hits,
                self._misses,
                self._evictions,
                self._stale_writes,
            )
