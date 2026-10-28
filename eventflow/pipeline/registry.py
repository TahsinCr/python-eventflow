"""Copy-on-change ordered registries; callers serialize configuration changes."""

from __future__ import annotations

import heapq
from dataclasses import dataclass, replace
from typing import Generic, TypeVar

C = TypeVar("C")


def _name(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Pipeline component name must be a non-empty string")
    return value.strip()


@dataclass(frozen=True, slots=True)
class _Entry(Generic[C]):
    name: str
    callback: C
    before: str | None = None
    after: str | None = None
    enabled: bool = True


class _Registry(Generic[C]):
    def __init__(self) -> None:
        self._entries: dict[str, _Entry[C]] = {}
        self.ordered: tuple[_Entry[C], ...] = ()
        self.plan: tuple[_Entry[C], ...] = ()

    def _commit(self, entries: dict[str, _Entry[C]]) -> None:
        positions = {name: index for index, name in enumerate(entries)}
        edges: dict[str, set[str]] = {name: set() for name in entries}
        degree = dict.fromkeys(entries, 0)
        for entry in entries.values():
            for source, target in ((entry.name, entry.before), (entry.after, entry.name)):
                if source is None or target is None:
                    continue
                if source not in entries or target not in entries:
                    raise KeyError(f"Unknown pipeline ordering reference: {source!r}, {target!r}")
                if target not in edges[source]:
                    edges[source].add(target)
                    degree[target] += 1
        ready = [(positions[name], name) for name, count in degree.items() if count == 0]
        heapq.heapify(ready)
        ordered = []
        while ready:
            _, name = heapq.heappop(ready)
            ordered.append(entries[name])
            for target in edges[name]:
                degree[target] -= 1
                if degree[target] == 0:
                    heapq.heappush(ready, (positions[target], target))
        if len(ordered) != len(entries):
            raise ValueError("Pipeline ordering contains a cycle")
        self._entries = entries
        self.ordered = tuple(ordered)
        self.plan = tuple(entry for entry in ordered if entry.enabled)

    def add(self, name: str, callback: C, before: str | None, after: str | None) -> None:
        name = _name(name)
        if name in self._entries:
            raise ValueError(f"Pipeline component already exists: {name}")
        entry = _Entry(
            name,
            callback,
            _name(before) if before is not None else None,
            _name(after) if after is not None else None,
        )
        self._commit({**self._entries, name: entry})

    def remove(self, name: str) -> C:
        name = _name(name)
        removed = self._entries[name]
        entries = {
            key: replace(
                value,
                before=None if value.before == name else value.before,
                after=None if value.after == name else value.after,
            )
            for key, value in self._entries.items()
            if key != name
        }
        self._commit(entries)
        return removed.callback

    def replace(self, name: str, callback: C) -> None:
        name = _name(name)
        self._commit({**self._entries, name: replace(self._entries[name], callback=callback)})

    def enable(self, name: str, enabled: bool) -> None:
        name = _name(name)
        self._commit({**self._entries, name: replace(self._entries[name], enabled=enabled)})
