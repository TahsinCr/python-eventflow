"""Small in-process policy evaluation with explicit deny precedence."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from ._callbacks import synchronous_result

PolicyResult = Literal["allow", "deny", "abstain"]
Rule = Callable[[object, str, object, Mapping[str, object]], PolicyResult | bool | None]
__all__ = ["PolicyDecision", "PolicyEvaluator"]


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    allowed: bool
    reason: str
    matched: tuple[str, ...]


class PolicyEvaluator:
    """Evaluate sync rules; any explicit deny wins and the default is deny."""

    def __init__(
        self,
        *,
        default: Literal["allow", "deny"] = "deny",
        observer: Callable[[PolicyDecision], None] | None = None,
    ) -> None:
        if default not in ("allow", "deny"):
            raise ValueError("default must be 'allow' or 'deny'")
        if observer is not None and not callable(observer):
            raise TypeError("observer must be callable")
        self._default = default
        self._observer = observer
        self._rules: dict[str, Rule] = {}
        self._rules_snapshot: tuple[tuple[str, Rule], ...] | None = None
        self._lock = threading.RLock()

    def add(self, name: str, rule: Rule) -> Rule:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("rule name must be non-empty")
        if not callable(rule):
            raise TypeError("rule must be callable")
        with self._lock:
            if name in self._rules:
                raise KeyError(f"Policy rule {name!r} already exists")
            self._rules[name] = rule
            self._rules_snapshot = None
        return rule

    def remove(self, name: str) -> Rule:
        with self._lock:
            rule = self._rules.pop(name)
            self._rules_snapshot = None
            return rule

    def snapshot(self) -> Mapping[str, Rule]:
        with self._lock:
            return MappingProxyType(dict(self._rules))

    def evaluate(
        self,
        *,
        subject: object,
        action: str,
        resource: object,
        context: Mapping[str, object] | None = None,
    ) -> PolicyDecision:
        if not isinstance(action, str) or not action.strip():
            raise ValueError("action must be a non-empty string")
        values = MappingProxyType(dict(context or {}))
        with self._lock:
            rules = self._rules_snapshot
            if rules is None:
                rules = tuple(self._rules.items())
                self._rules_snapshot = rules
        if not rules:
            decision = PolicyDecision(self._default == "allow", "default policy", ())
            if self._observer is not None:
                synchronous_result(self._observer(decision))
            return decision
        allowed: list[str] = []
        denied: list[str] = []
        for name, rule in rules:
            result = synchronous_result(rule(subject, action, resource, values))
            if result is True or result == "allow":
                allowed.append(name)
            elif result is False or result == "deny":
                denied.append(name)
            elif result is not None and result != "abstain":
                raise TypeError(
                    f"Policy rule {name!r} must return allow, deny, abstain, bool, or None"
                )
        if denied:
            decision = PolicyDecision(False, "explicit deny", tuple(denied))
        elif allowed:
            decision = PolicyDecision(True, "explicit allow", tuple(allowed))
        else:
            decision = PolicyDecision(self._default == "allow", "default policy", ())
        if self._observer is not None:
            synchronous_result(self._observer(decision))
        return decision
