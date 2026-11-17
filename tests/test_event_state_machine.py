"""Shrinkable reference traces for routed, finite EventBus subscriptions."""

from collections import deque
from dataclasses import dataclass

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, precondition, rule

from eventflow import EventBus, Subscription


@dataclass
class Subscriber:
    handle: Subscription
    route: int | None
    remaining: int
    fails: bool


class EventBusModel(RuleBasedStateMachine):
    identifiers = Bundle("identifiers")

    def __init__(self):
        super().__init__()
        self.bus = EventBus[int](history=7, routing_key=lambda event: event % 2)
        self.history: deque[int] = deque(maxlen=7)
        self.subscribers: dict[int, Subscriber] = {}
        self.received: list[tuple[int, int]] = []
        self.expected: list[tuple[int, int]] = []
        self.published = self.delivered = self.failed = 0

    def teardown(self):
        self.bus.close()

    @rule(
        target=identifiers,
        route=st.sampled_from([None, 0, 1]),
        quota=st.integers(1, 4),
        replay=st.integers(0, 5),
        fails=st.booleans(),
    )
    def subscribe(self, route, quota, replay, fails):
        identifier = len(self.subscribers)

        def receive(event):
            self.received.append((identifier, event))
            if fails:
                raise ValueError("callback rejected event")

        handle = self.bus.subscribe(
            receive,
            routing_key=route,
            predicate=lambda event: event >= 0,
            replay=replay,
            once=quota == 1,
            times=quota if quota > 1 else None,
        )
        matches = [event for event in self.history if event >= 0 and route in (None, event % 2)]
        selected = matches[-replay:][:quota] if replay else []
        self.subscribers[identifier] = Subscriber(handle, route, quota - len(selected), fails)
        self.expected.extend((identifier, event) for event in selected)
        self.delivered += 0 if fails else len(selected)
        self.failed += len(selected) if fails else 0
        return identifier

    @rule(event=st.integers(-4, 4))
    def publish(self, event):
        self.history.append(event)
        self.published += 1
        delivered = failed = 0
        for identifier, subscriber in self.subscribers.items():
            if not subscriber.remaining or event < 0 or subscriber.route not in (None, event % 2):
                continue
            self.expected.append((identifier, event))
            subscriber.remaining -= 1
            if subscriber.fails:
                failed += 1
            else:
                delivered += 1
        result = self.bus.publish(event)
        assert (result.matched, result.delivered, result.failed) == (
            delivered + failed,
            delivered,
            failed,
        )
        assert all(isinstance(error, ValueError) for error in result.errors)
        self.delivered += delivered
        self.failed += failed

    @rule(identifier=identifiers)
    def cancel(self, identifier):
        subscriber = self.subscribers[identifier]
        subscriber.handle.cancel()
        subscriber.remaining = 0

    @precondition(lambda self: bool(self.history))
    @rule()
    def reject_replay_filter(self):
        def reject(event):
            raise LookupError(event)

        try:
            self.bus.subscribe(lambda _: None, replay=1, once=True, predicate=reject)
        except LookupError as error:
            assert error.args == (self.history[-1],)
        else:
            raise AssertionError("expected the replay filter error")

    @invariant()
    def matches_model(self):
        assert self.received == self.expected
        self.received.clear()
        self.expected.clear()
        assert self.bus.query() == tuple(self.history)
        active = [subscriber.remaining > 0 for subscriber in self.subscribers.values()]
        assert [subscriber.handle.active for subscriber in self.subscribers.values()] == active
        assert self.bus.subscriber_count == sum(active)
        stats = self.bus.stats
        assert (stats.published, stats.delivered, stats.failed) == (
            self.published,
            self.delivered,
            self.failed,
        )


TestEventBusModel = EventBusModel.TestCase
TestEventBusModel.settings = settings(
    max_examples=60, stateful_step_count=35, deadline=None, derandomize=True
)
