**English** | [Türkçe](https://github.com/TahsinCr/python-eventflow/blob/main/README-TR.md)

# EventFlow

[Repository](https://github.com/TahsinCr/python-eventflow) · [Latest release](https://github.com/TahsinCr/python-eventflow/releases/latest) · [MIT License](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE)

EventFlow lets components in one Python process communicate through ordinary
Python objects. It provides a synchronous event bus, an asyncio-native event
bus, optional in-memory history, named channels, and explicit subscription
lifecycle management.

The library is for in-process delivery. It does not persist events, serialize
them, or transport them between processes. Those responsibilities remain with
the application or its messaging infrastructure.

EventFlow was extracted from the event subsystem of
[Vehicle Autonomy Core](https://github.com/TahsinCr/vehicle-autonomy-core) and
is maintained here as a standalone library. The source project is not needed
to install or use this package.

## Contents

- [Getting started](#getting-started)
  - [Install](#install)
  - [Version](#version)
  - [Publish your first event](#first-event)
  - [Choose a bus](#choose-a-bus)
- [Core concepts](#core-concepts)
  - [Events and subscriptions](#subscriptions)
  - [Filtering and delivery limits](#filtering)
  - [Failures and results](#failures)
- [Working with events](#working-with-events)
  - [History and replay](#history)
  - [Waiting for an event](#wait-for-event)
  - [Periodic publishing](#periodic)
- [Async applications](#async-applications)
- [Organizing channels](#channels)
- [Callback policies](#callback-policies)
- [Lifecycle and reference](#reference)
  - [Defaults](#defaults)
  - [Bus methods](#bus-api)
  - [Engine methods](#engine-api)
  - [Public types and errors](#public-types)
- [Development and license](#development)

## Getting started

### Install

Python 3.10 or newer is required. From the repository root, install the
distribution into the active Python environment:

~~~bash
python -m pip install .
~~~

The distribution name is `eventflow`; its import package is `eventflow`. Python
3.10 installs `backports.strenum` and `exceptiongroup` to supply features that
are built into Python 3.11 and newer. These dependencies are selected by the
package metadata only on Python 3.10.

The `eventflow` name is currently registered on PyPI to another project
([PyPI listing](https://pypi.org/project/eventflow/)). Consequently,
`python -m pip install eventflow` does not install this library. For now, use
the source installation above. Publishing this project under that distribution
name requires an authorized transfer or the selection of an available name;
the Python import package can remain `eventflow` either way.

For editable development, install the optional tools and run the repository
checks from the project root:

~~~bash
python -m pip install -e ".[dev]"
python -m unittest discover -s tests -v
ruff check .
pyright
~~~

The `dev` extra provides Ruff and Pyright. The tests use Python's standard
`unittest` framework.

### Version

The installed package exposes its version through `eventflow.__version__`:

~~~python
from eventflow import __version__

print(__version__)  # 1.0
~~~

<a id="first-event"></a>
### Publish your first event

An event is a value that describes something that has happened. It can be any
Python object; a frozen dataclass is a useful choice when the event should be
explicit and immutable.

~~~python
from dataclasses import dataclass

from eventflow import EventBus


@dataclass(frozen=True)
class OrderPlaced:
    order_id: str


def send_confirmation(event: OrderPlaced) -> None:
    print(f"Order placed: {event.order_id}")


with EventBus[OrderPlaced]() as bus:
    with bus.subscribe(send_confirmation):
        result = bus.publish(OrderPlaced("order-42"))
        print(f"delivered={result.delivered}, failed={result.failed}")
~~~

Output:

~~~text
Order placed: order-42
delivered=1, failed=0
~~~

The publisher creates an `OrderPlaced` value and passes it to the bus. The bus
calls the subscribed handler before `publish()` returns. The inner context
manager cancels the subscription when it ends; the outer one closes the bus.
This makes the lifetime of both resources visible at the point of use.

The generic annotation in `EventBus[OrderPlaced]` helps type checkers and IDEs.
It does not validate event types at runtime. Use a filter when a runtime type
check is needed.

### Choose a bus

| | `EventBus` | `AsyncEventBus` |
| --- | --- | --- |
| Handler | Synchronous callable | Async callable |
| Publish | Blocks until selected handlers finish | Await `publish()` |
| Execution context | Calling thread by default; optional executor | One owning asyncio event loop |
| Delivery order | Concurrent publisher calls may overlap | Sequential by default; optional concurrent delivery |

Choose `EventBus` for synchronous code. By default, each handler runs on the
thread that called `publish()`. An optional `concurrent.futures.Executor` can
run handlers on executor workers, but the publishing call still waits for them.

Publishing from multiple threads is safe. Two simultaneous publish calls may
run their handlers at the same time, so handlers that update shared state must
provide their own synchronization.

Choose `AsyncEventBus` when the application already uses asyncio and handlers
are coroutines. Bus operations belong to the first running event loop that
uses the bus. Do not share an instance across different loops. To publish from
a worker thread, use `publish_threadsafe()`; see [Async applications](#async-applications).

## Core concepts

A bus stores active subscriptions and delivers each published event to the
subscriptions whose filters match it. Subscriptions are independent: a handler
can be cancelled without affecting other handlers or the bus.

<a id="subscriptions"></a>
### Events and subscriptions

Events do not inherit from a library base class. Define the values that suit
the application, then publish them with `publish(event)`. Keep event objects
small and give their fields names that describe the information a handler
needs.

`subscribe(callback)` returns a `Subscription` for `EventBus` and an
`AsyncSubscription` for `AsyncEventBus`. Keep the returned handle while the
handler is needed. Call `cancel()` when it is no longer needed, or manage the
handle with a context manager as in the first example. Cancellation is safe to
repeat. The handle exposes `id` and `active`.

<a id="filtering"></a>
### Filtering and delivery limits

A subscription can receive every event, events of one or more classes, or
events that pass a predicate. A type check and predicate can be combined:

~~~python
from dataclasses import dataclass

from eventflow import EventBus, EventFilter


@dataclass(frozen=True)
class PaymentReceived:
    account_id: str
    amount_cents: int


large_payment = EventFilter(
    event_type=PaymentReceived,
    predicate=lambda event: event.amount_cents >= 10_000,
)


def write_audit_record(event: PaymentReceived) -> None:
    print(f"Audit {event.account_id}: {event.amount_cents}")


with EventBus[PaymentReceived]() as bus:
    with bus.subscribe(write_audit_record, event_filter=large_payment):
        bus.publish(PaymentReceived("acct-7", 2_500))
        bus.publish(PaymentReceived("acct-7", 12_500))
~~~

Only the second payment reaches the handler. A type filter may also be a tuple
of classes. Predicates are synchronous functions that receive the event and
return a boolean, including when attached to an async bus. Async predicates
and predicates that return awaitables are rejected because filtering itself
is synchronous.

Use either an `EventFilter` or the direct `event_type`/`predicate` arguments
on a bus method; do not combine the two forms. For a handler that should stop
after its first match, use `once=True` or `once(callback, ...)`. For a fixed
number of matching deliveries, use `times=N`. These limits count matching
events, not every event published to the bus.

<a id="failures"></a>
### Failures and results

The default `ErrorPolicy.ISOLATE` records handler failures and continues with
the other selected handlers. Each call to `publish()` returns a `PublishResult`:

| Field | Meaning |
| --- | --- |
| `matched` | Subscriptions selected for immediate delivery by this publish call |
| `delivered` | Handlers that completed successfully |
| `failed` / `errors` | Collected delivery or action failures |
| `successful` | `True` when no failures were collected |

Events buffered for a subscription while replay is in progress are excluded
from `matched`, even when its filter accepts them.

A synchronous `on_error` hook can report a failure without coupling the handler
to a particular logging system:

~~~python
from eventflow import EventBus, EventErrorContext


def report_failure(context: EventErrorContext[str]) -> None:
    print(f"{context.event}: {type(context.error).__name__}")


def persist_order(event: str) -> None:
    raise RuntimeError("storage is unavailable")


with EventBus[str](on_error=report_failure) as bus:
    bus.subscribe(persist_order)
    result = bus.publish("order-42")
    print(result.matched, result.delivered, result.failed)
~~~

Set `error_policy=ErrorPolicy.RAISE` when the caller should handle failures
directly. The bus attempts delivery to the selected handlers, then raises an
`ExceptionGroup` if failures were collected. That publish call does not return
a `PublishResult`. The `bus.stats` property provides cumulative
`published`, `delivered`, and `failed` counts.

These counters are not a per-publish invariant. `published` is incremented
before callbacks run; `delivered` and `failed` are finalized afterward. An
in-flight or cancelled async publish can leave them different. `failed` counts
collected errors from handlers, filters, and actions, not only failed handlers.

To catch grouped failures with the same import on Python 3.10 and newer, import
`ExceptionGroup` from `eventflow.compatibility`. On Python 3.11 and newer, that
name refers to the built-in exception group class.

## Working with events

<a id="history"></a>
### History and replay

History is disabled by default. Set `history` to a positive capacity to keep
recent events in a bounded, thread-safe `MemoryEventHistory`. When the buffer
is full, each new event replaces the oldest one. History lives in memory; it
is not a durable event log.

~~~python
from eventflow import EventBus


with EventBus[str](history=3) as bus:
    for status in ("created", "paid", "packed", "shipped"):
        bus.publish(status)

    print(bus.query(limit=2))
    print(bus.latest())
~~~

Output:

~~~text
('packed', 'shipped')
shipped
~~~

`query()` returns retained events in publication order. It accepts the same
filter options as a subscription and an optional positive `limit`. With a
limit, it returns the newest matching events in chronological order.
`latest()` returns the newest match or `None`. Without history,
`query()` returns an empty tuple and `latest()` returns `None`.
History order reflects when events were recorded, not when callbacks start or
finish. Async publish hooks can yield after recording, and separate publish
calls may overlap.

A new subscriber can receive recent events before it starts handling live
ones. Pass `replay=N` to deliver up to the N newest retained events that match
the subscription filter:

~~~python
with EventBus[str](history=10) as bus:
    bus.publish("started")
    bus.publish("ready")

    with bus.subscribe(print, replay=2):
        pass
~~~

The handler prints `started` and `ready` as the subscription is created. Live
events that arrive during replay are buffered per subscriber.
`replay_buffer_limit` (default 1000) bounds this temporary queue. If the queue
overflows, that subscription is cancelled and the overflow is reported using
the bus's error policy.

To provide a custom history store, implement `EventHistory`:
`append()`, `latest()`, `query()`, `clear()`, and `__len__()`. Bus
`clear()` removes active subscriptions; it does not erase the history. To
delete retained events, call `clear()` on the configured history object.
EventFlow calls a custom history's `append()` while holding the bus publication
lock to preserve event order. Keep that method fast and non-blocking, and do
not publish recursively through the same bus from it.

<a id="wait-for-event"></a>
### Waiting for an event

Use `wait_for()` when the caller needs one matching event as a value instead
of registering a long-lived handler. It accepts the same filter options as
`subscribe()` and an optional timeout. It returns the event, or `None` if the
wait times out or the bus closes before a match. A synchronous wait blocks the
calling thread. Timeouts must be finite, non-negative numbers; zero requests
an immediate wait.

This async example schedules a publisher and then waits for its event. The
wait registers its temporary subscription before the publisher task gets to
run:

~~~python
import asyncio

from eventflow import AsyncEventBus


async def main() -> None:
    async with AsyncEventBus[str]() as bus:
        publish_task = asyncio.create_task(bus.publish("ready"))
        event = await bus.wait_for(event_type=str, timeout=1.0)
        await publish_task
        print(event)


asyncio.run(main())
~~~

The output is `ready`. In synchronous code, use
`bus.wait_for(event_type=..., timeout=...)`; omit `timeout` to wait without a
deadline. A configured `on_timeout` hook receives an `EventTimeoutContext`
with the filter and timeout that expired.

<a id="periodic"></a>
### Periodic publishing

`publish_every(event, interval, times=None, immediately=True)` publishes the
same event repeatedly and returns a cancellable subscription. `interval` must
be positive. The first publication happens immediately unless
`immediately=False`. Set `times` to a positive integer for a finite schedule,
or cancel the returned subscription to stop early. A bus allows at most
`max_schedules` active schedules (default 64).

~~~python
import time

from eventflow import EventBus


with EventBus[str]() as bus:
    bus.subscribe(print)
    schedule = bus.publish_every("heartbeat", interval=1.0, times=3)

    while schedule.active:
        time.sleep(0.05)
~~~

The handler receives three heartbeats. `AsyncEventBus` provides the awaited
version and runs it on the owning loop:

~~~python
import asyncio

from eventflow import AsyncEventBus


async def main() -> None:
    async with AsyncEventBus[str]() as bus:
        async def report(message: str) -> None:
            print(message)

        await bus.subscribe(report)
        schedule = await bus.publish_every("heartbeat", 0.05, times=3)
        while schedule.active:
            await asyncio.sleep(0.01)


asyncio.run(main())
~~~

Closing a bus cancels its schedules. With `ErrorPolicy.RAISE`, an error during
a periodic publish stops that schedule. A schedule does not return each
`PublishResult`; configure the bus `on_error` hook to observe handler, filter,
or action failures. With `ErrorPolicy.ISOLATE`, the schedule continues and the
publish result is otherwise discarded.

## Async applications

`AsyncEventBus` accepts async handlers and async bus hooks. Its default
`DeliveryMode.SEQUENTIAL` awaits matching handlers in subscription order.
`DeliveryMode.CONCURRENT` awaits them together. Concurrent mode can be useful
when handlers are independent, but their completion order is not guaranteed.
Sequential ordering applies within one `publish()` call; separate publish calls
may overlap. In sequential mode, a handler-raised `asyncio.CancelledError`
propagates from `publish()` and stops delivery to later handlers. In concurrent
mode, an independently cancelled handler task is recorded as a failed delivery
in `PublishResult`. Cancelling the task that called `publish()` propagates in
either mode and cancels in-flight handler work.

~~~python
import asyncio

from eventflow import AsyncEventBus, DeliveryMode


async def main() -> None:
    async with AsyncEventBus[int](
        delivery_mode=DeliveryMode.CONCURRENT,
    ) as bus:
        async def save_metric(value: int) -> None:
            print(f"Saved metric: {value}")

        async def notify_dashboard(value: int) -> None:
            print(f"Dashboard notified: {value}")

        await bus.subscribe(save_metric)
        await bus.subscribe(notify_dashboard)
        result = await bus.publish(42)
        print(f"delivered={result.delivered}")


asyncio.run(main())
~~~

Bus operations such as `subscribe()`, `publish()`, `wait_for()`,
`publish_every()`, `clear()`, and `close()` are awaited. The
`publish_threadsafe(event)` entry point is for a different thread: it returns
a `concurrent.futures.Future` and schedules delivery on the owning loop.

~~~python
import asyncio

from eventflow import AsyncEventBus


async def main() -> None:
    async with AsyncEventBus[str]() as bus:
        async def report(message: str) -> None:
            print(message)

        await bus.subscribe(report)  # binds the bus to this loop
        future = await asyncio.to_thread(
            bus.publish_threadsafe,
            "from a worker",
        )
        result = await asyncio.wrap_future(future)
        print(result.delivered)


asyncio.run(main())
~~~

Do not call `future.result()` on the event-loop thread: waiting there would
block the loop needed to deliver the event.

<a id="channels"></a>
## Organizing channels

`EventEngine` creates synchronous buses lazily by channel name. Use it when
several parts of an application need separate event channels with shared
defaults:

~~~python
from eventflow import EventEngine


def update_order_view(event: str) -> None:
    print(f"Order view updated: {event}")


with EventEngine(history=50) as engine:
    engine.subscribe("orders", update_order_view)
    result = engine.publish("orders", "order-42")
    print(result.delivered)
~~~

Channel names are stripped and lowercased, so `" Orders "` and `"orders"`
refer to the same channel. `engine.channel(name)` returns a channel, creating
it when necessary. Use `engine.add(name, bus)` to register a bus with custom
settings. `remove(name)` closes one channel; `stop()` closes every channel.
An engine can own a bus instance under only one channel name at a time.
Treat a bus passed to `add()` as owned by that engine; do not pass the same bus
to another engine, since removing or stopping either engine closes it.
The engine exposes `running` and a sorted `channel_names` tuple.
During `stop()`, both engines attempt every channel. If some closes fail, the
failures are raised together and the engine retains those channels so a later
`stop()` can retry cleanup.

`AsyncEventEngine` offers the same channel model for async buses. Its
`channel()` and `add()` methods return directly. Publish, subscribe, once,
wait, remove, start, and stop operations are awaited:

~~~python
import asyncio

from eventflow import AsyncEventEngine


async def main() -> None:
    async with AsyncEventEngine(history=50) as engine:
        async def update_order_view(event: str) -> None:
            print(f"Order view updated: {event}")

        await engine.subscribe("orders", update_order_view)
        result = await engine.publish("orders", "order-42")
        print(result.delivered)


asyncio.run(main())
~~~

Engine defaults are passed to the buses the engine creates. `EventEngine`
carries history, error policy, executor, and hooks. `AsyncEventEngine` carries
history, error policy, delivery mode, and async hooks.

## Callback policies

`CallbackSubscription` is a lower-level utility for applying limits and hooks
to one callback when it is invoked directly. It is independent of a bus and is
not created by `EventBus.subscribe()`. A caller supplies its identifier, a
cancellation function, and the callback:

~~~python
from eventflow import CallbackSubscription


def handle_message(message: str) -> str:
    return message.upper()


policy = CallbackSubscription(
    identifier=1,
    cancel=lambda: None,
    callback=handle_message,
    max_calls=3,
    frequency_hz=2,
    on_success=lambda context: print(context.result),
)

print(policy.invoke("ready"))
~~~

`invoke()` applies the policy to a synchronous callback. For an asynchronous
callback, set `asynchronous=True` and await `invoke_async()`:

~~~python
import asyncio

from eventflow import CallbackSubscription


async def handle_message(message: str) -> str:
    return message.upper()


async def main() -> None:
    policy = CallbackSubscription(
        identifier=2,
        cancel=lambda: None,
        callback=handle_message,
        asynchronous=True,
        timeout=1.0,
    )
    print(await policy.invoke_async("ready"))


asyncio.run(main())
~~~

The policy can limit accepted calls with `once` or `max_calls`, cap the rate
with `frequency_hz`, apply a timeout, filter with a synchronous predicate,
and be toggled with `enable()`/`disable()`. It supports before, success,
error, timeout, and after hooks; each hook receives a `CallbackContext` with
the event, result, error, and elapsed time. Disabled, filtered, and
rate-limited events do not consume the call limit. An invocation that passes
those checks consumes a call even if the callback later raises.

Use `on_before()`, `on_success()`, `on_error()`, `on_timeout()`, or
`on_after()` to register hooks after construction. Calling the wrapper itself
invokes the underlying callback directly and bypasses its policies; use
`invoke()` or `invoke_async()` when those policies should apply.

A synchronous timeout is observational: Python cannot safely terminate a
running thread, so the callback completes before an overrun is reported.
An asynchronous timeout is cooperative: it requests cancellation and waits for
the callback task to finish before raising `CallbackTimeoutError`. A callback
that suppresses cancellation, or a hook that does not return, can extend the
invocation beyond its configured budget. Python cannot force-stop a coroutine.
A callback's own `TimeoutError` is distinct from `CallbackTimeoutError`.

<a id="reference"></a>
## Lifecycle and reference

### Defaults

| Option | `EventBus` | `AsyncEventBus` |
| --- | --- | --- |
| `history` | `None` | `None` |
| `error_policy` | `ErrorPolicy.ISOLATE` | `ErrorPolicy.ISOLATE` |
| `executor` | `None` | Not used |
| `delivery_mode` | Not used | `DeliveryMode.SEQUENTIAL` |
| `replay_buffer_limit` | 1000 | 1000 |
| `max_schedules` | 64 | 64 |
| `shutdown_timeout` | 5 seconds | Not used |
| `actions` or direct `on_*` hooks | None | None |

`history` accepts a positive capacity or an `EventHistory` implementation.
Pass either an `EventBusActions`/`AsyncEventBusActions` object or individual
`on_before`, `on_after`, `on_error`, and `on_timeout` callbacks, not both.
Hooks on `EventBus` are synchronous; hooks on `AsyncEventBus` are async.

Use `close()` when a bus is no longer needed; context managers do this
automatically. A closed bus rejects new subscriptions and publications with
`EventBusClosedError`. `clear()` removes subscriptions but leaves history
intact and completes pending `wait_for()` calls with `None` on both bus types.
A synchronous wait also returns `None` when the bus closes.

The synchronous bus waits up to `shutdown_timeout` for periodic workers to
stop. If a worker does not stop in time, `EventShutdownTimeoutError` is
raised. The worker is not forcibly terminated, so `close()` can be retried
after the blocking handler finishes. Async close cancels and awaits periodic
tasks. Closing a bus does not wait for active `publish()` calls: a callback
that has already started may finish after `close()` returns.

<a id="bus-api"></a>
### Bus methods

| Method | Behavior |
| --- | --- |
| `subscribe(callback, *, event_filter=None, event_type=None, predicate=None, once=False, times=None, replay=0)` | Register a handler. Supply either `event_filter` or direct filter arguments. |
| `once(callback, *, event_filter=None, event_type=None, predicate=None, replay=0)` | Register for the first matching event. |
| `publish(event)` | Deliver an event and return a `PublishResult`, unless the error policy raises. |
| `wait_for(*, event_filter=None, event_type=None, predicate=None, timeout=None)` | Return one matching event or `None`. |
| `publish_every(event, interval, *, times=None, immediately=True)` | Start a cancellable periodic schedule. |
| `latest(...)` / `query(..., limit=None)` | Read retained history. These queries are synchronous on both bus types. |
| `clear()` / `close()` | Clear subscriptions or close the bus. Both are awaited on `AsyncEventBus`. |

On `AsyncEventBus`, await `subscribe()`, `once()`, `publish()`,
`wait_for()`, `publish_every()`, `clear()`, `close()`, and subscription
cancellation. `publish_threadsafe(event)` is called from another thread
without `await` and returns a `concurrent.futures.Future`.

<a id="engine-api"></a>
### Engine methods

`EventEngine` provides `channel()`, `add()`, `remove()`, `publish()`,
`subscribe()`, `once()`, `wait_for()`, `publish_every()`, `start()`, and
`stop()`. `channel()` and `add()` return directly.

Engine `subscribe()` accepts `event_filter`, `event_type`, `predicate`, `once`,
`times`, and `replay`. Engine `once()` accepts the filter options and `replay`.
The async engine exposes the same typed options for async handlers.

`AsyncEventEngine` provides the same operations for async buses. `channel()`
and `add()` return directly; publishing, subscription, waiting, removal, start,
and stop are awaited. Both engines support context managers; the async engine
uses `async with`.

After a successful `stop()`, an engine can be reused: a later `channel()` or
`add()` call creates or registers new open channels and marks it running again.

<a id="public-types"></a>
### Public types and errors

The package exports the following public names:

| Area | Public names |
| --- | --- |
| Buses | `EventBus`, `AsyncEventBus`, `BaseEventBus` |
| Engines | `EventEngine`, `AsyncEventEngine` |
| Subscriptions | `Subscription`, `AsyncSubscription`, `CallbackSubscription` |
| Filters and history | `EventFilter`, `EventHistory`, `MemoryEventHistory` |
| Results and modes | `PublishResult`, `EventBusStats`, `ErrorPolicy`, `DeliveryMode` |
| Actions and contexts | `EventBusActions`, `AsyncEventBusActions`, `EventErrorContext`, `EventTimeoutContext`, `CallbackContext` |
| Errors | `EventBusError`, `EventBusClosedError`, `InvalidEventHandlerError`, `CallbackTimeoutError`, `EventShutdownTimeoutError` |

`EventFilter` accepts an event class or tuple of classes and an optional
predicate. `MemoryEventHistory(capacity=1000)` is the built-in bounded,
thread-safe history; `capacity` reports its configured size. Buses expose
`closed`, `subscriber_count`, `stats`, `history`, and `error_policy`.
Engines expose `running` and `channel_names`. Subscription handles expose
`id` and `active`; `CallbackSubscription` also exposes `enabled`.

| Error | Raised when |
| --- | --- |
| `EventBusError` | A bus operation or schedule capacity cannot be completed. |
| `EventBusClosedError` | A publish or registration is attempted after close. |
| `InvalidEventHandlerError` | A handler or hook does not match the bus's sync/async mode. |
| `CallbackTimeoutError` | A callback exceeds its configured time budget. |
| `EventShutdownTimeoutError` | A synchronous periodic worker outlives `shutdown_timeout`. |

`BaseEventBus` is the abstract base for custom bus implementations.
`EventHistory` is the interface for custom history stores. Most applications
can work entirely with `EventBus` or `AsyncEventBus`.

<a id="development"></a>
## Development and license

The project supports Python 3.10 and newer and uses setuptools as its build
backend. The compatibility dependencies are installed only on Python 3.10.
Development setup and checks are described under
[Install](https://github.com/TahsinCr/python-eventflow/blob/main/README.md#install).
Run `python -m benchmarks.benchmark_replay` from the repository root to compare
full history reads with bounded sync and async replay setup. The standard
library benchmark defaults to 100,000 retained events and can be adjusted with
`--events`, `--replay`, `--iterations`, and `--samples`.

Copyright © 2026 TahsinCr. EventFlow is licensed under the MIT License.
See [LICENSE](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE) and
[CHANGELOG](https://github.com/TahsinCr/python-eventflow/blob/main/CHANGELOG.md).
