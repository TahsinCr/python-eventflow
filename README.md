**English** | [Türkçe](https://github.com/TahsinCr/python-eventflow/blob/main/README-TR.md)

# EventFlow

[Repository](https://github.com/TahsinCr/python-eventflow) · [Latest release](https://github.com/TahsinCr/python-eventflow/releases/latest) · [MIT License](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE)

EventFlow lets components in one Python process communicate through ordinary
Python objects. It provides a synchronous event bus, an asyncio-native event
bus, optional in-memory history, named channels, and explicit subscription
lifecycle management.

Optional pipelines transform values through named stages and around middleware.
Async streams compose lazy transformations with bounded buffering. Scopes,
event envelopes and opt-in measurements let these pieces work together without
requiring a framework or changing ordinary bus publication.

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
- [Pipelines and middleware](#pipelines)
- [Async pipelines and streams](#streams)
- [Subscription scopes](#scopes)
- [Event context and measurements](#event-context)
- [Security and operational boundaries](#security-and-operational-boundaries)
- [Composing the components](#composition)
- [Lifecycle and reference](#reference)
  - [Defaults](#defaults)
  - [Bus methods](#bus-api)
  - [Engine methods](#engine-api)
  - [Public types and errors](#public-types)
- [Development and license](#development)

## Getting started

### Install

Python 3.10 or newer is required. From a checkout, install the source into the
active environment:

~~~bash
python -m pip install .
~~~

The reserved distribution name for the first PyPI release is
`eventflow-tahsincr`, while the Python import package remains `eventflow`. After
publication, install the distribution with:

~~~bash
python -m pip install eventflow-tahsincr
~~~

Python 3.10 installs `backports.strenum` and `exceptiongroup` to supply features
that are built into Python 3.11 and newer. Package metadata selects these
dependencies only on Python 3.10.

The package includes inline type annotations but does not include the PEP 561
`py.typed` marker. Type-checker discovery for installed distributions is
therefore not guaranteed.

The distribution names `eventflow` and `python-eventflow` are already registered
to other projects on PyPI ([eventflow](https://pypi.org/project/eventflow/),
[python-eventflow](https://pypi.org/project/python-eventflow/)). Use
`eventflow-tahsincr` to install this library; imports continue to use
`eventflow`.

Because the distribution installs files in the shared `eventflow` import
package, installing another distribution that provides that package in the
same environment can overwrite or conflict with these files. Use a separate
virtual environment when both distributions are needed.

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

print(__version__)  # 1.1
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

An unfiltered query with `limit` reads only the requested recent window. A
filtered query evaluates the predicate outside the history lock on a stable
snapshot. It searches newest to oldest when limited, but a rare match near the
oldest entry can still require scanning the full snapshot.
On `AsyncEventBus`, this filtered lookup runs synchronously on the owning event
loop. A rare match in a large history can therefore delay unrelated tasks;
bound the retained history and keep predicates inexpensive when replay latency
matters.

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
        max_concurrency=8,
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

Concurrent delivery is unbounded by default to preserve the existing behavior.
Set `max_concurrency=N` with `DeliveryMode.CONCURRENT` to cap callback fan-out
for each publish. When a callback finishes, its slot is reused immediately for
the next subscriber instead of waiting for the rest of a fixed group. Overlapping
publish calls each have their own limit; this does not bound how many publish
calls the application creates. Choose a finite value when one event's fan-out
could otherwise create too many tasks or use too many external resources.

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
history, error policy, delivery mode, `max_concurrency`, and async hooks.

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

<!-- integrated execution documentation -->
<a id="pipelines"></a>
## Pipelines and middleware

Use `Pipeline[T]` for a sequence of transformations from `T` to `T`. Every stage
receives the previous stage's result. The pipeline returns the final value and
does not publish it automatically. An empty pipeline returns its input.

~~~python
from dataclasses import dataclass, replace
from eventflow import Pipeline

@dataclass(frozen=True)
class Request:
    amount: int

pipeline = Pipeline[Request]()

@pipeline.stage("validate")
def validate(request: Request) -> Request:
    if request.amount < 0:
        raise ValueError("amount must be non-negative")
    return request

def double(request: Request) -> Request:
    return replace(request, amount=request.amount * 2)

pipeline.add("double", double, after="validate")

@pipeline.middleware("audit")
def audit(request, next_):
    print("before", request.amount)
    result = next_(request)
    print("after", result.amount)
    return result

print(pipeline.run(Request(4)))  # Request(amount=8)
~~~

`stage()` and `middleware()` register and return the original function. Its
identity, name and direct call behavior remain intact. Decorator registration
and `add()` use the same registry; neither wraps the registered function.

| Operation | Behavior |
| --- | --- |
| `add(name, callback, *, before=None, after=None)` | Add a stage with optional ordering constraints. |
| `stage(name, *, before=None, after=None)` | Decorator equivalent of `add`. |
| `remove(name)` | Remove the stage and references to it; return its callback. |
| `replace(name, callback)` | Keep the name, ordering constraints and enabled state. |
| `enable(name)` / `disable(name)` | Change inclusion in future runs. |
| `stage_names` | Ordered names, including disabled stages. |
| `add_middleware(...)` / `middleware(...)` | Register around middleware in its own namespace. |
| `remove_middleware`, `replace_middleware`, `enable_middleware`, `disable_middleware` | Apply the corresponding registry operation to middleware. |
| `middleware_names` | Names from outermost to innermost, including disabled middleware. |

Names are stripped, case-sensitive, non-empty and unique within each registry.
`before` and `after` must reference existing names. Unknown references raise
`KeyError`; duplicate names, empty names and ordering cycles raise `ValueError`.
Invalid mutations leave the existing plan intact. Registration order resolves
ties. Disabled stages retain their ordering constraints. Removing a stage
removes references to it from the remaining stages.

Ordering is prepared when configuration changes. Each run takes one immutable
execution snapshot; replacement or disabling during a run affects subsequent
runs. Configuration is thread-safe and callbacks execute outside its lock.
The snapshot does not copy payloads or make a callback's own mutable state safe
for concurrent use.

Middleware wraps the entire pipeline: the first registered middleware is the
outermost. It may call `next_(value)` once, transform its return value, catch a
`PipelineExecutionError` to recover, or return directly to short-circuit. A
continuation is valid only inside its middleware call and on the same thread
or async task. A second call, deferred call or detached-task call raises
`RuntimeError`. Existing bus actions and callback hooks retain their own
semantics; they are not executed as pipeline middleware.
Because middleware uses nested continuations, each pipeline accepts at most 64
registered middleware entries, including disabled entries. The limit is checked
when adding middleware and avoids reaching Python's recursion limit during a run.

Stage failures stop later stages and raise `PipelineExecutionError`. Inspect
`name`, `phase`, `context` and `__cause__` to locate the original failure.
The context is the value passed into the failing component. A synchronous
stage or middleware returning an awaitable is rejected, and returned coroutine
objects are closed. Python cannot forcibly stop a synchronous callback.

<a id="streams"></a>
## Async pipelines and streams

`AsyncPipeline[T]` has the same registry and decorators. Its callbacks return
awaitables; `await pipeline.run(value)` executes on the first owning loop.
Synchronous callbacks are never moved to threads. Concurrent runs use
independent snapshots and continuations. `CancelledError` propagates into the
active stage and is not converted into a delivery failure or retried.
Stage and middleware results must be resolved values: returning another
awaitable after the outer await (for example, forgetting `await next_`) is
rejected and any returned coroutine is closed.

`AsyncStream[T]` owns an `AsyncIterable[T]`. Its transformations accept ordinary
or awaitable callbacks, making both pipeline `run` methods directly usable as
map functions. Mapping, filtering and batching are lazy and start no workers.

~~~python
import asyncio
from eventflow import AsyncPipeline, AsyncStream

async def main() -> None:
    pipeline = AsyncPipeline[int]()

    @pipeline.stage("double")
    async def double(value: int) -> int:
        return value * 2

    async def source():
        for value in range(5):
            yield value

    stream = AsyncStream(source()).map(pipeline.run).filter(
        lambda value: value % 4 == 0
    ).buffer(2).batch(2)
    async with stream:
        async for batch in stream:
            print(batch)  # (0, 4), then (8,)

asyncio.run(main())
~~~

| Operation | Result and ownership |
| --- | --- |
| `AsyncStream(source)` | A single-use stream on one owning loop. |
| `map(callback)` | Transform values sequentially; await awaitable results. |
| `filter(predicate)` | Keep accepted values; synchronous or awaitable predicate. |
| `batch(size, *, partial=True)` | Yield tuples; optionally include the final partial tuple. |
| `buffer(capacity)` | Add one producer and a bounded queue with backpressure. |
| `aclose()` / `async with stream` | Close the entire upstream chain and await owned cleanup. |
| `closed` | Whether new reads have stopped. |

Batch sizes and buffer capacities must be positive integers; booleans, zero,
fractions and infinity are rejected. A full buffer blocks its producer. There
is room for `capacity` queued values plus one source value currently waiting
to enter the queue. Values are never dropped. Source failures reach the
consumer after earlier queued values; normal completion remains distinct from
failure and cancellation. Capacity counts objects, not bytes; a small number of
large payloads can still use substantial memory. Apply payload-size limits or
an application-owned byte budget when events have highly variable sizes.

A stream is consumed once. A second iterator acquisition and concurrent reads
are rejected. Use `async with` on the **terminal stream** when a consumer may
break early or raise. Python's `async for` protocol does not notify a source
when arbitrary consumer code uses `break`; `aclose()` supplies that explicit
ownership boundary. It also closes an unstarted upstream chain. Closing from
another task cancels an outstanding reader, then waits for source cleanup.
Cancellation of the caller is propagated after owned cleanup finishes, even
if cancellation is repeated. Sources and user cleanup still need to cooperate
with cancellation; a non-returning source can delay closure.

<a id="scopes"></a>
## Subscription scopes

Scopes group existing cancellation handles without changing what `subscribe`
returns. `add(handle)` returns the same handle and ignores repeated registration
of the same object. Separate scopes may own unrelated buses or callback policies.

~~~python
from eventflow import EventBus, SubscriptionScope

with EventBus[int]() as bus, SubscriptionScope() as scope:
    handle = scope.add(bus.subscribe(print))
    bus.publish(42)
assert not handle.active
~~~

`SubscriptionScope.close()` cancels synchronous handles in reverse registration
order. `AsyncSubscriptionScope.aclose()` awaits async handles in the same order
and supports `async with`. Both accept duck-typed handles with the corresponding
`cancel()` method. They attempt every handle, collect failures in the existing
Python 3.10-compatible `ExceptionGroup`, and retain failed handles for retry.
Handle implementations should make repeated cancellation safe. A handle's own
async cancellation is propagated after the other handles have been attempted.
Other handle-owned `BaseException` interrupts also retain their type after all
handles are attempted; additional failures are chained as an exception group.
Closed scopes reject additions. Async cleanup is shared by concurrent close
callers and finishes before caller cancellation is re-raised.

Scopes cancel registrations; they do not acquire ownership of the underlying
bus or forcibly stop callbacks already running. Keep bus/engine context managers
when those resources also need to close. The async scope itself belongs to one
loop and rejects reentrant cleanup that would await itself.

<a id="event-context"></a>
## Event context and measurements

`publish(event)` continues to accept raw objects. `EventEnvelope[T]` is an
optional frozen container with `event`, generated `id`, `correlation_id`,
`source`, timezone-aware UTC `timestamp` and `metadata`. Metadata is copied
into a read-only mapping; payloads and nested metadata values retain their
own mutability. `dataclasses.asdict()` and pickle work when the event and
metadata values themselves support those operations. Use `dataclasses.replace`
to transform an envelope's payload.

~~~python
from dataclasses import replace
from eventflow import EventEnvelope, Pipeline, PipelineStats, correlation_context

stats = PipelineStats()
pipeline = Pipeline[EventEnvelope[int]](observer=stats)
pipeline.add("increment", lambda envelope: replace(envelope, event=envelope.event + 1))
with correlation_context("request-42"):
    event = EventEnvelope(1, source="example", metadata={"attempt": 1})
    result = pipeline.run(event)
print(result.event, result.correlation_id)  # 2 request-42
print(stats.snapshot.runs.successful)     # 1
~~~

`correlation_context(id)` is a normal context manager, usable in async functions
too. `current_correlation_id()` reads the current identifier. An envelope uses
an explicit correlation ID, then the bound context, then its own ID. Context
is isolated across asyncio tasks and reset on exit. New threads and the bus
executor do not automatically inherit it; use `contextvars.copy_context` in
application code when propagation is required.

Pass `observer=PipelineStats()` or a synchronous `PipelineObserver` callable to
either pipeline. Each completed run, stage and middleware produces a
`PipelineObservation(kind, name, elapsed, status, correlation_id, error)`.
Statuses are `success`, `error` and `cancelled`; elapsed times are seconds.
Bind correlation around the caller's entire run for consistent run and stage
notifications. Middleware-local context applies only while that middleware is
active. With no observer, clocks and observation objects are omitted.

`stats.snapshot` returns immutable `runs`, `stages` and `middleware` aggregates.
Each `ExecutionMetrics` includes `calls`, `successful`, `failed`, `cancelled`,
`total_seconds` and `max_seconds`. Counts describe actual execution, so a
short-circuit does not count skipped stages. Run and middleware durations
include nested stages; do not add them together. `reset()` clears aggregates.
No payloads or traceback objects are retained by the collector. Metric keys are
stage and middleware names; stable names keep the collector small. Dynamically
generated names remain in the snapshot until `reset()` is called, so avoid
unbounded user or request values as component names.

Observers are synchronous and execute inline, including in `AsyncPipeline`;
they must be fast and non-blocking to avoid delaying the event loop. For logging
or telemetry I/O, enqueue a compact observation into an application-owned
bounded queue. An observer failure aborts an otherwise successful run with
`phase="observer"`. During an existing pipeline failure, observer errors are
available as `observer_errors` on the original `PipelineExecutionError`,
preserving its cause. Existing `EventBusStats` is unchanged.

### Security and operational boundaries

EventFlow invokes callbacks, predicates, middleware and stream sources in the
application process with that process's permissions. It does not sandbox
untrusted plugins; isolate such code with an operating-system process or
container boundary and explicit permissions and resource limits. Async
cancellation and callback timeouts are cooperative and do not provide a hard
deadline for code that suppresses cancellation or blocks the event loop.

`PipelineExecutionError.context` and observer errors may reference application
objects, including sensitive data. Avoid logging these objects wholesale;
redact fields in the application's structured logging layer.

<a id="composition"></a>
## Composing the components

Use the components independently or connect their normal callable/iterator
interfaces. A stage can be `CallbackSubscription.invoke`, a stream map can be
`Pipeline.run` or `AsyncPipeline.run`, and a scope can own the handle returned
by either bus. Bus publication does not implicitly invoke a pipeline or add
an envelope.

Two complete, network-free examples are executable from the repository root:

~~~bash
python -m examples.integrated
python -m examples.bus_stream
~~~

[integrated.py](https://github.com/TahsinCr/python-eventflow/blob/main/examples/integrated.py)
combines sync and async pipeline stages, decorators, callback policies,
correlation, metrics, a buffered stream and an async bus.
[bus_stream.py](https://github.com/TahsinCr/python-eventflow/blob/main/examples/bus_stream.py)
uses a bounded application queue as a bus subscription and stream source.
It owns one publisher task and joins it on exit; no task is created per event.

<a id="reference"></a>
## Lifecycle and reference

### Defaults

| Option | `EventBus` | `AsyncEventBus` |
| --- | --- | --- |
| `history` | `None` | `None` |
| `error_policy` | `ErrorPolicy.ISOLATE` | `ErrorPolicy.ISOLATE` |
| `executor` | `None` | Not used |
| `delivery_mode` | Not used | `DeliveryMode.SEQUENTIAL` |
| `max_concurrency` | Not used | `None` (unbounded) |
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
history `latest()`/`query()` calls through the history and bus APIs, along with
bounded sync and async replay setup. The standard-library benchmark defaults
to 100,000 retained events and can be adjusted with `--events`, `--replay`,
`--iterations`, and `--samples`.

Additional repeatable measurements use `python -m benchmarks.benchmark_publish`,
`python -m benchmarks.benchmark_pipeline` and `python -m benchmarks.benchmark_stream`.
Use `--output results.json` to retain the environment, throughput, p50/p95/p99
latency and separately traced peak allocations. Stream rates count items per
whole-stream operation. Traced allocation peaks are not process RSS and local
microbenchmarks are not deployment guarantees. Recorded baseline/final results
and methodology live under `benchmarks/results`.

Copyright © 2026 TahsinCr. EventFlow is licensed under the MIT License.
See [LICENSE](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE) and
[CHANGELOG](https://github.com/TahsinCr/python-eventflow/blob/main/CHANGELOG.md).
