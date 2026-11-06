[![Version: v1.3.1][version-shield]](https://github.com/TahsinCr/python-eventflow/blob/main/CHANGELOG.md)
[![Python 3.10+][python-shield]](https://www.python.org/downloads/)
[![MIT License][license-shield]](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE)

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
- [Application coordination](#application-coordination)
  - [Event bridges and queues](#coordination-bridges)
  - [Task and resource ownership](#coordination-lifecycle)
  - [Concurrency and duplicate work](#coordination-concurrency)
  - [Retries, cooldowns and deadlines](#coordination-resilience)
  - [Scheduling and bounded work](#coordination-scheduling)
  - [State, cache, services and policy](#coordination-state)
  - [Operation metrics](#coordination-metrics)
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

After the PyPI release is published, install EventFlow with:

~~~bash
python -m pip install flowevent
~~~

`flowevent` is the PyPI distribution name. The library is called EventFlow,
and Python code continues to import the `eventflow` package:

~~~python
from eventflow import EventBus
~~~

Python 3.10 installs `backports.strenum` and `exceptiongroup` to supply features
that are built into Python 3.11 and newer. Package metadata selects these
dependencies only on Python 3.10.

The distribution includes inline type annotations and a `py.typed` marker, so
type checkers can discover EventFlow's types when the package is installed.

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

print(__version__)  # 1.3.1
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
thread that called `publish()`. An optional `concurrent.futures.ThreadPoolExecutor`
can run handlers on worker threads, but the publishing call still waits for them.
When buses share a pool, a callback that publishes through another bus using
that same pool delivers the nested event inline on the active worker. This
prevents worker starvation during cross-bus reentrant publication.

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

These counters are not a per-publish invariant. `published` advances after
history recording succeeds (or is skipped when history is disabled) and before
callbacks run; `delivered` and `failed` are finalized afterward. An in-flight
or cancelled async publish can leave them different. `failed` counts collected
errors from handlers, filters, and actions, not only failed handlers.

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
`append()`, `latest()`, `query()`, `clear()`, and `__len__()`. Implementations
must synchronize all five operations and return `latest()`/`query()` results
from a consistent snapshot. A history shared by multiple buses must synchronize
across those buses too. The bus publication lock serializes `append()` only for
that bus; history queries run outside it. Keep `append()` fast and non-blocking,
and do not publish recursively through the same bus from it.

Bus `clear()` removes active subscriptions and wakes pending waiters; it does
not erase history or stop periodic schedules. Call the returned schedule's
`cancel()` to stop it, or close the bus to stop all schedules. To delete retained
events, call `clear()` on the configured history object.

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
either mode and cancels in-flight handler work. Cancellation cannot undo side
effects a handler produced before it was cancelled.

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
This still does not bound the number of waiting publisher tasks. For a
high-volume or untrusted input source, apply backpressure before creating those
tasks, for example with a bounded application queue and a fixed worker set.

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

Channel names must be strings and are stripped and lowercased, so `" Orders "`
and `"orders"` refer to the same channel. `engine.channel(name)` returns a
channel, creating it when necessary. Use `engine.add(name, bus)` to register a
bus with custom settings. `remove(name)` closes one channel; `stop()` closes
every channel.
An engine can own a bus instance under only one channel name at a time. A
successful `add()` claims that bus for the engine; another engine cannot claim
the same instance and receives `ValueError`. Removing or stopping the owner
closes the bus.
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
carries history, error policy, executor, and hooks; its optional executor must
be a `ThreadPoolExecutor`. `AsyncEventEngine` carries history, error policy,
delivery mode, `max_concurrency`, and async hooks.

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

    stream = (
        AsyncStream(source())
        .map(pipeline.run)
        .filter(lambda value: value % 4 == 0)
        .buffer(2)
        .batch(2)
    )
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
A callback running in the active read task cannot await that same terminal
stream's `aclose()`; it raises `RuntimeError` to prevent a cleanup deadlock.
The read's normal error path then closes the stream and its upstream chain.

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
An async handle's `cancel()` must return an awaitable. The scope checks this
when closing because `add()` cannot validate the result without cancelling the
handle.
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
`source`, UTC `timestamp` and `metadata`. A timezone-aware timestamp supplied
in another zone is normalized to UTC. Metadata is copied into a read-only
mapping, including after `copy.deepcopy()`; payloads and nested metadata values
retain their own mutability. `dataclasses.asdict()` and pickle work when the
event and metadata values themselves support those operations. Only unpickle
data from trusted sources: Python pickle can execute code during loading. Use
`dataclasses.replace` to transform an envelope's payload.

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
print(stats.snapshot.runs.successful)  # 1
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

<a id="application-coordination"></a>
## Application coordination

EventFlow also provides small execution and coordination primitives for
application code. Each is opt-in and has an explicit owner and shutdown
method. They use ordinary Python callables, awaitables, futures, context
managers and iterators, so they can be composed with buses, pipelines and
streams without changing event delivery or stage ordering.

| Component | Module | Purpose |
| --- | --- | --- |
| `AsyncEventQueue`, `LoopPortal` | `eventflow.bridges` | Bridge synchronous bus delivery and external threads to an owned async loop. |
| `AsyncTaskScope` | `eventflow.tasks` | Own background asyncio tasks. |
| `ResourceScope`, `AsyncResourceScope` | `eventflow.scopes` | Run reverse-order, retryable resource cleanup. |
| `KeyedConcurrencyLimiter`, `KeyedRateLimiter`, `SingleFlight` | `eventflow.concurrency` | Bound keyed work, apply a token bucket, or share an in-flight operation. |
| `LatestTaskCoordinator` | `eventflow.tasks` | Keep only the newest result for each key. |
| `AsyncStream.batch_within()` | `eventflow.stream` | Emit a batch when its size or time window is reached. |
| `BoundedWorkPool` | `eventflow.executors` | Bound synchronous work submitted to a thread pool. |
| `KeyedCoalescer`, `AsyncScheduler` | `eventflow.scheduling` | Merge keyed updates and own scheduled async jobs. |
| `RetryPolicy`, `CooldownRegistry`, `CircuitBreaker` | `eventflow.resilience` | Coordinate retries and temporary failure handling. |
| `Deadline` | `eventflow.timing` | Share a monotonic time budget across nested operations. |
| `StateMachine`, `VersionedValue` | `eventflow.state` | Guard local state transitions and versioned values. |
| `VersionedCache` | `eventflow.cache` | Bound an in-memory LRU cache with versions and optional TTL. |
| `ServiceRegistry` | `eventflow.registry` | Register owner-managed services with cleanup. |
| `PolicyEvaluator` | `eventflow.policy` | Evaluate ordered synchronous rules. |
| `OperationStats` | `eventflow.observability` | Collect bounded-cardinality aggregates without retaining payloads. |

These utilities are in-process building blocks. They do not persist state or
coordinate independent processes. Import a component from `eventflow` for the
lazy top-level API or from its module when a narrower import is preferred.

<a id="coordination-bridges"></a>
### Event bridges and queues

`AsyncEventQueue` subscribes to a synchronous `EventBus` and exposes received
events through `receive()` or async iteration. It has a fixed item capacity,
accepts publishes from other threads, and supports `drop_oldest`,
`drop_newest` and `error` overflow policies. The configured capacity includes
events waiting to be delivered onto the owning loop. It allows one active
consumer task at a time; another task may take over after the current consumer
has finished. The queue remains bound to its original event loop. Closing the
queue removes its subscription, allows already queued items to drain, then
makes the next read raise `EventQueueClosedError`. If that loop has already
closed, `aclose()` can still remove the synchronous bus subscription from a
different running loop. `stats()` reports queue size, deliveries,
drops and overflow errors. An `error` overflow raises `QueueOverflowError`
inside the synchronous subscriber; the bus applies its configured
`ErrorPolicy` to that callback failure.

~~~python
import asyncio
from eventflow import AsyncEventQueue, EventBus

bus = EventBus[int]()


async def receive_one():
    async with AsyncEventQueue(bus, event_type=int, capacity=64, overflow="drop_oldest") as events:
        await asyncio.to_thread(bus.publish, 42)
        return await events.receive()


assert asyncio.run(receive_one()) == 42
bus.close()
~~~

`LoopPortal` owns a dedicated event-loop thread. Submit an awaitable to get a
`concurrent.futures.Future`, or use `call()` from another thread when the
caller needs a synchronous result. It copies the current `contextvars` context
at submission time. `close()` rejects new work, cancels and waits for portal
tasks, shuts down its default executor, and joins its thread. A blocking
`call()` from the portal's own loop is rejected to prevent deadlock. The
portal does not take ownership of an application's existing event loop. A
`call(timeout=...)` timeout requests
cancellation; it cannot force a cancellation-resistant operation to stop.
An operation's own `TimeoutError` is propagated unchanged; the portal timeout
is raised only when `call()`'s wait deadline expires.
That operation remains owned by the portal until it actually finishes, and
`close()` can time out while waiting for it. Work already running through
`asyncio.to_thread()` cannot be stopped by task cancellation. A timed-out
`close()` retains the portal loop thread while that worker finishes; call
`close()` again to complete shutdown.

~~~python
from eventflow import LoopPortal


async def load_value():
    return 42


with LoopPortal() as portal:
    future = portal.submit(load_value)
    assert future.result(timeout=2) == 42
~~~

<a id="coordination-lifecycle"></a>
### Task and resource ownership

`AsyncTaskScope` owns tasks created with `start()`. Each task uses
`shutdown="cancel"` or `shutdown="drain"`; closing the scope applies that
task's policy, awaits cleanup, and rejects later starts. `TaskHandle.join()`
waits for that task and returns its result. `snapshot()` reports active,
completed, failed and cancelled work. A synchronous `on_error` callback
observes failures not retrieved by a join. If `aclose(timeout=...)` times out, unfinished tasks
remain owned and a later close can retry; a timeout never makes a still-running
task disappear.

~~~python
from eventflow import AsyncTaskScope


async def refresh_cache():
    pass


def report_task_error(_handle, _error):
    pass


async with AsyncTaskScope(on_error=report_task_error) as tasks:
    refresh = tasks.start(refresh_cache, name="cache-refresh", shutdown="cancel")
    await refresh.join()
~~~

`ResourceScope` and `AsyncResourceScope` own arbitrary cleanup operations.
`add(resource, close=..., owned=True)` registers a method name or cleanup
callable; `defer(cleanup)` registers a no-argument callback. Cleanup runs in
reverse registration order, attempts every entry, and retains failures for a
later retry. Use `owned=False` for a borrowed resource, or `detach(resource)`
to transfer ownership out of the scope. `pending_cleanup()` reports entries
still requiring cleanup. The async version awaits async closers and supports
`async with`; cleanup is shared across concurrent close callers. A synchronous
cleanup that reenters its own `ResourceScope.close()` gets a no-op from the
nested call; the outer close retains responsibility for the pending count and
cleanup results.

~~~python
from eventflow import ResourceScope


class Resource:
    def close(self):
        pass


def flush_metrics():
    pass


with ResourceScope() as resources:
    resources.add(Resource())
    resources.defer(flush_metrics)
~~~

<a id="coordination-concurrency"></a>
### Concurrency and duplicate work

`KeyedConcurrencyLimiter(max_concurrency)` keeps a separate active-work limit
for every key. Use `async with limiter.acquire(key)` for cancellation-safe
release, or `acquire_nowait(key)` when the caller can handle immediate
rejection. Idle keys are removed. `aclose()` rejects new acquisitions and
wakes waiters. Queued acquisitions run in FIFO order, and `acquire_nowait(key)`
returns `False` while a waiter is queued so it cannot bypass earlier work. This
is a concurrency cap; it does not limit requests per second. If
`acquire_nowait(key)` succeeds, release the slot with `await limiter.release(key)`.
`stats()` reports aggregate counts; `key_stats(key)` reports active permits and
waiters for one key.

`KeyedRateLimiter(rate, period, burst=...)` is a per-key token bucket.
`await acquire(key)` waits cancellably for a token; `try_acquire(key)` returns
immediately. While an `acquire(key)` is waiting, `try_acquire(key)` for that key
returns `False` so new immediate calls cannot consume its next token. Waiting
`acquire()` calls are not guaranteed strict FIFO order. `max_keys` bounds
retained keys and `prune()` removes idle state; if the key table is full and no
idle bucket can be pruned, a new key is rejected. It has loop ownership and
should be closed with `aclose()`.

`SingleFlight.run(key, factory)` shares one in-flight awaitable among callers
with the same key. Different keys run independently, one waiter's cancellation
does not cancel shared work, and current waiters receive the same result or
failure. The result is not cached after completion. Shared work continues if
all waiters leave; it remains owned and visible through `stats().active` until
completion. Call `await flight.aclose()` when the instance is no longer needed;
it cancels and drains active work, then rejects new calls. Factories must
cooperate with cancellation for shutdown to finish. If the task awaiting
`SingleFlight.aclose()` is cancelled, cancellation propagates while the
shielded shutdown task remains owned; a later `aclose()` can await it. In
contrast, `AsyncTaskScope` and `AsyncResourceScope` finish cleanup before
re-raising caller cancellation. A factory cannot close its own `SingleFlight`
instance; that call raises `RuntimeError`. `stats()` must be called from the
instance's owning event loop.
`LatestTaskCoordinator.submit(key, factory, on_result)` cancels prior work by
default and applies a result only if that submission is still the newest for
the key. Generation checks also prevent a cancellation-resistant old task
from publishing a stale result. `on_result` runs on the owning loop and may be
synchronous or awaitable. Close the coordinator with `aclose()`.

~~~python
from eventflow import KeyedConcurrencyLimiter, SingleFlight

limiter = KeyedConcurrencyLimiter[str](max_concurrency=2)
flight = SingleFlight[str]()


async def fetch_account(account_id):
    return {"id": account_id}


async def load_for(account_id):
    async with limiter.acquire(account_id):
        return await flight.run(account_id, lambda: fetch_account(account_id))


await limiter.aclose()
~~~

<a id="coordination-resilience"></a>
### Retries, cooldowns and deadlines

`RetryPolicy` offers synchronous `run()` and asynchronous `run_async()` with
fixed or exponential backoff, a delay ceiling, optional jitter and an
optional synchronous observation callback. Retries are explicit: without
`should_retry`, a failure is raised after the first attempt even when
`max_attempts` is larger. The synchronous predicate receives ordinary
`Exception` instances and must return `bool`; cancellation is propagated and
is never retried. A `Deadline` is a retry budget: it prevents attempts from
starting after expiry and caps backoff delays. It does not interrupt an
operation already in progress, and a successful operation may return after the
deadline. Wrap an operation in a separate cancellation-aware timeout when its
execution itself needs a hard time limit.

`Deadline.after(seconds)` uses a monotonic clock and exposes `remaining` and
`expired`. `child(seconds)` creates a shorter budget that cannot outlive its
parent. Passing `None` represents no deadline. The object communicates a time
budget; it cannot forcibly stop synchronous code or a coroutine that suppresses
cancellation.

`CooldownRegistry` stores bounded per-key monotonic expiry times. `block()`
sets or extends a cooldown, `remaining()` reports its duration, and `wait()` /
`wait_async()` can wait until it expires or is cleared. `CircuitBreaker` wraps
synchronous `call()` or asynchronous `call_async()` operations and moves through
`closed`, `open` and `half_open` states.
It limits concurrent half-open probes and ignores cancellation as a counted
failure. These primitives coordinate one process; they do not share state
across workers or machines.

~~~python
from eventflow import Deadline, RetryPolicy


class TemporaryError(Exception):
    pass


async def fetch_remote():
    return "response"


deadline = Deadline.after(5)
retry = RetryPolicy(max_attempts=3, initial_delay=0.05, jitter=0.1)
result = await retry.run_async(
    fetch_remote,
    should_retry=lambda error: isinstance(error, TemporaryError),
    deadline=deadline,
)
~~~

<a id="coordination-scheduling"></a>
### Scheduling and bounded work

`AsyncScheduler` owns one-shot and periodic asyncio jobs. Periodic jobs choose
`overlap="skip"`, `"queue"` or bounded `"parallel"` execution; the parallel
limit is set with `max_parallel`. Queue mode retains at most one missed tick
while a callback runs. Retain the returned `ScheduledHandle` when individual
cancellation or joining is needed. `aclose()` stops new schedules,
cancels active jobs and waits for their cleanup.

`BoundedWorkPool` runs synchronous callables in a thread pool with an explicit
accepted-work bound:
`max_workers` controls concurrent threads and `max_pending` controls queued
work beyond those workers. `submit()` returns a standard future and rejects
immediately when capacity is full. Close the pool with `close()` or its
context manager; cancellation cannot stop a function that is already running.

`KeyedCoalescer` merges repeated values by a caller-supplied key and merge
function, then delivers them from one owned worker at the configured interval.
The first value for a key starts that key's interval. `max_pending_keys` bounds
retained keys. Its worker starts on the first accepted `push()`, so an unused
instance creates no background thread. The key function runs on the submitting
thread before locking. The merge function also runs there, under the
coalescer's condition lock, to preserve atomic updates for a key. Keep both
functions fast and non-reentrant. Delivery runs on the worker. `flush()`
delivers pending values immediately; `close(flush=True)` flushes before
stopping, while `flush=False` discards pending values. Callback failures are
reported through `on_error` and `stats()`. Concurrent flush calls retain
independent requests, so one caller's timeout cannot cancel another's flush.
The first `close()` call chooses the flush/discard policy; later calls can retry
waiting for shutdown but do not change that choice. If `close(flush=False)`
discards work during an active flush, that flush raises `RuntimeError`.

~~~python
from eventflow import BoundedWorkPool


def parse_payload(payload):
    return payload


raw_payload = b"example"
with BoundedWorkPool(max_workers=4, max_pending=8) as workers:
    result = workers.submit(parse_payload, raw_payload).result(timeout=2)
~~~

`AsyncStream.batch_within(max_items, max_delay, partial=True)` emits when the
first limit is reached. Its timer begins with the first item in a batch; it
does not create one task per item. The final partial batch is emitted when
`partial=True` and discarded when false. It retains the same single-consumer,
event-loop ownership and explicit cleanup rules as other stream operations.

~~~python
from eventflow import AsyncStream


async def source():
    for item in range(3):
        yield item


async def store_batch(batch):
    print(batch)


async with AsyncStream(source()).batch_within(100, 0.05) as batches:
    async for batch in batches:
        await store_batch(batch)
~~~

<a id="coordination-state"></a>
### State, cache, services and policy

`StateMachine(initial, transitions, terminal=..., guard=...)` validates
synchronous state transitions. `transition()` changes state and increments
its version atomically; `compare_and_transition(expected_version, target)`
returns `None` when another caller already changed the version. Observers run
after the state update and outside the lock. `VersionedValue` offers the same
snapshot/version and compare-and-set pattern for an arbitrary value. Its guard
runs outside the lock and can be called again after a transition race, so it
should be synchronous and free of side effects. A `VersionedValue` snapshot
holds the value reference and does not copy mutable objects.

`VersionedCache` is a bounded in-memory LRU with optional TTL and byte budget.
Provide `getsizeof(key, value)` when `max_bytes` is set. `put()` requires a
version and rejects an older write while a newer version for that key remains
resident. Expiry, eviction and removal also discard the version history, so an
older write can be accepted after the entry leaves the cache. Use
`VersionedValue` or durable state when versions must remain monotonic beyond
cache residency. The cache is process-local and does not provide persistence or
cross-process invalidation.

`ServiceRegistry` maps trimmed, non-empty names to explicitly registered
services. Its cleanup callbacks must be synchronous. Every registration has an owner; `close_owner(owner)` closes that owner's
resources in reverse registration order and retains failed cleanup for retry.
An optional synchronous observer receives immutable registration and removal
changes after mutation and outside the registry lock. Observer errors do not
undo completed changes. The registry does not construct services or resolve
dependency graphs.

`PolicyEvaluator` evaluates named synchronous rules in registration order.
A rule returns `"allow"`, `"deny"`, `"abstain"`, `True`, `False` or `None`;
an explicit deny takes priority, then an allow applies, followed by the
configured default (deny by default).
Its `PolicyDecision` carries the decision and matching rule details. It is a
small in-process rule combiner, not an identity provider, policy language,
authorization boundary, or sandbox. Enforce decisions at the protected
operation and isolate untrusted code with operating-system permissions.

~~~python
from eventflow import StateMachine, VersionedValue

machine = StateMachine("queued", {"queued": {"running", "cancelled"}})
running = machine.transition("running")
value = VersionedValue("initial")
value.compare_and_set(value.snapshot().version, "ready")
~~~

<a id="coordination-metrics"></a>
### Operation metrics

`OperationStats` is a thread-safe aggregate sink for calls, outcomes, elapsed
time, queue depth, drops, retries, rejections, timeouts and shutdown time.
Pass one instance to components that accept `observer=` to collect their
measurements in the same immutable snapshot. `max_operations` bounds label
cardinality; names beyond the limit share an `other` bucket. `reset()` clears
the aggregates. Metrics retain counters only, not event payloads, callback
results or exception objects. Keep operation names stable, and send telemetry
outside the hot path through an application-owned bounded queue.

~~~python
from eventflow import KeyedConcurrencyLimiter, OperationStats


async def update_account():
    pass


metrics = OperationStats(max_operations=64)
limiter = KeyedConcurrencyLimiter[str](1, observer=metrics)
async with limiter.acquire("account-42"):
    await update_account()
print(metrics.snapshot().operations["keyed_concurrency.acquire"].successful)
await limiter.aclose()
~~~

### Security and operational boundaries

EventFlow invokes callbacks, predicates, middleware and stream sources in the
application process with that process's permissions. It does not sandbox
untrusted plugins; isolate such code with an operating-system process or
container boundary and explicit permissions and resource limits. Async
cancellation and callback timeouts are cooperative and do not provide a hard
deadline for code that suppresses cancellation or blocks the event loop.
The bus `on_before` hook is not an authorization or validation gate: its errors
are recorded, and even `ErrorPolicy.RAISE` is applied after selected subscribers
run. Check permission before calling `publish()` or at the protected operation.

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

Three complete, network-free examples are executable from the repository root:

~~~bash
python -m examples.integrated
python -m examples.bus_stream
python -m examples.coordination
~~~

[integrated.py](https://github.com/TahsinCr/python-eventflow/blob/main/examples/integrated.py)
combines sync and async pipeline stages, decorators, callback policies,
correlation, metrics, a buffered stream and an async bus.
[bus_stream.py](https://github.com/TahsinCr/python-eventflow/blob/main/examples/bus_stream.py)
uses a bounded application queue as a bus subscription and stream source.
It owns one publisher task and joins it on exit; no task is created per event.
[coordination.py](https://github.com/TahsinCr/python-eventflow/blob/main/examples/coordination.py)
connects a synchronous bus, `AsyncEventQueue`, `AsyncTaskScope`, a keyed
concurrency limit, explicit retries, a deadline and shared operation metrics.

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
`EventBusClosedError`. `clear()` removes subscriptions, leaves history intact,
does not stop periodic schedules, and completes pending `wait_for()` calls with
`None` on both bus types. A synchronous wait also returns `None` when the bus
closes.

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
| `clear()` / `close()` | Clear subscriptions (schedules continue) or close the bus (schedules stop). Both are awaited on `AsyncEventBus`. |

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
| Coordination and lifecycle | `AsyncEventQueue`, `AsyncTaskScope`, `ResourceScope`, `AsyncResourceScope`, `LoopPortal`, `TaskHandle` |
| Concurrency and scheduling | `KeyedConcurrencyLimiter`, `KeyedConcurrencyKeyStats`, `KeyedRateLimiter`, `SingleFlight`, `LatestTaskCoordinator`, `AsyncScheduler`, `KeyedCoalescer`, `BoundedWorkPool` |
| State and resilience | `StateMachine`, `VersionedValue`, `VersionedCache`, `ServiceRegistry`, `RegistryChange`, `PolicyEvaluator`, `RetryPolicy`, `CooldownRegistry`, `CircuitBreaker`, `Deadline` |
| Stream and metrics | `AsyncStream`, `OperationStats`, `OperationMetrics`, `PipelineStats`, `EventEnvelope` |
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
| `EventQueueClosedError` | A read is attempted after a closed queue has drained. |
| `QueueOverflowError` | An `AsyncEventQueue` configured with `overflow="error"` is full. |
| `LoopPortalClosedError` | Work is submitted after a portal has closed. |
| `CircuitOpenError` | A circuit is open or its half-open probe limit has been reached. |
| `StateTransitionError` | A state transition is not allowed by the configured graph. |

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
`python -m benchmarks.benchmark_pipeline`, `python -m benchmarks.benchmark_stream`
and `python -m benchmarks.benchmark_runtime`. The stream benchmark also compares
burst-input `batch_within(64, 60)` against native size-only batching; this
isolates batch coordination overhead and does not measure sparse-source timer
latency. The runtime-load benchmark compares
sync and async publisher contention, async `max_concurrency` values, callback
error rates, periodic schedule resources, and traced memory over a configurable
event soak. It defaults to five samples, 500 publishes per publisher in each
sample, and 100,000 soak events; use `--iterations`, `--publisher-counts`,
`--schedule-counts`, `--samples` or `--soak-operations` to adjust the workload.
Use `--output results.json` to retain the environment, throughput, p50/p95/p99
latency and separately traced peak allocations. Stream rates count items per
whole-stream operation. Runtime memory samples use `tracemalloc`, not process
RSS, and include event history when configured. Local microbenchmarks are not
deployment guarantees. Recorded results and methodology live under
`benchmarks/results`.

Copyright © 2026 TahsinCr. EventFlow is licensed under the MIT License.
See [LICENSE](https://github.com/TahsinCr/python-eventflow/blob/main/LICENSE) and
[CHANGELOG](https://github.com/TahsinCr/python-eventflow/blob/main/CHANGELOG.md).

[version-shield]: https://img.shields.io/badge/version-v1.3.1-b7a3e3.svg?style=for-the-badge
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
