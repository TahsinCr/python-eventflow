# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Changed

- Prevent `KeyedRateLimiter.try_acquire()` from consuming a token while an
  asynchronous acquisition for the same key is waiting; waiting acquisitions
  still have no strict FIFO guarantee.

### Fixed

- Ignore circuit-breaker outcomes from earlier generations after the circuit
  opens, recovers, or is reset.
- Restore a callback subscription's final-call quota when automatic unsubscribe
  fails, allowing a later delivery to retry cancellation.
- Keep concurrent coalescer flush requests independent and make the first close
  call's flush/discard policy stable across shutdown retries.

## [v1.3.1] - 2026-11-04

### Changed

- Skip empty error-notification work on successful sync and async publications.
- Bypass disabled pipeline observers on successful runs and stages.
- Skip retry timing and observation allocation when no retry observer is set.
- Skip expiry-heap processing when a cache has no expiring entries.
- Reuse immutable policy-rule snapshots between evaluations and return the
  default decision directly when no rules are registered.
- Reduce per-submission allocation in `LoopPortal` while keeping task
  cancellation routed through its owning event loop.
- Reduce per-submission state allocation in `BoundedWorkPool` while preserving
  pending and running task accounting.
- Paired local microbenchmarks measured lower median latency for synchronous
  publish (3.3–6.0%), sequential async publish (5.4–10.0%), five-stage sync and
  async pipelines (5.2–11.3% and 7.4–7.7%), one-attempt sync and async retries
  without observers (45.6–52.4% and 52.0–60.2%), non-expiring cache hits
  (16.9–24.3%), policy evaluation (14.8–17.0% with no rules; 12.0% with four
  rules), and `LoopPortal.call()` (5.2–7.0%). `BoundedWorkPool` submit latency
  varied by interpreter, so its entry records the allocation reduction without
  claiming a speedup. Workloads and measurements are documented in the
  [benchmark results](benchmarks/results/README.md#unreleased-micro-optimizations-2026-11-04).

### Fixed

- Preserve explicitly supplied falsey filters, action configurations, cache
  size callbacks and circuit-breaker classifiers instead of silently using
  fallback defaults.
- Preserve populated falsey policy contexts so explicit deny rules still see
  their authorization attributes.
- Reject policy defaults and rule outcomes that only compare equal to valid
  decisions, keeping authorization decisions fail-closed.
- Close unstarted coroutines when task creation is rejected across timed
  callbacks, `SingleFlight`, managed cleanup, stream producers, scheduled jobs
  and async bus delivery.
- Preserve the current keyed task when replacement task creation fails, and
  clean up eagerly completed tasks without leaving stale coordinator state.

## [v1.3.0] - 2026-11-02

### Added

- Add a bounded, thread-safe `AsyncEventQueue` bridge for synchronous bus
  events and an owned-thread `LoopPortal` for coroutine submission.
- Add `AsyncTaskScope`, `ResourceScope` and `AsyncResourceScope` for explicit,
  cancellation-aware task and resource ownership.
- Add keyed concurrency and token-bucket rate limiters, `SingleFlight`, and
  latest-result task coordination.
- Add `AsyncStream.batch_within()` for size- or time-triggered batching.
- Add `BoundedWorkPool`, `KeyedCoalescer` and `AsyncScheduler` with explicit
  capacity, overlap and shutdown behavior.
- Add sync/async `RetryPolicy`, `CooldownRegistry`, `CircuitBreaker` and
  monotonic `Deadline` primitives.
- Add thread-safe `StateMachine` and `VersionedValue`, bounded
  `VersionedCache`, owner-aware `ServiceRegistry` and synchronous
  `PolicyEvaluator`.
- Extend observability with bounded-cardinality `OperationStats` aggregates
  and optional instrumentation on coordination components.
- Expose per-key active/waiting counts for keyed concurrency and immutable
  registration/removal observations from `ServiceRegistry`.
- Add `SingleFlight.aclose()` to cancel and drain shared operations owned by an
  instance.
- Document and test the new public APIs, their ownership boundaries and their
  composition with the existing buses, pipelines and streams.

### Changed

- Serve queued keyed-concurrency acquisitions in FIFO order and reject
  `acquire_nowait()` while queued work exists; keep observer queue-depth updates
  independent of the number of active keys.
- Require `SingleFlight.stats()` calls to run on the instance's owning event loop.
- Make identity-based membership checks in scopes and engine channels average
  constant-time, avoiding quadratic growth when registering large batches.
- Skip dispatch bookkeeping for publications without subscribers or before/after
  hooks while preserving history and publication statistics.
- Transfer timed-stream batches between producer and consumer in one bounded
  handoff, using one deadline timer per batch instead of per-item timers.
- Fuse chained `AsyncStream.map()` and `filter()` callbacks into one lazy
  iterator stage, avoiding an intermediate stream read for each operation.
- Reuse subscriber-matching error storage and check the synchronous periodic
  stop signal directly in its publisher loop.
- Start a `KeyedCoalescer` worker only after the first accepted update, avoiding
  idle threads for unused instances.
- Allow an `AsyncEventQueue` consumer to take over after its previous consumer
  task finishes.
- Clarify retry-budget and cache-version retention semantics in both READMEs.

### Fixed

- Preserve operation-raised `TimeoutError` in `LoopPortal.call()` and distinguish
  it from an actual wait timeout.
- Reject `LoopPortal.start()` and `submit()` once loop stopping begins, including
  after a timed-out join that leaves the loop owned for retry.
- Finish keyed-limiter waiter cleanup before propagating cancellation from
  `aclose()`, without cancelling the waiters themselves.
- Close scheduled coroutines when `AsyncScheduler` task creation fails.
- Ensure `LoopPortal.start()` returns only after its event loop is running, so
  immediate submissions cannot race loop startup.
- Preserve shared keyed-limiter wakeups when one waiter is cancelled, serialize
  concurrent portal startup/shutdown, and close unstarted coroutine inputs.
- Keep portal loop ownership intact when a timed-out shutdown coordinator is
  cancelled, and allow shutdown to retry after owned tasks finish.
- Retain failed service cleanups for retry and report cleanup failures together.
- Allow timed-out task-scope and scheduler shutdowns to retry after their work
  completes, even when the original shutdown attempt has already failed.
- Expire cache entries through a bounded stale-record heap instead of scanning
  the full cache on every operation, including when TTL is disabled.
- Reject awaitables passed to the synchronous circuit-breaker API.
- Label grouped cleanup failures from resource scopes as resource cleanup errors.
- Restore final-delivery claims when an accepted executor future is cancelled
  before its callback starts.
- Keep `ResourceScope.pending_cleanup()` consistent during reentrant cleanup.
- Shut down a `LoopPortal` loop's default executor and allow queue subscription
  cleanup after its owning event loop has already closed.
- Reject closing a `SingleFlight` instance from one of its own active operations.

## [v1.2.1] - 2026-11-02

### Changed

- Isolate sync and async subscriber matching in focused bus helpers while
  keeping their cancellation and replay behavior separate.
- Share finite timeout validation and replay-history window selection across
  both buses without merging their independent delivery paths.
- Count a publication only after a configured history append succeeds, and
  reject non-string engine channel names instead of coercing them.
- Skip repeated event-loop lookups on stream reads after the owning consumer
  task has been established.
- Run Twine metadata checks in strict mode in CI.

### Fixed

- Claim engine-created channels for their owning engine, preserve finite
  subscription deliveries when cancellation or executor submission fails, and
  coordinate concurrent synchronous subscription cancellation retries.

## [v1.2] - 2026-11-02

### Added

- Add independent `Pipeline[T]` and `AsyncPipeline[T]` execution with named
  stages, identity-preserving decorators, stable before/after ordering,
  replacement, enable/disable controls, and immutable execution snapshots.
- Add ordered pipeline middleware with around execution, explicit
  short-circuit/recovery, and single-use, caller-owned continuations.
- Add `PipelineExecutionError` with the failing component, input context,
  original cause, and separately retained observer failures.
- Add single-consumer `AsyncStream` with lazy sync/async map and filter,
  tuple batching, bounded prefetch, backpressure, and cancellation-safe closure.
- Add `SubscriptionScope` and `AsyncSubscriptionScope` for existing cancellation
  handles, reverse-order cleanup, grouped failures, and retryable shutdown.
- Add optional immutable `EventEnvelope`, scoped correlation identifiers,
  `PipelineObservation`, and thread-safe aggregate `PipelineStats`.
- Add v1.1 public signature/decorator regression contracts, executable README
  checks, composed bus/pipeline/stream examples, and latency/allocation benchmarks.
- Report branch coverage alongside line coverage in the CI test matrix.
- Add an equivalent native async map/filter case to the stream benchmark so its
  API overhead can be compared separately from raw source iteration.
- Add repeatable publisher-contention, `max_concurrency`, callback-error,
  middleware, periodic-schedule resource, and 100,000-event memory-soak benchmarks.
- Add an opt-in per-publish async callback concurrency limit and expose it
  through async engine-created channels.
- Add sparse filtered-history measurements to the replay benchmark and a
  CodeQL security-analysis workflow with weekly dependency update checks.
- Document shared contracts, snapshot ownership, middleware ordering, stream
  backpressure, context propagation, and intentional architectural limits.

### Changed

- Refill bounded async delivery slots as callbacks finish instead of waiting
  for every callback in a fixed batch.
- Avoid an extra full result-list copy when bounded async delivery completes.
- Clarify that another distribution installing the shared `eventflow` import
  package in the same environment can overwrite or conflict with package files.
- Document that sparse filtered replay scans history synchronously on the async
  event loop and may delay unrelated tasks.
- Use `flowevent` as the PyPI distribution name while keeping EventFlow as the
  library name and `eventflow` as the Python import package.
- Bound pipeline middleware registrations at 64 to fail before exhausting the
  Python call stack, and update `PipelineStats` in place rather than allocating
  a new metrics snapshot for every observation.
- Load new execution components lazily through the package exports so ordinary
  bus imports and publication do not initialize optional execution machinery.
- Keep the PEP 561 marker in built distributions and verify it in installed-
  artifact CI.
- Extend CI typing to the runnable examples and benchmarks, require at least
  90% pipeline line coverage, and smoke-test new APIs from installed artifacts.
  The existing 88% package coverage floor and Python/platform matrix remain.
- Set separate branch-coverage floors for callback, callback helpers, base bus,
  and async bus modules so package-wide coverage cannot hide core regressions.
- Refresh the recorded stream benchmark with equivalent native and EventFlow
  map/filter work.
- Document that custom `EventHistory` implementations must synchronize all
  operations and produce query results from consistent snapshots.
- Limit the sync bus executor option to `ThreadPoolExecutor` and document the
  cross-bus reentrant delivery behavior for shared pools.
- Clarify that `clear()` leaves history and periodic schedules running, while
  `close()` stops schedules.
- Normalize timezone-aware `EventEnvelope` timestamps to UTC and warn against
  unpickling data from untrusted sources.

### Fixed

- Cancel and finish timed callback tasks when their invoking task is cancelled,
  including when cancellation is repeated during callback cleanup.
- Reject closing an `AsyncStream` from its active reader task with `RuntimeError`
  instead of allowing a self-wait deadlock; the read error path still closes
  the stream and its upstream sources.
- Reject awaitable results from synchronous callback hooks and event-bus
  actions, closing coroutine results before reporting the error.
- Report a clear cleanup error when an async cancellation handle's `cancel()`
  returns a non-awaitable; document that the scope validates this at close
  because checking during `add()` would invoke cancellation.
- Recheck bus state when registering a periodic publisher so a concurrent close
  cannot be followed by a newly started worker thread.
- Preserve `EventEnvelope` metadata immutability while supporting
  `dataclasses.asdict()` and pickle for serializable payloads.
- Restore subscription activity when sync or async removal fails so cancellation
  scopes and callers can retry cleanup.
- Reject non-boolean results from event and stream filter predicates instead of
  treating truthy or falsey values as matches.
- Preserve a stream source failure when closing also fails, retain both errors
  through exception chaining, and still attempt upstream cleanup.
- Enforce single-task stream ownership between reads so concurrent transformed
  consumption cannot steal items from the source.
- Continue scope cleanup after handle-owned cancellation or other BaseException
  interrupts, retaining failed handles and preserving additional cleanup errors.
- Keep synchronous `wait_for()` waiting for its claimed one-shot callback when
  an unrelated condition notification arrives before the value is captured.
- Prevent deadlock when callbacks publish through different buses sharing one
  saturated thread pool.
- Make concurrent async subscription cancellation wait for the in-flight
  removal result so scopes retain handles after a failed removal.
- Cancel and drain a `Task` or `Future` returned by an async callback when its
  timeout budget expires before callback delivery begins.
- Preserve read-only `EventEnvelope.metadata` after `copy.deepcopy()` and close
  async middleware continuations that are created but never awaited.
- Reject unsupported history values, non-thread executors, closed engine buses,
  boolean schedule intervals, and non-boolean callback predicate results at
  the relevant API boundary.
- Prevent multiple engine instances from claiming one sync or async bus, including
  concurrent registration races, without retaining the owner after its lifetime.

## [v1.1] - 2026-10-25

### Added

- Ship the PEP 561 `py.typed` marker and run tests, Ruff, and Pyright across
  supported Python versions and Linux, Windows, and macOS in GitHub Actions.
- Expose typed filter and replay options on the engine subscription methods.
- Check package line coverage and build, inspect, rebuild, and install wheel and
  source distributions in CI.
- Audit declared runtime dependencies for known vulnerabilities in CI.
- Add a standalone benchmark for history latest/query and sync/async replay
  paths.

### Changed

- Clarify that bus shutdown does not wait for callbacks already running in
  active publish calls.
- Document sequential and concurrent callback cancellation semantics, that
  cancellation cannot undo callback side effects, and the cooperative limits
  of async callback timeouts.
- Request only the requested replay window from history for unfiltered
  subscriptions, avoiding full-history copies with the built-in memory history.
- Document that custom history `append()` implementations run under the
  publication lock and must remain fast and non-reentrant.
- Document periodic error reporting, exclusive engine ownership, and async
  publication/statistics ordering semantics.
- Define `PublishResult.matched` as subscriptions selected for immediate
  delivery, excluding events buffered during replay setup.
- Pin GitHub Actions to the full commit SHAs used by the verified workflow run.

### Fixed

- Reject asynchronous event filter predicates instead of treating their
  coroutine objects as truthy matches.
- Remove cancellation-sensitive awaits from periodic task cleanup so a
  repeated cancellation cannot leave a finished schedule registered.
- Keep async periodic schedules registered until task cancellation completes,
  including when cancellation arrives before the coroutine starts.
- Let repeated async subscription cancellation retry removal after a lock-wait
  cancellation, and preserve `wait_for()` cleanup during repeated cancellation.
- Preserve unfiltered history fast paths through bus queries and validate
  filter conflicts even when history is disabled.
- Reject awaitable results from synchronous callbacks during direct, executor,
  and replay delivery, closing coroutine objects before reporting the error.
- Prevent registering one bus instance under multiple engine channel names,
  including while a removed channel is still closing.
- Release synchronous periodic schedule capacity when its worker thread cannot
  start.
- Wake synchronous `wait_for()` calls when `clear()` removes their subscriptions.
- Run Pyright against the Python version selected by each CI matrix job.
- Keep cancelled periodic publishers under bus ownership until their threads
  or tasks have actually finished.
- Make the replay benchmark use the checkout under test when run as a script.
- Clean up replay subscriptions when replay filtering or setup raises, and make
  asynchronous bus shutdown safe to retry after caller cancellation.
- Attempt to close every engine channel, retain channels whose shutdown failed,
  and report collected failures together.
- Report a cancelled callback as a delivery failure in concurrent async mode
  while preserving cancellation of the publishing task.
- Validate replay counts, history query limits, and wait timeouts consistently,
  including boolean, fractional, negative, and non-finite values.

## [v1.0] - 2026-10-24

### Added

- Expose the package version as `eventflow.__version__`.
- Extracted the event subsystem from [Vehicle Autonomy Core](https://github.com/TahsinCr/vehicle-autonomy-core)
  into the standalone EventFlow package. It provides thread-safe EventBus
  and asyncio-native AsyncEventBus implementations with immutable generic
  EventFilter values, type and predicate matching, one-time and limited
  subscriptions, replay, blocking or asynchronous waits, and cancellable
  periodic publishing.
- Added optional bounded EventHistory and MemoryEventHistory storage, latest
  and query helpers, PublishResult and EventBusStats, configurable ErrorPolicy,
  and sequential or concurrent asynchronous delivery.
- Added reusable synchronous and asynchronous bus actions for before, after,
  subscriber-error, and wait-timeout handling, with typed error and timeout
  contexts.
- Added EventEngine and AsyncEventEngine for lazy named channels, shared
  defaults, and aggregate lifecycle management.
- Added callable CallbackSubscription policies for frequency limits, maximum
  calls, one-time delivery, timeouts, predicates, enable/disable controls, and
  before/success/error/timeout/after hooks. Framework callback timeouts raise
  CallbackTimeoutError.
- Added bounded replay buffers and periodic schedule capacity, a bounded
  synchronous shutdown timeout with EventShutdownTimeoutError, and English and
  Turkish event guides.

### Changed

- Changed the project license from GPL-3.0-only to MIT.
- Renamed the distribution and public import package to EventFlow (`eventflow`).
- Preserved Python 3.10 support through one compatibility module. Python 3.10 uses conditional
  backports for StrEnum and ExceptionGroup; Python 3.11 and newer use the
  standard-library implementations.
- Shared capacity and timeout validation between both buses. Unfiltered latest
  reads are constant time; bounded queries stop after collecting the requested
  newest matches, while filtered history predicates run against a stable
  snapshot.
- Deliver replayed events before live events buffered during subscription
  setup.
- Reorganized the English and Turkish READMEs into a progressive usage guide
  with practical examples, behavioral explanations, and a public API reference.

### Fixed

- Preserve narrowed history filters during subscription setup. Evaluate replay
  and history predicates outside internal locks against stable snapshots.
- Make replay-buffer overflow visible and isolate it to the affected
  subscription. A full replay buffer does not silently discard live events or
  interrupt other subscribers.
- Close the AsyncEventBus.wait_for() registration race, clean up subscriptions
  when asynchronous replay is cancelled, and prevent reentrant publishing from
  deadlocking a single-worker executor.
- Apply the same admission-to-callback timeout budget to synchronous and
  asynchronous callback policies, and keep a user-raised TimeoutError distinct
  from a framework timeout.
- Retain ownership of timed-out synchronous publisher threads so a later
  close() can finish joining them, and join synchronous periodic publisher
  threads during shutdown.
