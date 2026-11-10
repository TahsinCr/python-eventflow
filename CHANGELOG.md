# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- Add indexed routing keys to synchronous `EventBus` subscriptions and named
  `EventEngine` channels. Keyed and wildcard callbacks retain registration
  order, while unrelated keyed subscribers are skipped.
- Let `AsyncEventQueue` apply EventFlow event filters, predicates and routing
  keys before bounded queue capacity is consumed.
- Allow `BoundedWorkPool` users to set the worker thread name prefix.
- Add routing-key correctness tests and pyperf publisher-scaling cases.
- Add a root-level `run_tests.py` command that prints each test result and
  returns a failure status when the suite fails.
- Extend the pyperf matrix to measure named engine channels, cache and state
  operations, context and metrics, policy evaluation, resilience paths, callback
  policies, lifecycle scopes, bounded work submission and shared async work.
- Add a single-publisher bridge baseline, history-size and filter-selectivity
  cases, and async fan-out callbacks that yield once to the event loop.
- Add focused regression tests for retry deadlines and cancellation, cooldown
  waiter release, scheduler/coalescer callback failures, strict operation metric
  validation and routing order after subscription cancellation.
- Add an optional Python/Cython/C benchmark suite for history type filtering,
  routing subscriber merges and complete synchronous EventBus publish paths.
  Native extensions stay outside the EventFlow runtime and package.
- Add seven focused regression tests for synchronous awaitable cleanup, retry
  deadline limits, retry observer failures and cancellation, and callback
  predicate awaitables.
- Expand the native benchmark with `heapq`, two-pointer and `sorted()` Python
  routing candidates, Cython key-lookup and item-iterator variants, history
  type tuples and limits, small subscriber counts, routing profiles and bounded
  callback work. Add smoke and decision matrices for quick checks and focused
  comparisons.
- Record Cython, compiler flags and native binary SHA-256 values with native
  pyperf runs, and validate candidate routing under concurrent publishing and
  subscription churn.
- Compare routed EventBus publication with one, four and eight publisher threads
  in the native decision matrix, including the active production Python path.
- Add a routing-only pyperf matrix for targeted snapshot, publish and contention
  comparisons without unrelated history workloads.
- Add a history-only native benchmark matrix for focused 100,000-event query
  comparisons without routing workloads.
- Cover skewed keyed/wildcard indexes and concurrent subscription churn in the
  synchronous routing regression tests.

### Changed

- Merge keyed and wildcard subscriber indexes with a linear two-pointer path in
  synchronous `EventBus` snapshots, preserving registration order and the
  existing one-index fast paths.
- Label the current production EventBus merge as the Python benchmark baseline;
  retain `heapq` only as an explicit comparison candidate.
- Consolidate repeated benchmark callback and positive-integer parser helpers,
  and reuse one event-type fixture across the native reference tests.
- Include committed C and Cython benchmark sources in the pyperf source
  fingerprint so native candidate results identify the exact kernels measured.
- Remove 31 benchmark result files from earlier source revisions and harnesses,
  along with their legacy guide, so the current 1.4.0 performance baseline is
  unambiguous.
- Refresh the performance report and CSV with 409 results from corrected
  benchmark source `4af998d`: 180 broad CPython 3.14.9 cases, 81 Python 3.10.22
  focus cases, matched GIL-on/off sets, bridge latency distributions and
  repeated 100k history queries. Remove the 19 superseded result files.
- Rename the Python-version compatibility helper to `eventflow._compatibility`
  to mark it as an internal module.
- Use pyperf as the shared timing and result engine for all benchmark modules.
  Benchmark runs now produce standard pyperf JSON, which can be compared with
  `pyperf compare_to`; source identity and the worker's observed GIL mode remain
  recorded as benchmark metadata.
- Measure bridge-publisher wall time and process CPU time per delivered event
  after queue and thread setup, so GIL-enabled and free-threaded runs can be
  compared with the same workload and result format.
- Record 240 source-pinned Python/Cython/C benchmark cases across CPython
  3.10.18, 3.14.9, 3.15.2 with the GIL and 3.15.2t without it. For
  2,048-subscriber publishes, candidate latency was 11–30% lower with no-op
  callbacks and 25–54% lower when filters rejected delivery; the 3.15.2
  GIL-on filtered pair was high-variance. These benchmark results do not change
  runtime behavior.
- Record a reversed-order repeat of the expanded native benchmark on CPython
  3.14.9, with 346 summary rows across two orders and 30 pooled measurements
  per case. Separate isolated merge/filter kernels from full publish results,
  and record the observed variance and single-host limits.

### Fixed

- Keep optional Cython and C benchmark imports out of the default development
  dependency path while type-checking benchmark kernels and call signatures in
  clean Python 3.10 and 3.14 environments.
- Preserve arbitrary-size Python integer ordering in the C routing candidate,
  support limited tuple history filtering, and remove redundant integer
  conversions from the merge tails.
- Measure cross-thread queue roundtrips with a publisher thread that calls
  `EventBus.publish()` directly. Remove the benchmark-only per-event request
  queue and future handoff that inflated the measured bridge path.
- Close each synchronous schedule benchmark bus after cancelling its handles,
  so pyperf loop calibration cannot accumulate still-exiting schedule threads
  and hit the bus capacity limit.
- Skip generic awaitable inspection for exact boolean event-filter results;
  the sparse 100,000-event history query measured 4.48x faster on CPython
  3.14.9 and 4.85x faster on GIL-disabled CPython 3.15.2t. These are local
  microbenchmark results, not general latency guarantees.
- Skip generic awaitable inspection for exact `int` and `bool` results in
  synchronous `AsyncStream` transformations; the 5,000-item map/filter case
  measured 1.63x faster unbuffered on CPython 3.14.9 and 1.57x faster on
  GIL-disabled CPython 3.15.2t. The 16-item buffered case measured 1.25x
  faster on CPython 3.14.9; its free-threaded baseline was too noisy to use.

- Forward custom benchmark arguments to pyperf worker processes so publisher
  counts, workload sizes and benchmark metadata remain consistent across every
  worker process, including list-valued and comma-separated options.
- Build async pipeline benchmark instances inside each pyperf worker event loop
  and exclude setup from the measured execution time.

### Removed

- Remove the custom benchmark sampler and comparison command in favor of
  pyperf's worker, statistics, JSON and comparison tools. Earlier result
  snapshots are removed when their source revision is no longer current.

## [v1.3.2] - 2026-11-08

### Added

- Run the full suite on GIL-disabled CPython 3.14t and 3.15t on Windows and
  macOS as well as the existing Ubuntu jobs.
- Measure synchronous-to-async bridge throughput and round-trip latency with
  2, 4, 8, and 16 concurrent publisher threads, including process CPU use.
- Add focused `LoopPortal` tests for retrying a failed thread start and releasing
  the loop thread after startup failure.
- Run the full unit suite on Ubuntu with the GIL explicitly disabled on CPython
  3.14t and 3.15t, and verify the interpreter mode before testing.
- Add a bridge benchmark for direct async calls, `LoopPortal` round-trips, and
  separately labeled same-thread and cross-thread waiting-consumer round-trips
  using the shared measurement harness.
- Add a worker-backed `KeyedCoalescer` benchmark for single and concurrent
  producers, merged updates, and concurrent flushes across configurable key
  counts, with repeated end-to-end timing samples.
- Add a same-key `KeyedRateLimiter` benchmark that reports acquisition wait
  distributions, worker completion spread, and observed waiter cancellations.
- Add public contract coverage for the published lowercase string values of
  `ErrorPolicy` and `DeliveryMode`.

### Changed

- Clarify engine behavior when a channel is created after the shutdown snapshot
  while `stop()` is still closing its existing channels.
- Document that `VersionedCache.get()` cannot distinguish a stored `None` from
  an absent, expired, or stale entry.
- Include the source revision, source-only working-tree status, a deterministic
  source fingerprint, and the interpreter's GIL mode in benchmark JSON reports.
  Comparison reports also identify the baseline source independently.
- Define bridge benchmark CPU utilization as process CPU time normalized to one
  core; concurrent process threads can make it exceed 100%.
- Run each synchronous executor callback in a fresh copy of the context active
  during `publish()`, keeping `ContextVar` behavior consistent across GIL modes.
- Record a matched CPython 3.15.2 GIL-on/free-threaded bridge comparison: local
  cross-thread P50 was 18.6% lower without the GIL, while same-thread P50 was
  11.7% higher. See the
  [measurement details](benchmarks/results/README.md#cpython-3152-free-threading-comparison-2026-11-07).
- Clarify that `KeyedCoalescer.stats().failures` counts delivery callback
  failures, while `on_error` and observer errors are retained for `flush()` or
  `close()` to report.
- Complete an `AsyncEventQueue` waiter directly when synchronous publication
  runs on the queue's event-loop thread; publishers on other threads continue
  to use `call_soon_threadsafe()`. Across seven local runs, same-thread P50
  round-trip latency fell 31.1% on Python 3.10.18 and 41.1% on Python 3.14.9.
  Cross-thread P50 changed by less than 1.5%; tail latency varied between runs.
  See the
  [measurement details](benchmarks/results/README.md#async-event-queue-waiter-handoff-2026-11-07).
- Read fused `AsyncStream.map()`/`filter()` chains directly from their root
  iterator, avoiding a nested `AsyncStream.__anext__()` handoff while preserving
  single-consumer ownership and close propagation. A 5,000-item local run
  reduced unbuffered median latency by 14.9% on Python 3.10.18 and 8.5% on
  Python 3.14.9; buffer-16 latency fell by 6.3% and 2.0%. Peak traced memory
  increased by 736 and 1,248 bytes for unbuffered runs. See the
  [measurement details](benchmarks/results/README.md#asyncstream-root-read-2026-11-07).
- Check Ruff formatting in GitHub Actions alongside lint rules.
- Keep the full operating-system/Python test matrix while running Ruff once,
  Pyright on Python 3.10 and 3.14, and the 3.10/Linux suite once under branch
  coverage.
- Expand the package summary to cover EventFlow's released pipeline and
  coordination APIs, and name the frozen API contract for the v1.3.1 release.
- Index pending `KeyedCoalescer` deadlines in a min-heap, so each due item is
  selected in O(log K) time instead of scanning all K pending keys. Merged
  updates keep the first value's deadline.
- Draining expired coalescer entries measured 87.1–99.9% lower median time
  across 128–16,384 keys on Python 3.10.18 and 3.14.9; see the
  [benchmark report](benchmarks/results/README.md#keyedcoalescer-due-item-drain-2026-11-07)
  for the workload and sample counts.
- Clarify that `on_before` is not a delivery gate or authorization boundary,
  document `PolicyEvaluator`'s in-process security boundary, and explain how
  to bound work before publisher tasks are created.
- Document that cancelling a `SingleFlight.aclose()` caller leaves its owned
  shutdown running, while task and resource scopes finish cleanup before
  re-raising caller cancellation.
- Prevent `KeyedRateLimiter.try_acquire()` from consuming a token while an
  asynchronous acquisition for the same key is waiting; waiting acquisitions
  still have no strict FIFO guarantee.
- Avoid scanning every cooldown key on ordinary `block()` updates; prune all
  expired keys only when a new key needs capacity.
- Repeated existing-key cooldown updates measured 26.6–97.9% lower median
  latency on CPython 3.10.18 and 31.5–98.8% lower on 3.14.9 across 10–4,096
  stored keys; exact results and workload details are in the
  [benchmark report](benchmarks/results/README.md#cooldownregistry-existing-key-update-2026-11-07).

### Fixed

- Keep short bridge benchmark smoke tests valid when CPU timers report zero by
  checking that the reported CPU metrics are finite and non-negative.
- Run free-threaded GIL checks with a shell-independent Python command on
  Windows.
- Give the `LoopPortal` executor-shutdown test enough time to reach its
  stopping phase on Windows runners.
- Resolve strict Pyright diagnostics in the publish/runtime benchmarks by typing
  their sample collections and initializing memory-soak midpoint snapshots.
- Make rate-limiter benchmark cancellation setup independent of the measured
  token refill rate, and include captured output when a benchmark subprocess
  fails.
- Require baseline and candidate source provenance in new before/after
  benchmark records while preserving existing historical results.
- Synchronize the synchronous `EventBus.wait_for()` test on subscription
  registration instead of waiting on a condition that receives no notification.
- Preserve upstream `AsyncStream.__anext__()` overrides in fused `map()` and
  `filter()` chains by using the direct root-iterator path only when no override
  is present.
- Wake waiting `AsyncEventQueue` consumers for accepted events even when
  observer reporting raises.
- Keep `KeyedCoalescer` workers alive after observer failures, finish accepted
  deliveries, and surface reporting errors through `flush()` or `close()`.
- Reject `BoundedWorkPool.shutdown(wait=True)` from completion callbacks running
  on the pool's worker threads.
- Keep Python 3.10 rate-limiter acquisitions waiting after `asyncio.wait_for()`
  reaches its token-refill timeout.
- Read `BoundedWorkPool` worker identities under the same lock used to update
  them before checking for a self-waiting shutdown.
- Ignore circuit-breaker outcomes from earlier generations after the circuit
  opens, recovers, or is reset.
- Restore a callback subscription's final-call quota when automatic unsubscribe
  fails, allowing a later delivery to retry cancellation.
- Keep concurrent coalescer flush requests independent and make the first close
  call's flush/discard policy stable across shutdown retries.
- Notify work-pool capacity observers outside the pool lock and preserve the
  capacity error if observer reporting fails.

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
