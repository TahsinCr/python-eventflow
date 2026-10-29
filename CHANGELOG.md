# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

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
- Use the available `eventflow-tahsincr` PyPI distribution name while keeping
  the `eventflow` Python import package.
- Bound pipeline middleware registrations at 64 to fail before exhausting the
  Python call stack, and update `PipelineStats` in place rather than allocating
  a new metrics snapshot for every observation.
- Load new execution components lazily through the package exports so ordinary
  bus imports and publication do not initialize optional execution machinery.
- Keep inline annotations but stop marking installed distributions as PEP 561
  typed while the `py.typed` marker is absent.
- Extend CI typing to the runnable examples and benchmarks, require at least
  90% pipeline line coverage, and smoke-test new APIs from installed artifacts.
  The existing 88% package coverage floor and Python/platform matrix remain.

### Fixed

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
- Document sequential and concurrent callback cancellation semantics and the
  cooperative limits of async callback timeouts.
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
