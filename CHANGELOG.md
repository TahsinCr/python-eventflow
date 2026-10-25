# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- Ship the PEP 561 `py.typed` marker and run tests, Ruff, and Pyright across
  supported Python versions and Linux, Windows, and macOS in GitHub Actions.
- Expose typed filter and replay options on the engine subscription methods.
- Check package line coverage and build, inspect, rebuild, and install wheel and
  source distributions in CI.
- Add a standalone, standard-library replay benchmark for sync and async buses.

### Changed

- Clarify that bus shutdown does not wait for callbacks already running in
  active publish calls.
- Request only the requested replay window from history for unfiltered
  subscriptions, avoiding full-history copies with the built-in memory history.
- Document that custom history `append()` implementations run under the
  publication lock and must remain fast and non-reentrant.
- Define `PublishResult.matched` as subscriptions selected for immediate
  delivery, excluding events buffered during replay setup.
- Pin GitHub Actions to the full commit SHAs used by the verified workflow run.

### Fixed

- Reject asynchronous event filter predicates instead of treating their
  coroutine objects as truthy matches.
- Remove cancellation-sensitive awaits from periodic task cleanup so a
  repeated cancellation cannot leave a finished schedule registered.
- Prevent registering one bus instance under multiple engine channel names,
  including while a removed channel is still closing.
- Release synchronous periodic schedule capacity when its worker thread cannot
  start.
- Wake synchronous `wait_for()` calls when `clear()` removes their subscriptions.
- Run Pyright against the Python version selected by each CI matrix job.
- Keep cancelled periodic publishers under bus ownership until their threads
  or tasks have actually finished.
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
