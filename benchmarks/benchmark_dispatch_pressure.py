"""Measure bounded route-cache pressure and sparse native dispatch matching."""

from __future__ import annotations

from itertools import cycle

from eventflow import EventBus

from ._runner import bench_sync, create_runner, forward_cli_arguments
from ._support import ignore_event


def main() -> None:
    runner = create_runner(__doc__ or "Dispatch pressure", add_cmdline_args=forward_cli_arguments("suite"))
    runner.argparser.add_argument("--suite", choices=("routing", "matching"), default="routing")
    args = runner.parse_args()
    if args.suite == "matching":
        for count in (64, 100, 2048):
            for percent in (0, 1, 10, 50, 100):
                accepted = count * percent // 100
                with EventBus[int]() as bus:
                    for index in range(count):
                        bus.subscribe(ignore_event, predicate=lambda event, index=index: index < accepted)
                    bench_sync(
                        runner, f"matching/{count}/{percent}", lambda bus=bus: bus.publish(1),
                        metadata={"subscribers": count, "matched_subscribers": accepted},
                    )
        return
    for count in (16, 64, 65, 80, 256, 1000):
        for wildcard in (0, 100, 1024):
            for profile in ("uniform", "late-hot"):
                with EventBus[int](routing_key=lambda event: event) as bus:
                    for key in range(count):
                        bus.subscribe(ignore_event, routing_key=key)
                    for _ in range(wildcard):
                        bus.subscribe(ignore_event)
                    for key in range(min(count, 64)):
                        bus.publish(key)
                    keys = cycle(range(count) if profile == "uniform" else (count - 1,))
                    bench_sync(
                        runner, f"routing/{count}/{wildcard}/{profile}",
                        lambda bus=bus, keys=keys: bus.publish(next(keys)),
                        metadata={"routes": count, "wildcard_subscribers": wildcard, "access_profile": profile},
                    )


if __name__ == "__main__":
    main()
