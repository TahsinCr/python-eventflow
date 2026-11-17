"""A minimal synchronous EventFlow application."""

from dataclasses import dataclass

from eventflow import EventBus


@dataclass(frozen=True)
class TemperatureChanged:
    sensor: str
    celsius: float


def main() -> None:
    received: list[TemperatureChanged] = []

    with EventBus[TemperatureChanged]() as bus:
        subscription = bus.subscribe(received.append)
        result = bus.publish(TemperatureChanged("north", 21.5))
        subscription.cancel()

    print(
        f"matched={result.matched}; received={len(received)}; "
        f"sensor={received[0].sensor}; celsius={received[0].celsius}"
    )


if __name__ == "__main__":
    main()
