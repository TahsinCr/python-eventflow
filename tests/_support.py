"""Small shared test utilities."""


class ManualClock:
    """A controllable monotonic clock for deterministic timing tests."""

    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def set(self, value: float) -> None:
        self.value = value

    def advance(self, seconds: float) -> None:
        self.value += seconds
