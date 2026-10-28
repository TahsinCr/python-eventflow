"""Pipeline execution errors shared by sync and async implementations."""

from typing import Literal


class PipelineExecutionError(RuntimeError):
    """Fail-fast error with component, input and the original exception as cause.

    Observer failures during an existing failure are retained separately so
    instrumentation cannot replace the original cause.
    """

    def __init__(
        self,
        name: str | None,
        context: object,
        *,
        phase: Literal["stage", "middleware", "run", "observer"] = "stage",
    ) -> None:
        self.name = name
        self.context = context
        self.phase = phase
        self.observer_errors: tuple[Exception, ...] = ()
        super().__init__(f"Pipeline {phase} failed: {name or '<run>'}")
