from abc import ABC, abstractmethod

VALID_TRIGGER_TYPES = {"TIMER", "API"}


class KeepAlivePolicy(ABC):
    """Abstract base class for keep-alive policies.

    Subclasses implement get_timeout() to return the number of seconds a
    container should be kept alive after an invocation completes.
    """

    @abstractmethod
    def get_timeout(self, function_id: str, trigger_type: str, **kwargs) -> float:
        """Return the keep-alive timeout in seconds for the given invocation."""


class Baseline60sPolicy(KeepAlivePolicy):
    """Static 60-second timeout for all invocations regardless of trigger type."""

    def get_timeout(self, function_id: str, trigger_type: str, **kwargs) -> float:
        if trigger_type not in VALID_TRIGGER_TYPES:
            raise ValueError(f"Unknown trigger_type '{trigger_type}'. Expected one of {VALID_TRIGGER_TYPES}.")
        return 60.0


class TADKPolicy(KeepAlivePolicy):
    """Trigger-Aware Dynamic Keep-Alive policy.

    - API invocations: fixed 20-second timeout.
    - TIMER invocations: timer_interval + jitter_buffer, so the container is
      still warm when the next timer fires.
    """

    def __init__(self, timer_interval: float, jitter_buffer: float = 5.0) -> None:
        if timer_interval <= 0:
            raise ValueError(f"timer_interval must be positive, got {timer_interval}.")
        self._timer_interval = timer_interval
        self._jitter_buffer = jitter_buffer

    def get_timeout(self, function_id: str, trigger_type: str, **kwargs) -> float:
        if trigger_type == "API":
            return 20.0
        if trigger_type == "TIMER":
            return self._timer_interval + self._jitter_buffer
        raise ValueError(f"Unknown trigger_type '{trigger_type}'. Expected one of {VALID_TRIGGER_TYPES}.")
