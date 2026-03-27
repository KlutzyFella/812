from dataclasses import dataclass, field


@dataclass(frozen=True)
class Event:
    """A single simulation event. Frozen for immutability; sortable by timestamp."""

    timestamp: float
    event_type: str  # "INVOCATION" | "TIMEOUT"
    function_id: str
    trigger_type: str  # "TIMER" | "API"
    duration: float = 0.0  # execution duration; 0.0 for TIMEOUT events

    def __lt__(self, other: "Event") -> bool:
        return self.timestamp < other.timestamp


@dataclass
class Pod:
    """Represents a warm container instance."""

    function_id: str
    state: str  # "RUNNING" | "IDLE"
    last_active: float
