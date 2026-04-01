from dataclasses import dataclass, field


@dataclass(frozen=True)
class Event:
    """A single simulation event. Frozen for immutability; sortable by timestamp."""

    timestamp: float
    # event_type: "INVOCATION" | "TIMEOUT"
    event_type: str
    function_id: str
    # trigger_type: "TIMER" | "API"
    trigger_type: str
    # duration: execution duration; 0.0 for TIMEOUT events
    duration: float = 0.0
    # ground_truth_cold_start: True if the function was cold started in the physical server trace
    ground_truth_cold_start: bool = False

    def __lt__(self, other: "Event") -> bool:
        return self.timestamp < other.timestamp


@dataclass
class Pod:
    """Represents a warm container instance."""

    function_id: str
    # state: "RUNNING" | "IDLE"
    state: str
    # last_active: timestamp of the last invocation
    last_active: float
