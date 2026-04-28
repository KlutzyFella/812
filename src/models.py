from dataclasses import dataclass, field
from enum import StrEnum


class EventType(StrEnum):
    """Type tag for events on the simulator's priority queue."""

    INVOCATION = "INVOCATION"
    TIMEOUT = "TIMEOUT"


class TriggerType(StrEnum):
    """Normalized trigger source for an invocation."""

    API = "API"
    TIMER = "TIMER"


def normalize_trigger(raw: str) -> TriggerType:
    """Normalize a raw trace trigger string (e.g. 'APIG', 'TIMER') to TriggerType.

    Why: trace files use mixed labels (APIG, HTTP, TIMER, CRON-style); the policy
    layer must dispatch on a closed enum, not a substring sniff.
    """
    return TriggerType.TIMER if "timer" in str(raw).lower() else TriggerType.API


@dataclass(frozen=True)
class Event:
    """A single simulation event. Frozen for immutability; sortable by timestamp."""

    timestamp: float
    event_type: EventType
    function_id: str
    trigger_type: TriggerType
    duration: float = 0.0
    ground_truth_cold_start: bool = False
    # Per-fid epoch tag for stale-TIMEOUT detection (see simulator._handle_timeout).
    epoch: int = 0

    def __lt__(self, other: "Event") -> bool:
        return self.timestamp < other.timestamp


@dataclass(frozen=True, slots=True)
class Pod:
    """Represents a warm container instance. Replaced (not mutated) on each refresh."""

    function_id: str
    last_active: float
    memory_mb: int = 0


@dataclass(frozen=True)
class SimulationMetrics:
    """Outcome metrics from one simulation run. Public, returned by Simulator.run()."""

    total_invocations: int
    total_cold_starts: int
    cold_start_rate: float
    idle_memory_mb_seconds: float
    baseline_accuracy_matches: int
    baseline_accuracy: float | None = None
