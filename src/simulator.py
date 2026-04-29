from __future__ import annotations

import heapq
from itertools import count

import pandas as pd

from src.models import Event, Pod, SimulationMetrics
from src.policies import Baseline60sPolicy, KeepAlivePolicy, TADKPolicy

_REQUIRED_COLUMNS = {"event_time", "func_id", "trigger_type", "exec_time", "cold_start_flag"}

_TRUTHY_COLD_START = {"1", "true", "t", "yes", "y"}
_FALSY_COLD_START = {"0", "false", "f", "no", "n", ""}


def _parse_cold_start_flag(raw: object) -> bool:
    """Parse a CSV cold_start_flag cell into a bool.

    Why: traces use either ``0``/``1`` (Huawei) or ``"true"``/``"false"`` (legacy).
    The previous ``str.lower() == "true"`` check silently returned False for the
    integer encoding, making Baseline Accuracy meaningless on those datasets.
    """
    if isinstance(raw, bool):
        return raw
    token = str(raw).strip().lower()
    if token in _TRUTHY_COLD_START:
        return True
    if token in _FALSY_COLD_START:
        return False
    raise ValueError(f"Unrecognized cold_start_flag value: {raw!r}")


def _parse_memory_mb(func_id: str) -> int:
    """Extract the memory tier (MB) encoded as the trailing token of func_id.

    Why: trace func_ids look like ``1118---631---pool24-600-512`` where the last
    ``-``-separated token is the pod's memory size in MB. Used to weight idle
    time by memory footprint (MB·s).
    """
    token = func_id.rsplit("-", 1)[-1]
    return int(token) if token.isdigit() else 0


class ServerlessSimulator:
    """Discrete-Event Simulator for serverless container keep-alive policies."""

    def __init__(self, policy: KeepAlivePolicy) -> None:
        self._policy = policy
        # Plain list driven by heapq — single-threaded, no lock overhead.
        # Items are 3-tuples (timestamp, seq, event) so equal timestamps fall
        # back to FIFO via the monotonic sequence counter.
        self._event_queue: list[tuple[float, int, Event]] = []
        self._virtual_clock: float = 0.0
        self._active_pods: dict[str, Pod] = {}
        # Maps function_id -> the timestamp of the most recently scheduled TIMEOUT.
        # Used to discard stale TIMEOUT events when a newer invocation refreshed the pod.
        self._pending_timeouts: dict[str, float] = {}
        self._total_invocations: int = 0
        self._total_cold_starts: int = 0
        self._baseline_accuracy_matches: int = 0
        self._total_idle_memory_mbs: float = 0.0
        # Per-function breakdown for research-grade reporting; keyed by func_id.
        # Schema kept in sync with SimulationMetrics.per_function_stats.
        self._func_stats: dict[str, dict] = {}
        self._seq = count()  # tie-breaker for equal timestamps

    # ------------------------------------------------------------------
    # Trace loading
    # ------------------------------------------------------------------

    def load_trace(self, filepath: str) -> None:
        """Read a CSV trace and enqueue all rows as INVOCATION events."""
        df = pd.read_csv(filepath)

        missing = _REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"CSV is missing required columns: {missing}")

        for row in df.itertuples(index=False):
            event = Event(
                timestamp=float(row.event_time),
                event_type="INVOCATION",
                function_id=str(row.func_id),
                trigger_type=str(row.trigger_type),
                duration=float(row.exec_time),
                ground_truth_cold_start=_parse_cold_start_flag(row.cold_start_flag),
            )
            heapq.heappush(self._event_queue, (event.timestamp, next(self._seq), event))

    # ------------------------------------------------------------------
    # Simulation loop
    # ------------------------------------------------------------------

    def run(self) -> SimulationMetrics:
        """Process all events in chronological order, print, and return metrics."""
        while self._event_queue:
            _, _, event = heapq.heappop(self._event_queue)
            self._virtual_clock = event.timestamp

            if event.event_type == "INVOCATION":
                self._handle_invocation(event)
            elif event.event_type == "TIMEOUT":
                self._handle_timeout(event)

        self._print_metrics()
        return self._build_metrics()

    def _build_metrics(self) -> SimulationMetrics:
        if self._total_invocations > 0:
            cold_rate = self._total_cold_starts / self._total_invocations
            accuracy: float | None = self._baseline_accuracy_matches / self._total_invocations
        else:
            cold_rate = 0.0
            accuracy = None
        return SimulationMetrics(
            total_invocations=self._total_invocations,
            total_cold_starts=self._total_cold_starts,
            cold_start_rate=cold_rate,
            idle_memory_mb_seconds=self._total_idle_memory_mbs,
            baseline_accuracy_matches=self._baseline_accuracy_matches,
            baseline_accuracy=accuracy,
            per_function_stats=self._func_stats,
        )

    def _handle_invocation(self, event: Event) -> None:
        self._total_invocations += 1

        fid = event.function_id
        simulated_cold_start = fid not in self._active_pods
        if simulated_cold_start:
            # No warm container available — cold start.
            self._total_cold_starts += 1

        if simulated_cold_start == event.ground_truth_cold_start:
            self._baseline_accuracy_matches += 1

        # Per-function tally — separate from aggregate counters; arithmetic
        # for aggregates above is unchanged.
        stats = self._func_stats.setdefault(
            fid,
            {
                "trigger_type": str(event.trigger_type),
                "total_invocations": 0,
                "total_cold_starts": 0,
                "total_idle_memory_mbs": 0.0,
            },
        )
        stats["total_invocations"] += 1
        if simulated_cold_start:
            stats["total_cold_starts"] += 1

        # Create or refresh the pod (immutable replacement, no in-place mutation).
        self._active_pods[fid] = Pod(
            function_id=fid,
            last_active=event.timestamp + event.duration,
            memory_mb=_parse_memory_mb(fid),
        )

        # Schedule a TIMEOUT event after execution + keep-alive window.
        timeout_duration = self._policy.get_timeout(fid, event.trigger_type)
        timeout_ts = event.timestamp + event.duration + timeout_duration
        self._pending_timeouts[fid] = timeout_ts

        timeout_event = Event(
            timestamp=timeout_ts,
            event_type="TIMEOUT",
            function_id=fid,
            trigger_type=event.trigger_type,
        )
        heapq.heappush(self._event_queue, (timeout_ts, next(self._seq), timeout_event))

    def _handle_timeout(self, event: Event) -> None:
        fid = event.function_id
        # Discard stale timeouts — a newer invocation may have rescheduled the
        # pod's timeout to a later time, making this event obsolete.
        if self._pending_timeouts.get(fid) != event.timestamp:
            return

        pod = self._active_pods.get(fid)
        if pod is not None:
            idle_seconds = event.timestamp - pod.last_active
            idle_mbs = idle_seconds * pod.memory_mb
            self._total_idle_memory_mbs += idle_mbs
            stats = self._func_stats.get(fid)
            if stats is not None:
                stats["total_idle_memory_mbs"] += idle_mbs

        self._active_pods.pop(fid, None)
        self._pending_timeouts.pop(fid, None)

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def _print_metrics(self) -> None:
        print(f"Total Invocations : {self._total_invocations}")
        print(f"Total Cold Starts : {self._total_cold_starts}")
        if self._total_invocations > 0:
            rate = self._total_cold_starts / self._total_invocations
            print(f"Cold Start Rate   : {rate:.2%}")
            accuracy = self._baseline_accuracy_matches / self._total_invocations
            print(f"Baseline Accuracy : {accuracy:.2%} ({self._baseline_accuracy_matches}/{self._total_invocations} matches)")
        else:
            print("Cold Start Rate   : N/A")
            print("Baseline Accuracy : N/A")
        print(f"Total Idle Memory : {self._total_idle_memory_mbs:,.2f} MB·s")


# ------------------------------------------------------------------
# Quick smoke test
# ------------------------------------------------------------------

if __name__ == "__main__":
    TRACE = "data/region2_simulator_input.csv"

    print("=== Baseline60sPolicy ===")
    sim_baseline = ServerlessSimulator(Baseline60sPolicy())
    sim_baseline.load_trace(TRACE)
    sim_baseline.run()

    print()
    print("=== TADKPolicy (timer_interval=300s) ===")
    sim_tadk = ServerlessSimulator(TADKPolicy(timer_interval=300.0))
    sim_tadk.load_trace(TRACE)
    sim_tadk.run()
