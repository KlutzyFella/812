from __future__ import annotations

import queue
from itertools import count

import pandas as pd

from src.models import Event, Pod
from src.policies import Baseline60sPolicy, KeepAlivePolicy, TADKPolicy

_REQUIRED_COLUMNS = {"event_time", "func_id", "trigger_type", "exec_time", "cold_start_flag"}


class ServerlessSimulator:
    """Discrete-Event Simulator for serverless container keep-alive policies."""

    def __init__(self, policy: KeepAlivePolicy) -> None:
        self._policy = policy
        self._event_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._virtual_clock: float = 0.0
        self._active_pods: dict[str, Pod] = {}
        # Maps function_id -> the timestamp of the most recently scheduled TIMEOUT.
        # Used to discard stale TIMEOUT events when a newer invocation refreshed the pod.
        self._pending_timeouts: dict[str, float] = {}
        self._total_invocations: int = 0
        self._total_cold_starts: int = 0
        self._baseline_accuracy_matches: int = 0
        self._total_idle_time: float = 0.0
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
                ground_truth_cold_start=str(row.cold_start_flag).strip().lower() == "true",
            )
            self._event_queue.put((event.timestamp, next(self._seq), event))

    # ------------------------------------------------------------------
    # Simulation loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Process all events in chronological order and print metrics."""
        while not self._event_queue.empty():
            _, _, event = self._event_queue.get()
            self._virtual_clock = event.timestamp

            if event.event_type == "INVOCATION":
                self._handle_invocation(event)
            elif event.event_type == "TIMEOUT":
                self._handle_timeout(event)

        self._print_metrics()

    def _handle_invocation(self, event: Event) -> None:
        self._total_invocations += 1

        fid = event.function_id
        simulated_cold_start = fid not in self._active_pods
        if simulated_cold_start:
            # No warm container available — cold start.
            self._total_cold_starts += 1

        if simulated_cold_start == event.ground_truth_cold_start:
            self._baseline_accuracy_matches += 1

        # Create or refresh the pod (immutable replacement, no in-place mutation).
        self._active_pods[fid] = Pod(
            function_id=fid,
            state="IDLE",
            last_active=event.timestamp + event.duration,
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
        self._event_queue.put((timeout_ts, next(self._seq), timeout_event))

    def _handle_timeout(self, event: Event) -> None:
        fid = event.function_id
        # Discard stale timeouts — a newer invocation may have rescheduled the
        # pod's timeout to a later time, making this event obsolete.
        if self._pending_timeouts.get(fid) != event.timestamp:
            return

        pod = self._active_pods.get(fid)
        if pod is not None:
            self._total_idle_time += event.timestamp - pod.last_active

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
        print(f"Total Idle Time   : {self._total_idle_time:.2f}s")


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
