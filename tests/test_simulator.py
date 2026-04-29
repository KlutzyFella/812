"""Regression invariants for the DES.

Three guards against silent breakage of the simulator's metric accounting and
chronological event ordering:

  1. cold_starts <= total_invocations          (sanity)
  2. idle_memory_mb_seconds >= 0               (sanity)
  3. Hand-computed determinism: shuffling input row order must not change
     the metrics, AND the metrics must match a hand-computed expected value
     on a fixed fixture trace.

Fixture trace
-------------
Single function ``test---1---pool1-100-512`` (memory tier = 512 MB).
Three invocations under Baseline60sPolicy, each 1s of execution:

  t=0   exec=1s  -> ends t=1.   Pod warm until t=61   (cold start, no prior pod)
  t=30  exec=1s  -> ends t=31.  Pod refreshed; warm until t=91   (warm hit, t=30<61)
  t=200 exec=1s  -> ends t=201. Pod refreshed; warm until t=261  (cold start, t=200>91)

Expected metrics:
  total_invocations = 3
  total_cold_starts = 2  (t=0 and t=200)
  idle_memory_mb_seconds = 60s * 512 MB + 60s * 512 MB = 61_440  MB·s
                            ^ from the t=91 timeout (t=30 invocation)
                                              ^ from the t=261 timeout (t=200 invocation)
  (the t=61 stale TIMEOUT is discarded because t=30 rescheduled to t=91)
"""
from __future__ import annotations

import csv
import random
from pathlib import Path

import pytest

from src.policies import Baseline60sPolicy
from src.simulator import ServerlessSimulator


FID = "test---1---pool1-100-512"
ROWS = [
    # event_time, func_id, trigger_type, exec_time, cold_start_flag
    (0.0, FID, "API", 1.0, 1),
    (30.0, FID, "API", 1.0, 0),
    (200.0, FID, "API", 1.0, 1),
]
EXPECTED_INVOCATIONS = 3
EXPECTED_COLD_STARTS = 2
EXPECTED_IDLE_MEMORY_MBS = 61_440.0


def _write_trace(path: Path, rows: list[tuple]) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["event_time", "func_id", "trigger_type", "exec_time", "cold_start_flag"])
        writer.writerows(rows)


def _run(trace_path: Path) -> ServerlessSimulator:
    sim = ServerlessSimulator(Baseline60sPolicy())
    sim.load_trace(str(trace_path))
    sim.run()
    return sim


def test_cold_starts_le_total_invocations(tmp_path: Path) -> None:
    trace = tmp_path / "fixture.csv"
    _write_trace(trace, ROWS)
    sim = _run(trace)
    assert sim._total_cold_starts <= sim._total_invocations


def test_idle_memory_nonnegative(tmp_path: Path) -> None:
    trace = tmp_path / "fixture.csv"
    _write_trace(trace, ROWS)
    sim = _run(trace)
    assert sim._total_idle_memory_mbs >= 0.0


def test_chronological_determinism_and_known_values(tmp_path: Path) -> None:
    sorted_trace = tmp_path / "sorted.csv"
    shuffled_trace = tmp_path / "shuffled.csv"

    _write_trace(sorted_trace, ROWS)

    shuffled = list(ROWS)
    random.Random(1234).shuffle(shuffled)
    _write_trace(shuffled_trace, shuffled)

    sim_sorted = _run(sorted_trace)
    sim_shuffled = _run(shuffled_trace)

    # Order independence: the priority queue must impose chronological order.
    assert sim_sorted._total_invocations == sim_shuffled._total_invocations
    assert sim_sorted._total_cold_starts == sim_shuffled._total_cold_starts
    assert sim_sorted._total_idle_memory_mbs == pytest.approx(sim_shuffled._total_idle_memory_mbs)

    # Hand-computed expected values.
    assert sim_sorted._total_invocations == EXPECTED_INVOCATIONS
    assert sim_sorted._total_cold_starts == EXPECTED_COLD_STARTS
    assert sim_sorted._total_idle_memory_mbs == pytest.approx(EXPECTED_IDLE_MEMORY_MBS)
