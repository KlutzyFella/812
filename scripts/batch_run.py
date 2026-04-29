"""Batch runner: sweep 3 input sizes × 2 policies and print a Markdown summary.

Does not modify simulator or policy logic — only invokes them and reads
the resulting metric attributes off each ServerlessSimulator instance.
"""
from __future__ import annotations

import contextlib
import csv
import io
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Make `src` importable when run from project root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.policies import Baseline30sPolicy, Baseline60sPolicy, TADKPolicy  # noqa: E402
from src.simulator import ServerlessSimulator  # noqa: E402

INPUT_DIR = PROJECT_ROOT / "OutputData" / "OutputData"
DATASETS = [
    ("20k", INPUT_DIR / "region2_20000_simulator_input.csv"),
    ("50k", INPUT_DIR / "region2_50000_simulator_input.csv"),
    ("100k", INPUT_DIR / "region2_100000_simulator_input.csv"),
]
POLICIES = [
    ("Baseline60sPolicy", lambda: Baseline60sPolicy()),
    ("TADKPolicy(10s)", lambda: TADKPolicy(timer_interval=10.0, jitter_buffer=5.0)),
]
# Extra single-dataset comparison: 100k across three policies.
EXTRA_100K_POLICIES = [
    ("Baseline60sPolicy", lambda: Baseline60sPolicy()),
    ("Baseline30sPolicy", lambda: Baseline30sPolicy()),
    ("TADKPolicy(10s)", lambda: TADKPolicy(timer_interval=10.0, jitter_buffer=5.0)),
]


@dataclass(frozen=True)
class RunResult:
    dataset: str
    policy: str
    total_requests: int
    cold_starts: int
    cold_start_rate: float
    idle_memory_mbs: float
    per_function_stats: dict[str, dict] = field(default_factory=dict)


def run_one(dataset: str, csv_path: Path, policy_name: str, policy_factory) -> RunResult:
    sim = ServerlessSimulator(policy_factory())
    sim.load_trace(str(csv_path))
    # Suppress simulator's own print output so our table stays clean.
    with contextlib.redirect_stdout(io.StringIO()):
        metrics = sim.run()

    total = metrics.total_invocations
    cold = metrics.total_cold_starts
    rate = (cold / total * 100.0) if total else 0.0
    return RunResult(
        dataset=dataset,
        policy=policy_name,
        total_requests=total,
        cold_starts=cold,
        cold_start_rate=rate,
        idle_memory_mbs=metrics.idle_memory_mb_seconds,
        per_function_stats=metrics.per_function_stats,
    )


PER_FUNCTION_CSV_HEADERS = [
    "dataset_size",
    "policy_name",
    "func_id",
    "trigger_type",
    "total_invocations",
    "total_cold_starts",
    "total_idle_memory_mbs",
]


def write_per_function_csv(results: list[RunResult], output_path: Path) -> int:
    """Flatten per-function stats from each RunResult into one CSV. Returns row count."""
    rows_written = 0
    with output_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(PER_FUNCTION_CSV_HEADERS)
        for r in results:
            for func_id, stats in r.per_function_stats.items():
                writer.writerow([
                    r.dataset,
                    r.policy,
                    func_id,
                    stats.get("trigger_type", ""),
                    stats.get("total_invocations", 0),
                    stats.get("total_cold_starts", 0),
                    stats.get("total_idle_memory_mbs", 0.0),
                ])
                rows_written += 1
    return rows_written


def format_markdown(results: list[RunResult]) -> str:
    header = (
        "| Dataset | Policy | Total Requests | Cold Starts | Cold Start Rate (%) | Idle Memory (MB·s) |\n"
        "|---------|--------|---------------:|------------:|--------------------:|-------------------:|\n"
    )
    rows = []
    for r in results:
        rows.append(
            f"| {r.dataset} | {r.policy} | {r.total_requests:,} | {r.cold_starts:,} "
            f"| {r.cold_start_rate:.2f} | {r.idle_memory_mbs:,.2f} |"
        )
    return header + "\n".join(rows)


def main() -> int:
    results: list[RunResult] = []
    for dataset, csv_path in DATASETS:
        if not csv_path.exists():
            print(f"[skip] missing: {csv_path}", file=sys.stderr)
            continue
        for policy_name, factory in POLICIES:
            print(f"[run]  {dataset:>4}  {policy_name}", file=sys.stderr, flush=True)
            results.append(run_one(dataset, csv_path, policy_name, factory))

    print()
    print("## Simulator Batch Results — Region2 Scaling Sweep")
    print()
    print(format_markdown(results))
    print()

    # Extra evaluation: 100k across three policies.
    extra_csv = INPUT_DIR / "region2_100000_simulator_input.csv"
    extra_results: list[RunResult] = []
    if extra_csv.exists():
        for policy_name, factory in EXTRA_100K_POLICIES:
            print(f"[run]  100k  {policy_name}  (extra)", file=sys.stderr, flush=True)
            extra_results.append(run_one("100k", extra_csv, policy_name, factory))

        print("## 100k Three-Way Policy Comparison")
        print()
        print(format_markdown(extra_results))
        print()

        per_func_csv = PROJECT_ROOT / "per_function_results_100k.csv"
        rows = write_per_function_csv(extra_results, per_func_csv)
        print(f"[csv]  wrote {rows:,} per-function rows to {per_func_csv}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
