# Trigger-Aware Dynamic Keep-Alive (TADK) — A Discrete-Event Simulator for Serverless Container Lifecycle Policies

A Python discrete-event simulator (DES) for evaluating keep-alive policies on
real serverless function traces. The simulator replays an invocation log
against a pluggable policy and reports cold-start counts, cold-start rate,
memory-weighted idle waste (MB·s), and per-function breakdowns suitable for
downstream statistical analysis.

## Abstract

Serverless platforms keep function containers warm for a fixed window after
each invocation (commonly **60 s**) to amortize cold-start latency. On
trigger-driven workloads (timers, schedulers) this static window is
systematically wasteful: a 60 s window is 6× longer than the inter-arrival
time of a 10 s timer, yet still leaves API-driven traffic exposed to cold
starts when bursts exceed the window.

**TADK** (Trigger-Aware Dynamic Keep-Alive) treats the keep-alive timeout as a
function of trigger metadata. TIMER invocations receive `timer_interval +
jitter_buffer` seconds — just long enough to bridge the next fire — while
API invocations receive a tighter 20 s window. On the Huawei Region 2 trace
at 100,000-event scale, TADK reduces aggregate idle-memory cost by **66.93 %**
versus the 60 s static baseline (7,073,280 → 2,339,200 MB·s) while holding
cold-start rate at a floor of **0.20 %** (203 / 100,000). The simulator,
the three policies, the canonical input traces, and the regression test
suite are all in this repository — every number above is reproducible with
two commands.

| Dataset | Policy | Cold Starts | Cold-Start Rate | Idle Memory (MB·s) |
|---|---|---:|---:|---:|
| 100k | `Baseline60sPolicy` | 181 | 0.18 % | 7,073,280 |
| 100k | `Baseline30sPolicy` | 194 | 0.19 % | 3,598,080 |
| 100k | `TADKPolicy(10 s)` | **203** | **0.20 %** | **2,339,200** |

---

## 1. Environment Setup

The simulator targets **Python ≥ 3.10**. The hard floor is set by `enum.StrEnum`
(introduced in 3.10) used in `src/models.py` for the `EventType` and
`TriggerType` enums. Earlier interpreters fail at import time.

```bash
# 1. Verify interpreter
python3 --version          # must be >= 3.10

# 2. Create and activate a clean virtual environment
python3 -m venv venv
source venv/bin/activate                     # POSIX
# .\venv\Scripts\activate                    # Windows PowerShell

# 3. Install runtime + test dependencies
pip install -r requirements.txt
pip install pytest                           # used by the regression suite
```

`requirements.txt` lists only `pandas`. The simulator's hot path uses the
standard library exclusively (`heapq`, `dataclasses`, `enum`, `itertools`);
pandas is only used to ingest the input trace.

---

## 2. Data Pipeline (Reproducibility Step 1)

### 2.1 Simulator input schema

The simulator consumes a single CSV with the following five columns. All are
required; column order is irrelevant. Loading fails fast on missing columns.

| Column | Type | Description |
|---|---|---|
| `event_time` | float | Invocation arrival time, seconds from a monotonic origin. |
| `func_id` | str | Function identifier. The trailing `-`-separated token encodes memory in MB (e.g. `1118---631---pool24-600-512` → 512 MB). |
| `trigger_type` | str | Free-form label. Any case-insensitive substring match of `"timer"` is mapped to `TriggerType.TIMER`; everything else to `TriggerType.API`. |
| `exec_time` | float | Execution duration in seconds. |
| `cold_start_flag` | int / str | Ground-truth cold-start indicator. Accepts `0`/`1`, `"true"`/`"false"`, `"yes"`/`"no"`, etc. (see `src/simulator.py::_parse_cold_start_flag`). |

### 2.2 Pre-formatted Region 2 traces (recommended)

Three pre-processed traces ship under `OutputData/OutputData/`:

```
region2_20000_simulator_input.csv      # 20 k events
region2_50000_simulator_input.csv      # 50 k events
region2_100000_simulator_input.csv     # 100 k events  (headline result set)
```

These are the inputs `scripts/batch_run.py` consumes by default — no
preprocessing required.

### 2.3 Building inputs from the raw Huawei dump

To regenerate simulator inputs from the raw Huawei Public Function Trace:

- **`scripts/preprocess.py`** — joins the raw request trace, the per-function
  metadata (`df_funcID_runtime_triggerType.csv`), and the day-30 cold-start
  dataset; emits `data/region2_sample_preprocessed.csv`. Edit the path
  constants at the top of the file to point at your local copy of the Huawei
  archive.
- **`scripts/preprocess_region2 1.py`** — argparse-driven variant that
  supports multiple sample sizes (`--rows 20000 / 50000 / 100000`) and
  writes into the configured `OUTPUT_DIR`. The path constants are currently
  Windows-style; adjust to your platform before running.

`scripts/generate_dummy_trace.py` emits a small synthetic trace useful for
sanity checks without touching the Huawei archive.

---

## 3. Execution & Reproduction (Reproducibility Step 2)

### 3.1 Verify mathematical invariants

```bash
python -m pytest tests/ -v
```

Expected output: **3 passed**. The suite (`tests/test_simulator.py`)
verifies:

1. `total_cold_starts ≤ total_invocations` (counter sanity).
2. `idle_memory_mb_seconds ≥ 0` (sign sanity).
3. **Chronological determinism + hand-computed values.** A fixed three-event
   fixture trace (one function, 512 MB, executions at *t* = 0 / 30 / 200 s,
   each 1 s long) under `Baseline60sPolicy` must produce exactly
   `total_invocations = 3`, `total_cold_starts = 2`, and
   `idle_memory_mb_seconds = 61,440`. The same trace fed in a Fisher-Yates
   shuffled order must return identical metrics — this guards against any
   regression in event ordering or stale-timeout discipline.

### 3.2 Reproduce the 100 k three-way comparison

```bash
python scripts/batch_run.py
```

This runs the full sweep (20 k / 50 k / 100 k × `Baseline60sPolicy` +
`TADKPolicy(10 s)`), then a 100 k three-way comparison across
`Baseline60sPolicy`, `Baseline30sPolicy`, and
`TADKPolicy(timer_interval=10.0, jitter_buffer=5.0)`. Two Markdown tables
print to stdout. After the three-way table, a per-function breakdown is
flattened into:

```
per_function_results_100k.csv
```

with the following exact schema (one row per `(policy, func_id)` pair —
540 rows for three policies × 180 unique functions):

```
dataset_size,policy_name,func_id,trigger_type,total_invocations,total_cold_starts,total_idle_memory_mbs
```

Per-policy column sums in this CSV equal the aggregate-table values exactly;
this is the test harness for downstream statistical figures.

### 3.3 Smoke test

```bash
python -m src.simulator
```

Runs `Baseline60sPolicy` and `TADKPolicy(timer_interval=300.0)` against
`data/region2_simulator_input.csv` and prints metrics directly. Useful for a
30-second confirmation that a fresh checkout works.

---

## 4. System Architecture

### 4.1 Discrete-event loop

`src/simulator.py::ServerlessSimulator.run()` consumes events from a single
priority queue in chronological order. Two event types circulate:

- **`INVOCATION`** — sourced from the input trace; updates aggregate and
  per-function counters, refreshes the warm-pod table for `func_id`, and
  schedules a corresponding `TIMEOUT`.
- **`TIMEOUT`** — synthesized at
  `event.timestamp + duration + policy.get_timeout(...)`; on dispatch,
  removes the warm pod from `_active_pods` and accumulates idle MB·s
  weighted by the pod's parsed memory tier.

### 4.2 Lock-free `heapq` priority queue

The event store is a plain Python list managed by the `heapq` module —
**not** `queue.PriorityQueue`. Because the simulator is single-threaded
and deterministic, the lock overhead of `queue.PriorityQueue` is pure tax;
`heapq` provides the same `O(log n)` push/pop on the heap-ordered list with
no synchronization cost. Items are 3-tuples `(timestamp, sequence, event)`,
where `sequence` is a monotonic counter (`itertools.count`) that breaks
ties between events sharing a timestamp, guaranteeing FIFO behavior at
equal-time without depending on the natural ordering of the `Event`
dataclass.

### 4.3 Stale-timeout discipline

When an invocation re-uses a warm pod, the previously scheduled `TIMEOUT`
becomes obsolete (the pod's last_active has advanced). The simulator
resolves this lazily — rather than re-heapifying — by storing the
latest scheduled timeout per `func_id` in `_pending_timeouts: dict[str, float]`.
On each `TIMEOUT` dispatch, the simulator compares the popped event's
timestamp to `_pending_timeouts[fid]`; mismatches are silently discarded as
stale. This produces correctness in `O(1)` per stale event.

### 4.4 Virtual clock

`_virtual_clock: float` advances monotonically with each dispatched event's
timestamp and provides the `now` reference for any policy or instrument
that needs it. Because it is driven by event timestamps rather than wall
time, the simulator is **fully deterministic** — independent of host CPU
or scheduler — and 100 k events run in well under one second.

### 4.5 Pluggable policy interface

`src/policies.py::KeepAlivePolicy` is an `abc.ABC` with one method:

```python
def get_timeout(self, function_id: str, trigger_type: str, **kwargs) -> float
```

Three implementations ship in-tree:

| Policy | API timeout | TIMER timeout | Notes |
|---|---|---|---|
| `Baseline60sPolicy` | 60 s | 60 s | Static; ignores trigger type. The industry default. |
| `Baseline30sPolicy` | 30 s | 30 s | Static; tighter budget. |
| `TADKPolicy(timer_interval, jitter_buffer=5.0)` | 20 s | `timer_interval + jitter_buffer` | Trigger-aware. Bridges the next timer fire with a small jitter cushion. |

To add a policy, subclass `KeepAlivePolicy` and implement `get_timeout`. No
changes to the simulator are required.

### 4.6 Outputs (`SimulationMetrics`)

`run()` returns a frozen `SimulationMetrics` dataclass (`src/models.py`)
containing the aggregates (`total_invocations`, `total_cold_starts`,
`cold_start_rate`, `idle_memory_mb_seconds`, `baseline_accuracy`,
`baseline_accuracy_matches`) and a per-function breakdown:

```python
per_function_stats: dict[str, dict]   # func_id -> {
                                       #   "trigger_type": str,
                                       #   "total_invocations": int,
                                       #   "total_cold_starts": int,
                                       #   "total_idle_memory_mbs": float,
                                       # }
```

---

## 5. Project Layout

```
812/
├── README.md
├── requirements.txt
├── per_function_results_100k.csv         generated by scripts/batch_run.py
├── src/
│   ├── __init__.py
│   ├── models.py                         Event, Pod, SimulationMetrics, EventType, TriggerType
│   ├── policies.py                       KeepAlivePolicy ABC + Baseline60s / Baseline30s / TADK
│   └── simulator.py                      ServerlessSimulator — DES event loop + metric accounting
├── scripts/
│   ├── batch_run.py                      Sweep input sizes × policies; emit Markdown + per-fn CSV
│   ├── preprocess.py                     Build a Region 2 trace (Linux paths)
│   ├── preprocess_region2 1.py           Multi-size Region 2 builder (argparse, --rows)
│   ├── generate_dummy_trace.py           Synthetic trace for sanity checks
│   └── checkData 1.py                    Trace inspection helper
├── tests/
│   ├── __init__.py
│   └── test_simulator.py                 3 regression invariants (sanity + determinism)
├── data/
│   ├── region2_simulator_input.csv       Canonical small input
│   ├── df_funcID_runtime_triggerType.csv Function metadata used by preprocess.py
│   └── dummy_trace.csv
└── OutputData/
    └── OutputData/
        ├── region2_20000_simulator_input.csv
        ├── region2_50000_simulator_input.csv
        └── region2_100000_simulator_input.csv
```

---

## 6. Reproducibility Checklist

A reviewer should be able to produce every headline number in this README by
executing, from a clean checkout:

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt pytest
python -m pytest tests/ -v                # expect: 3 passed
python scripts/batch_run.py               # expect: matches §1 abstract numbers
```

If the test suite passes and `batch_run.py` prints the exact 100 k row
shown in §1 (181 / 194 / 203 cold starts and 7,073,280 / 3,598,080 /
2,339,200 MB·s respectively), the simulator is faithfully reproducing the
published experiment.
