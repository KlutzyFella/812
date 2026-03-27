"""Generate a synthetic workload trace and save it to data/dummy_trace.csv.

The trace contains 1,000 rows:
- ~500 TIMER rows: func_timer_1, perfectly spaced every 300 s.
- ~500 API rows:   func_api_1..5, Poisson arrivals (mean 60 s inter-arrival).

Run from the project root:
    python scripts/generate_dummy_trace.py
"""

import os
import random

import pandas as pd

SEED = 42
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "dummy_trace.csv")


def generate_timer_rows(n: int) -> pd.DataFrame:
    """Perfectly spaced TIMER invocations for a single function."""
    timestamps = [i * 300.0 for i in range(n)]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "function_id": "func_timer_1",
            "trigger_type": "TIMER",
            "execution_duration": [random.uniform(0.5, 3.0) for _ in range(n)],
        }
    )


def generate_api_rows(n: int) -> pd.DataFrame:
    """Poisson-arrival API invocations spread across 5 functions."""
    functions = [f"func_api_{i}" for i in range(1, 6)]
    # Exponential inter-arrivals with mean=60s give Poisson arrivals.
    inter_arrivals = [random.expovariate(1 / 60.0) for _ in range(n)]
    timestamps = []
    t = 0.0
    for ia in inter_arrivals:
        t += ia
        timestamps.append(t)

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "function_id": [random.choice(functions) for _ in range(n)],
            "trigger_type": "API",
            "execution_duration": [random.uniform(0.1, 5.0) for _ in range(n)],
        }
    )


def main() -> None:
    random.seed(SEED)

    timer_rows = generate_timer_rows(500)
    api_rows = generate_api_rows(500)

    trace = (
        pd.concat([timer_rows, api_rows], ignore_index=True)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    out = os.path.abspath(OUTPUT_PATH)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    trace.to_csv(out, index=False)
    print(f"Wrote {len(trace)} rows to {out}")


if __name__ == "__main__":
    main()
