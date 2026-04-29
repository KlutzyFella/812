"""Regenerate every Section-6 figure for the IEEE paper.

Reads the simulator's authoritative outputs, then writes seven PDFs into
``paper/figures/`` and one LaTeX booktabs table into ``paper/tables/``.

Run from project root:

    venv/bin/python scripts/make_figures.py

Idempotent: every invocation overwrites the same files with identical content
because the simulator is fully deterministic and matplotlib output is seeded
implicitly by stable input order.
"""
from __future__ import annotations

import contextlib
import io
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless rendering for CI/server runs
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.policies import Baseline30sPolicy, Baseline60sPolicy, TADKPolicy  # noqa: E402
from src.simulator import ServerlessSimulator  # noqa: E402

INPUT_DIR = PROJECT_ROOT / "OutputData" / "OutputData"
FIG_DIR = PROJECT_ROOT / "paper" / "figures"
TABLE_DIR = PROJECT_ROOT / "paper" / "tables"
PER_FUNC_CSV = PROJECT_ROOT / "per_function_results_100k.csv"

DATASETS = [
    ("20k", INPUT_DIR / "region2_20000_simulator_input.csv"),
    ("50k", INPUT_DIR / "region2_50000_simulator_input.csv"),
    ("100k", INPUT_DIR / "region2_100000_simulator_input.csv"),
]
POLICIES = [
    ("Baseline60s", lambda: Baseline60sPolicy()),
    ("Baseline30s", lambda: Baseline30sPolicy()),
    ("TADK", lambda: TADKPolicy(timer_interval=10.0, jitter_buffer=5.0)),
]

# Consistent palette across all figures.
COLORS = {"Baseline60s": "#1f77b4", "Baseline30s": "#ff7f0e", "TADK": "#2ca02c"}


@dataclass(frozen=True)
class Run:
    dataset: str
    policy: str
    total: int
    cold: int
    rate_pct: float
    idle_mbs: float


def _run(dataset: str, csv_path: Path, policy_name: str, factory) -> Run:
    sim = ServerlessSimulator(factory())
    sim.load_trace(str(csv_path))
    with contextlib.redirect_stdout(io.StringIO()):
        m = sim.run()
    rate = (m.total_cold_starts / m.total_invocations * 100.0) if m.total_invocations else 0.0
    return Run(
        dataset=dataset,
        policy=policy_name,
        total=m.total_invocations,
        cold=m.total_cold_starts,
        rate_pct=rate,
        idle_mbs=m.idle_memory_mb_seconds,
    )


def _classify_trigger(raw: str) -> str:
    """Map the trace's raw label set onto the simulator's binary class."""
    return "Timer" if "timer" in str(raw).lower() else "API"


def _style(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", linestyle=":", alpha=0.4)


def _save(fig: plt.Figure, name: str) -> None:
    out = FIG_DIR / name
    fig.tight_layout()
    fig.savefig(out, format="pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out.relative_to(PROJECT_ROOT)}")


# ---------------------------------------------------------------------------
# Figure 3 — cold-start count distribution per function (100k, three policies)
# ---------------------------------------------------------------------------
def fig3_coldstart_distribution(per_func: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(5.5, 3.0))
    bins = np.arange(0, 8) - 0.5
    for policy in ["Baseline60sPolicy", "Baseline30sPolicy", "TADKPolicy(10s)"]:
        sub = per_func[per_func["policy_name"] == policy]
        label = policy.replace("Policy", "").replace("(10s)", "")
        ax.hist(
            sub["total_cold_starts"],
            bins=bins,
            histtype="step",
            linewidth=1.6,
            label=label,
            color=COLORS.get(label, None),
        )
    ax.set_xlabel("Cold starts per function (100k trace)")
    ax.set_ylabel("Number of functions")
    ax.set_xticks(np.arange(0, 7))
    ax.legend(frameon=False)
    _style(ax)
    _save(fig, "fig3_coldstart_dist.pdf")


# ---------------------------------------------------------------------------
# Figure 4 — cold starts vs total invocations per function (TADK at 100k)
# ---------------------------------------------------------------------------
def fig4_coldstart_scatter(per_func: pd.DataFrame) -> None:
    sub = per_func[per_func["policy_name"] == "TADKPolicy(10s)"]
    fig, ax = plt.subplots(figsize=(5.5, 3.2))
    ax.scatter(
        sub["total_invocations"],
        sub["total_cold_starts"] + 0.05,  # nudge so 0-cold points are visible on log axis
        s=18,
        alpha=0.6,
        color=COLORS["TADK"],
        edgecolor="white",
        linewidth=0.4,
    )
    ax.set_xscale("log")
    ax.set_xlabel("Total invocations per function (log scale)")
    ax.set_ylabel("Cold starts per function")
    ax.set_title("Per-function cold-start cost under TADK (100k trace)", fontsize=10)
    _style(ax)
    _save(fig, "fig4_coldstart_scatter.pdf")


# ---------------------------------------------------------------------------
# Figure 5 — cold-start rate by trigger class, three policies
# ---------------------------------------------------------------------------
def fig5_trigger_breakdown(per_func: pd.DataFrame) -> None:
    df = per_func[per_func["dataset_size"] == "100k"].copy()
    df["class"] = df["trigger_type"].map(_classify_trigger)

    rows = []
    for policy_name in ["Baseline60sPolicy", "Baseline30sPolicy", "TADKPolicy(10s)"]:
        sub = df[df["policy_name"] == policy_name]
        for cls in ["API", "Timer"]:
            seg = sub[sub["class"] == cls]
            inv = int(seg["total_invocations"].sum())
            cold = int(seg["total_cold_starts"].sum())
            rate = (cold / inv * 100.0) if inv else 0.0
            rows.append((policy_name, cls, inv, cold, rate))
    bd = pd.DataFrame(rows, columns=["policy", "class", "inv", "cold", "rate_pct"])

    fig, ax = plt.subplots(figsize=(5.5, 3.2))
    width = 0.35
    policies = ["Baseline60sPolicy", "Baseline30sPolicy", "TADKPolicy(10s)"]
    pretty = ["Baseline60s", "Baseline30s", "TADK"]
    x = np.arange(len(policies))
    api_rates = [bd[(bd.policy == p) & (bd["class"] == "API")]["rate_pct"].iloc[0] for p in policies]
    timer_rates = [bd[(bd.policy == p) & (bd["class"] == "Timer")]["rate_pct"].iloc[0] for p in policies]

    ax.bar(x - width / 2, api_rates, width, label="API", color="#4c72b0")
    ax.bar(x + width / 2, timer_rates, width, label="Timer", color="#dd8452")
    for i, (a, t) in enumerate(zip(api_rates, timer_rates)):
        ax.text(i - width / 2, a + 0.01, f"{a:.2f}%", ha="center", va="bottom", fontsize=8)
        ax.text(i + width / 2, t + 0.01, f"{t:.2f}%", ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels(pretty)
    ax.set_ylabel("Cold-start rate (%)")
    ax.set_title("Cold-start rate by trigger class (100k trace)", fontsize=10)
    ax.legend(frameon=False, loc="upper left")
    _style(ax)
    _save(fig, "fig5_trigger_breakdown.pdf")


# ---------------------------------------------------------------------------
# Figure 6 — idle memory bar chart (100k, three policies)
# ---------------------------------------------------------------------------
def fig6_idle_memory_bar(runs_100k: dict[str, Run]) -> None:
    policies = ["Baseline60s", "Baseline30s", "TADK"]
    values = [runs_100k[p].idle_mbs / 1e6 for p in policies]
    fig, ax = plt.subplots(figsize=(4.6, 3.0))
    bars = ax.bar(policies, values, color=[COLORS[p] for p in policies])
    for b, v in zip(bars, values):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.05, f"{v:.2f} M", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("Idle memory (million MB$\\cdot$s)")
    ax.set_title("Total idle memory at 100k invocations", fontsize=10)
    _style(ax)
    _save(fig, "fig6_idle_memory_bar.pdf")


# ---------------------------------------------------------------------------
# Figure 7 — cold-start rate bar chart (100k, three policies)
# ---------------------------------------------------------------------------
def fig7_coldstart_rate_bar(runs_100k: dict[str, Run]) -> None:
    policies = ["Baseline60s", "Baseline30s", "TADK"]
    values = [runs_100k[p].rate_pct for p in policies]
    fig, ax = plt.subplots(figsize=(4.6, 3.0))
    bars = ax.bar(policies, values, color=[COLORS[p] for p in policies])
    for b, v in zip(bars, values):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.003, f"{v:.3f}%", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("Cold-start rate (%)")
    ax.set_title("Cold-start rate at 100k invocations", fontsize=10)
    ax.set_ylim(0, max(values) * 1.25)
    _style(ax)
    _save(fig, "fig7_coldstart_rate_bar.pdf")


# ---------------------------------------------------------------------------
# Scaling sweep table — booktabs LaTeX
# ---------------------------------------------------------------------------
def write_scaling_table(all_runs: list[Run]) -> None:
    by_key = {(r.dataset, r.policy): r for r in all_runs}
    lines = [
        r"% Auto-generated by scripts/make_figures.py — do not hand-edit.",
        r"\begin{table*}[t]",
        r"\centering",
        r"\caption{Scaling sweep of cold-start count and idle memory across three trace sizes. All policies are deterministic; numbers are the simulator's exact output.}",
        r"\label{tab:scaling}",
        r"\renewcommand{\arraystretch}{1.15}",
        r"\begin{tabular}{@{}llrrrr@{}}",
        r"\toprule",
        r"Trace & Policy & Invocations & Cold starts & Cold start \% & Idle memory (MB$\cdot$s) \\",
        r"\midrule",
    ]
    for size in ["20k", "50k", "100k"]:
        for policy in ["Baseline60s", "Baseline30s", "TADK"]:
            r = by_key.get((size, policy))
            if r is None:
                continue
            lines.append(
                f"{size} & {policy} & {r.total:,} & {r.cold:,} & {r.rate_pct:.3f} & {r.idle_mbs:,.0f} \\\\"
            )
        if size != "100k":
            lines.append(r"\addlinespace")
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table*}",
    ]
    out = TABLE_DIR / "scaling_sweep.tex"
    out.write_text("\n".join(lines) + "\n")
    print(f"  wrote {out.relative_to(PROJECT_ROOT)}")


def write_numbers_summary(all_runs: list[Run]) -> None:
    """Dump key numbers as plain text for cross-checking the prose."""
    out = PROJECT_ROOT / "paper" / "tables" / "numbers_summary.txt"
    by_key = {(r.dataset, r.policy): r for r in all_runs}
    r60 = by_key[("100k", "Baseline60s")]
    rT = by_key[("100k", "TADK")]
    pct_savings = (1.0 - rT.idle_mbs / r60.idle_mbs) * 100.0
    abs_savings = r60.idle_mbs - rT.idle_mbs
    out.write_text(
        f"100k Baseline60s: cold={r60.cold} rate={r60.rate_pct:.3f}% idle={r60.idle_mbs:,.0f} MB·s\n"
        f"100k TADK:        cold={rT.cold} rate={rT.rate_pct:.3f}% idle={rT.idle_mbs:,.0f} MB·s\n"
        f"Idle reduction:   {abs_savings:,.0f} MB·s ({pct_savings:.2f}%)\n"
    )
    print(f"  wrote {out.relative_to(PROJECT_ROOT)}")


def main() -> int:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    TABLE_DIR.mkdir(parents=True, exist_ok=True)

    if not PER_FUNC_CSV.exists():
        print(f"[error] expected per-function CSV at {PER_FUNC_CSV}", file=sys.stderr)
        return 1
    per_func = pd.read_csv(PER_FUNC_CSV)

    print("[runs] sweeping 3 sizes x 3 policies ...")
    all_runs: list[Run] = []
    runs_100k: dict[str, Run] = {}
    for size, csv_path in DATASETS:
        if not csv_path.exists():
            print(f"  [skip] {csv_path}")
            continue
        for policy_name, factory in POLICIES:
            r = _run(size, csv_path, policy_name, factory)
            all_runs.append(r)
            if size == "100k":
                runs_100k[policy_name] = r
            print(f"  {size:>4}  {policy_name:<12}  cold={r.cold:>4}  rate={r.rate_pct:6.3f}%  idle={r.idle_mbs:>14,.0f} MB.s")

    print("[figs] writing ...")
    fig3_coldstart_distribution(per_func)
    fig4_coldstart_scatter(per_func)
    fig5_trigger_breakdown(per_func)
    fig6_idle_memory_bar(runs_100k)
    fig7_coldstart_rate_bar(runs_100k)

    print("[tables] writing ...")
    write_scaling_table(all_runs)
    write_numbers_summary(all_runs)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
