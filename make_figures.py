#!/usr/bin/env python3
"""Generate the paper's figures from results/.

Print-oriented: a single committed light look, thin marks, recessive axes,
direct labels, and a legend whenever more than one series is present. No
dual-axis charts -- two measures of different scale get two panels.

    python3 make_figures.py
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

RESULTS = Path(__file__).parent / "results"
FIGS = Path(__file__).parent / "figures"

# Validated categorical palette (see dataviz validator: all checks pass).
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#dcdcd8"
POLICY_COLOR = {"baseline": BLUE, "deadline": ORANGE}
POLICY_LABEL = {"baseline": "Baseline (cadence-driven)", "deadline": "Deadline-aware"}

plt.rcParams.update(
    {
        "figure.dpi": 160,
        "savefig.dpi": 160,
        "font.size": 9,
        "axes.titlesize": 10,
        "axes.labelsize": 9,
        "axes.edgecolor": GRID,
        "axes.labelcolor": INK2,
        "axes.titlecolor": INK,
        "xtick.color": INK2,
        "ytick.color": INK2,
        "text.color": INK,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "grid.color": GRID,
        "grid.linewidth": 0.6,
        "legend.frameon": False,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
    }
)


def load() -> list[dict]:
    out = []
    for p in sorted(RESULTS.glob("*.json")):
        if p.name == "combined.json":
            continue
        try:
            out.append(json.loads(p.read_text()))
        except Exception:
            pass
    return out


def sel(runs, **kw):
    res = []
    for r in runs:
        c = r["config"]
        if all(c.get(k) == v for k, v in kw.items()):
            res.append(r)
    return res


def _grid(ax, axis="y"):
    ax.grid(axis=axis, alpha=0.7, zorder=0)
    ax.set_axisbelow(True)


# ---------------------------------------------------------------- figure 1
def fig_stage_decomposition(runs) -> None:
    """Where residency actually accrues. The headline figure."""
    rows = []
    for pol in ("baseline", "deadline"):
        for mode in ("cow", "mor"):
            r = sel(runs, name="E1" if pol == "baseline" else "E2",
                    policy=pol, delete_mode=mode, layout="scattered")
            if r:
                rows.append((f"{POLICY_LABEL[pol].split(' (')[0]}\n{mode.upper()}",
                             r[0]["summary"]))
    if not rows:
        return

    fig, ax = plt.subplots(figsize=(7.2, 0.75 * len(rows) + 1.9))
    labels = [r[0] for r in rows]
    y = range(len(rows))

    # The `logical` stage completes on day 0 in every configuration -- the row
    # is reported deleted immediately. Plotting a zero-width segment would put
    # a colour in the legend that never appears on the chart, so it is called
    # out as an annotation instead. That it is always zero IS the point.
    stages = [
        ("median_days_to_rewritten", "gone from current snapshot", AQUA),
        ("median_days_to_unreferenced", "released by time travel", YELLOW),
        ("median_days_to_physical", "bytes actually erased", ORANGE),
    ]

    prev = [0.0] * len(rows)
    for key, label, color in stages:
        vals = [float(r[1].get(key) or 0) for r in rows]
        widths = [max(0.0, v - p) for v, p in zip(vals, prev)]
        ax.barh(list(y), widths, left=prev, height=0.55, color=color,
                label=label, edgecolor="white", linewidth=1.6, zorder=3)
        prev = vals

    for i, (_, s) in enumerate(rows):
        total = float(s.get("median_days_to_physical") or 0)
        ax.text(total + 0.7, i, f"{total:.0f}d", va="center", ha="left",
                fontsize=9, color=INK, fontweight="bold")

    ax.axvline(30, color=INK2, linestyle=(0, (4, 3)), linewidth=1.2, zorder=4)
    ax.text(30, -0.82, "30-day window", fontsize=8, color=INK2,
            va="bottom", ha="center")
    ax.text(0, -0.82, "reported deleted\non day 0", fontsize=8, color=INK2,
            va="bottom", ha="left")

    ax.set_yticks(list(y), labels)
    ax.set_ylim(len(rows) - 0.4, -1.25)  # inverted, with headroom for the labels
    ax.set_xlabel("median days after the erasure request", labelpad=8)
    ax.set_title("Deletion residency accrues downstream of the delete itself",
                 loc="left", pad=26)
    _grid(ax, "x")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.30), ncol=4,
              fontsize=8, columnspacing=1.4, handlelength=1.4)
    fig.tight_layout()
    fig.savefig(FIGS / "fig1_stage_decomposition.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- figure 2
def fig_drt_cdf(runs) -> None:
    """Distribution of deletion residency time, by policy."""
    fig, ax = plt.subplots(figsize=(4.6, 3.0))
    plotted = False
    for pol, name in (("baseline", "E1"), ("deadline", "E2")):
        r = sel(runs, name=name, policy=pol, delete_mode="mor", layout="scattered")
        if not r:
            continue
        obs = r[0]["obligations"]
        cfg = r[0]["config"]
        lo, hi = cfg["warmup_days"], cfg["horizon_days"] - cfg["tail_days"]
        drts = sorted(
            o["physical"] - o["issued"]
            for o in obs
            if o["physical"] is not None and lo <= o["issued"] <= hi
        )
        if not drts:
            continue
        ys = [(i + 1) / len(drts) for i in range(len(drts))]
        ax.step(drts, ys, where="post", color=POLICY_COLOR[pol], linewidth=2,
                label=POLICY_LABEL[pol], zorder=3)
        # Direct label beats a legend box at two series.
        ax.annotate(
            POLICY_LABEL[pol].split(" (")[0],
            (drts[len(drts) // 2], 0.5),
            textcoords="offset points",
            xytext=(8, -4) if pol == "deadline" else (8, 6),
            fontsize=8, color=POLICY_COLOR[pol], fontweight="bold",
        )
        plotted = True

    if not plotted:
        plt.close(fig)
        return

    ax.axvline(30, color=INK2, linestyle=(0, (4, 3)), linewidth=1.2, zorder=2)
    ax.text(30, 1.04, "30-day window", fontsize=8, color=INK2, ha="center")
    ax.set_xlabel("deletion residency time (days)")
    ax.set_ylabel("fraction of erasure requests")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_ylim(0, 1.02)
    ax.set_title("Residency distribution", loc="left", pad=16)
    _grid(ax)
    fig.tight_layout()
    fig.savefig(FIGS / "fig2_drt_cdf.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- figure 3
def fig_tradeoff(runs) -> None:
    """Cost against residency -- the tradeoff curve reviewers look for."""
    fig, ax = plt.subplots(figsize=(4.6, 3.0))
    seen = set()
    for r in runs:
        c, s = r["config"], r["summary"]
        if c["name"] not in ("E1", "E2") or not s.get("drt_days"):
            continue
        pol = c["policy"]
        ax.scatter(
            s["bytes_rewritten"] / 1e6,
            s["drt_days"]["p95"],
            s=46,
            color=POLICY_COLOR[pol],
            edgecolor="white",
            linewidth=1.4,
            zorder=3,
            label=POLICY_LABEL[pol] if pol not in seen else None,
        )
        seen.add(pol)
    if not seen:
        plt.close(fig)
        return
    ax.axhline(30, color=INK2, linestyle=(0, (4, 3)), linewidth=1.2, zorder=2)
    ax.text(ax.get_xlim()[0], 30.6, " 30-day window", fontsize=8, color=INK2)
    ax.set_xlabel("bytes rewritten over the run (MB)")
    ax.set_ylabel("p95 residency (days)")
    ax.set_title("Buying residency with rewrite cost", loc="left", pad=8)
    _grid(ax)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.26), ncol=2, fontsize=8)
    fig.tight_layout()
    fig.savefig(FIGS / "fig3_tradeoff.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- figure 4
def fig_sensitivity(runs) -> None:
    """Does the effect survive the parameter space? The synthetic-workload defence."""
    panels = [
        ("S-retention", "retention_days", "time-travel retention (days)"),
        ("S-orphan", "orphan_interval_days", "orphan cleanup interval (days)"),
        ("S-alpha", "subject_alpha", "subject skew (power-law alpha)"),
        ("S-demand", "dsar_rate_per_day", "erasure requests per day"),
    ]
    fig, axes = plt.subplots(1, 4, figsize=(11.5, 2.7))
    any_data = False
    for ax, (name, param, xlabel) in zip(axes, panels):
        for pol in ("baseline", "deadline"):
            pts = []
            for r in sel(runs, name=name, policy=pol):
                s = r["summary"]
                if s.get("drt_days"):
                    pts.append((r["config"][param], s["drt_days"]["p95"]))
            if not pts:
                continue
            pts.sort()
            xs, ys = zip(*pts)
            ax.plot(xs, ys, marker="o", markersize=5, linewidth=2,
                    color=POLICY_COLOR[pol], label=POLICY_LABEL[pol],
                    markeredgecolor="white", markeredgewidth=1.2, zorder=3)
            any_data = True
        ax.axhline(30, color=INK2, linestyle=(0, (4, 3)), linewidth=1.0, zorder=2)
        ax.set_xlabel(xlabel)
        _grid(ax)
    if not any_data:
        plt.close(fig)
        return
    axes[0].set_ylabel("p95 residency (days)")
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2,
                   bbox_to_anchor=(0.5, 1.10), fontsize=9)
    fig.suptitle("")
    fig.tight_layout()
    fig.savefig(FIGS / "fig4_sensitivity.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- figure 5
def fig_buckets(runs) -> None:
    """Bucket count vs amplification, at two levels of erasure demand."""
    rs = sel(runs, name="S-buckets")
    if not rs:
        return
    fig, ax = plt.subplots(figsize=(4.6, 3.0))
    for color, demand, label in ((BLUE, 2.0, "2 requests/day"),
                                 (ORANGE, 10.0, "10 requests/day")):
        pts = [
            (r["config"]["n_buckets"], r["summary"].get("deletion_amplification"))
            for r in rs
            if r["config"]["dsar_rate_per_day"] == demand
            and r["summary"].get("deletion_amplification")
        ]
        if not pts:
            continue
        pts.sort()
        xs, ys = zip(*pts)
        ax.plot(xs, ys, marker="o", markersize=5, linewidth=2, color=color,
                label=label, markeredgecolor="white", markeredgewidth=1.2, zorder=3)
        ax.annotate(label, (xs[-1], ys[-1]), textcoords="offset points",
                    xytext=(6, 0), fontsize=8, color=color, va="center")
    ax.set_xscale("log", base=2)
    ax.set_xlim(right=ax.get_xlim()[1] * 3.2)  # room for the direct labels
    ax.set_xlabel("buckets on the erasure key")
    ax.set_ylabel("deletion amplification (x)")
    ax.set_title("Partitioning only prunes while a batch\ntouches few buckets",
                 loc="left", pad=8)
    _grid(ax)
    fig.tight_layout()
    fig.savefig(FIGS / "fig5_buckets.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- figure 6
def fig_timeline(runs) -> None:
    """Overdue obligations over time -- the operational view."""
    fig, ax = plt.subplots(figsize=(6.4, 2.8))
    plotted = False
    for pol, name in (("baseline", "E1"), ("deadline", "E2")):
        r = sel(runs, name=name, policy=pol, delete_mode="mor", layout="scattered")
        if not r:
            continue
        tl = r[0]["timeline"]
        ax.plot([d["day"] for d in tl], [d["overdue_obligations"] for d in tl],
                color=POLICY_COLOR[pol], linewidth=2, label=POLICY_LABEL[pol],
                zorder=3)
        plotted = True
    if not plotted:
        plt.close(fig)
        return
    ax.set_xlabel("simulated day")
    ax.set_ylabel("erasure requests past deadline")
    ax.set_title("Backlog of overdue erasure obligations", loc="left", pad=8)
    _grid(ax)
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    fig.savefig(FIGS / "fig6_backlog.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- figure 8
def fig_objstore(runs) -> None:
    """Stage 5: what the object store adds, and where it defeats the scheduler."""
    rs = sel(runs, name="O-store")
    if not rs:
        return
    fig, ax = plt.subplots(figsize=(5.4, 3.4))
    for pol in ("baseline", "deadline"):
        pts = []
        for r in rs:
            c, s_ = r["config"], r["summary"]
            if c["policy"] != pol or c["object_store"] != "versioned":
                continue
            pts.append((c["noncurrent_expiration_days"], s_["drt_days"]["p50"]))
        if not pts:
            continue
        pts.sort()
        xs, ys = zip(*pts)
        ax.plot(xs, ys, marker="o", markersize=5, linewidth=2,
                color=POLICY_COLOR[pol], markeredgecolor="white",
                markeredgewidth=1.2, zorder=3, label=POLICY_LABEL[pol])
        ax.annotate(POLICY_LABEL[pol].split(" (")[0], (xs[-1], ys[-1]),
                    textcoords="offset points", xytext=(-6, 8),
                    fontsize=8, color=POLICY_COLOR[pol], fontweight="bold",
                    ha="right")

    ax.axhline(30, color=INK2, linestyle=(0, (4, 3)), linewidth=1.2, zorder=2)
    ax.text(29.5, 31.2, "30-day window", fontsize=8, color=INK2, ha="right")

    # The documented default, and where the scheduler stops coping.
    ax.axvline(7, color=GRID, linewidth=8, zorder=1)
    ax.text(7, 64, "provider\ndefault", fontsize=7.5, color=INK2,
            ha="center", va="top")
    ax.set_ylim(15, 68)

    ax.set_xlabel("noncurrent-version expiration $D_{nc}$ (days)")
    ax.set_ylabel("median residency (days)")
    ax.set_title("Bucket versioning adds to residency, additively",
                 loc="left", pad=8)
    _grid(ax)
    fig.tight_layout()
    fig.savefig(FIGS / "fig8_objstore.png", bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------- figure 9
def fig_budget(runs) -> None:
    """Compliance against rewrite budget -- the fairness result, now enforceable."""
    rs = sel(runs, name="V-budget")
    if not rs:
        return
    fig, ax = plt.subplots(figsize=(5.4, 3.3))
    for pol in ("baseline", "deadline"):
        pts = []
        for r in rs:
            if r["config"]["policy"] != pol:
                continue
            s_ = r["summary"]
            censored = s_["n_censored"] / max(1, s_["n_obligations"])
            pts.append((r["config"]["daily_rewrite_budget_bytes"],
                        100 * s_["deadline_met_fraction"], censored))
        if not pts:
            continue
        pts.sort()
        xs = [p[0] / 1000 for p in pts]
        ys = [p[1] for p in pts]
        ax.plot(xs, ys, marker="o", markersize=5, linewidth=2,
                color=POLICY_COLOR[pol], markeredgecolor="white",
                markeredgewidth=1.2, zorder=3)
        ax.annotate(POLICY_LABEL[pol].split(" (")[0], (xs[-1], ys[-1]),
                    textcoords="offset points", xytext=(-6, 9), fontsize=8,
                    color=POLICY_COLOR[pol], fontweight="bold", ha="right")

    ax.set_xscale("log")
    ax.set_ylim(-4, 108)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_xlabel("daily rewrite budget (KB, log scale)")
    ax.set_ylabel("erasure requests meeting the 30-day window")
    ax.set_title("The mechanism holds full compliance on 1/80th the budget",
                 loc="left", pad=8)
    _grid(ax)
    fig.tight_layout()
    fig.savefig(FIGS / "fig9_budget.png", bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    FIGS.mkdir(exist_ok=True)
    runs = load()
    if not runs:
        print("no results found -- run run_experiments.py first")
        return
    print(f"loaded {len(runs)} runs")
    for fn in (fig_stage_decomposition, fig_drt_cdf, fig_tradeoff,
               fig_sensitivity, fig_buckets, fig_timeline, fig_objstore, fig_budget):
        try:
            fn(runs)
            print(f"  {fn.__name__}")
        except Exception as e:  # keep going; a missing suite shouldn't block
            print(f"  {fn.__name__} SKIPPED: {e}")
    print(f"figures -> {FIGS}")


if __name__ == "__main__":
    main()
