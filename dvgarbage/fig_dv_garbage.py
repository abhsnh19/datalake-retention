"""Figure 7: DV garbage vs Puffin packing factor."""
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#dcdcd8"
plt.rcParams.update({
    "figure.dpi": 160, "savefig.dpi": 160, "font.size": 9,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "axes.titlecolor": INK,
    "xtick.color": INK2, "ytick.color": INK2, "text.color": INK,
    "axes.spines.top": False, "axes.spines.right": False,
    "grid.color": GRID, "grid.linewidth": 0.6, "legend.frameon": False,
    "figure.facecolor": "white", "axes.facecolor": "white",
})

R = json.load(open("dv_garbage_results.json"))
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9.2, 3.2))

# -- panel A: garbage vs packing, by reclaim policy (supersession regime) ----
for reclaim, color, label in (
    ("none",  ORANGE, "no reclaim (what Iceberg tooling does today)"),
    ("ideal", BLUE,   "ideal file-granular reclaimer"),
):
    pts = sorted(
        (r["params"]["packing"], r["final"]["garbage_ratio"])
        for r in R if r["regime"] == "supersession" and r["params"]["reclaim"] == reclaim
    )
    xs, ys = zip(*pts)
    ax1.plot(xs, ys, marker="o", markersize=5, linewidth=2, color=color,
             markeredgecolor="white", markeredgewidth=1.2, zorder=3, label=label)
    ax1.annotate(label.split(" (")[0], (xs[-1], ys[-1]), textcoords="offset points",
                 xytext=(-4, 10 if reclaim == "none" else -14), fontsize=8,
                 color=color, fontweight="bold", ha="right")

ax1.set_xscale("log", base=2)
ax1.set_ylim(-0.03, 1.0)
ax1.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0%}"))
ax1.set_xlabel("deletion vectors packed per Puffin file")
ax1.set_ylabel("dead bytes as share of Puffin bytes on disk")
ax1.set_title("Packing defeats file-granular reclamation", loc="left", pad=8)
ax1.grid(axis="y", alpha=0.7); ax1.set_axisbelow(True)

# -- panel B: the tradeoff it creates ---------------------------------------
pts = sorted(
    (r["params"]["packing"], r["final"]["puffin_files_on_disk"],
     r["final"]["garbage_ratio"])
    for r in R if r["regime"] == "supersession" and r["params"]["reclaim"] == "ideal"
)
files = [p[1] for p in pts]
garb = [p[2] for p in pts]
ax2.plot(files, garb, marker="o", markersize=6, linewidth=2, color=BLUE,
         markeredgecolor="white", markeredgewidth=1.2, zorder=3)
for (pack, f, g) in pts:
    ax2.annotate(f"{pack} DV/file", (f, g), textcoords="offset points",
                 xytext=(6, 6), fontsize=8, color=INK2)
ax2.set_xscale("log")
ax2.set_xlim(right=ax2.get_xlim()[1] * 1.9)
ax2.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0%}"))
ax2.set_ylim(-0.05, 0.85)
ax2.set_xlabel("Puffin files on disk (small-file pressure)")
ax2.set_ylabel("residual garbage")
ax2.set_title("The tradeoff: small files vs space amplification", loc="left", pad=8)
ax2.grid(axis="y", alpha=0.7); ax2.set_axisbelow(True)

fig.tight_layout()
fig.savefig("../figures/fig7_dv_garbage.png", bbox_inches="tight")
print("wrote ../figures/fig7_dv_garbage.png")
