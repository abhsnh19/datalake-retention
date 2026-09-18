"""Reconcile the two decks against the two result sets.

Reads lhbench's ``results/*.json`` and the erasure harness's ``harness.json``
as data (the RNGs differ, so neither is re-derived here), normalises both to
one row shape, and prints the claims each deck makes with the number each
dataset actually supports.  Verdicts:

  agree        same claim, same physics, both datasets support it
  operating    same physics, different operating point -- state the
               configuration on the slide and both numbers are right
  stale        a number on a slide that its own dataset no longer produces
  conflict     the two decks say different things about the same quantity
"""
from __future__ import annotations

import glob
import json
import os
import statistics


def load_lhbench(results_dir: str) -> list[dict]:
    rows = []
    for p in sorted(glob.glob(os.path.join(results_dir, "*.json"))):
        if os.path.basename(p) == "combined.json":
            continue
        with open(p) as fh:
            r = json.load(fh)
        c, s = r["config"], r["summary"]
        if not s.get("drt_days"):
            continue
        rows.append({"source": "lhbench", "tag": r.get("tag", os.path.basename(p)), "suite": c.get("name"),
                     "policy": c["policy"], "layout": c["layout"], "delete_mode": c["delete_mode"],
                     "retention": c["retention_days"], "orphan_interval": c["orphan_interval_days"],
                     "expire_interval": c["expire_interval_days"],
                     "store": c.get("object_store", "none"), "noncurrent_days": c.get("noncurrent_expiration_days"),
                     "dirty_threshold": c.get("dirty_threshold"), "budget": c.get("daily_rewrite_budget_bytes"),
                     "horizon": c["horizon_days"], "tail": c["tail_days"], "seed": c["seed"],
                     "p50": s["drt_days"]["p50"], "p95": s["drt_days"]["p95"], "met": s["deadline_met_fraction"],
                     "to_rewritten": s["median_days_to_rewritten"], "to_unreferenced": s["median_days_to_unreferenced"],
                     "to_marker": s.get("median_days_to_delete_marker"), "to_physical": s["median_days_to_physical"],
                     "bytes": s["bytes_rewritten"], "amp": s.get("deletion_amplification"),
                     "n": s["n_obligations"], "censored": s["n_censored"]})
    return rows


def load_erasure(harness_json: str) -> dict:
    with open(harness_json) as fh:
        h = json.load(fh)
    groups = {(g["policy"], g["store"]["label"]): g for g in h["groups"]}
    return {"raw": h, "groups": groups, "runs": h["runs"]}


def _lh(rows, **match):
    out = [r for r in rows if all(r.get(k) == v for k, v in match.items())]
    return out


def claims(lh_rows: list[dict], er: dict, model: dict | None = None) -> list[dict]:
    G = er["groups"]
    n7 = "S3 · versioned + NoncurrentDays 7"
    A, B, C, D, E = (G[(p, n7)] for p in ("A-default", "B-compactor-only", "C-coordinated", "D-cow-default", "E-asap-daily"))
    e1m = _lh(lh_rows, suite="E1", layout="scattered", delete_mode="mor")[0]
    e1c = _lh(lh_rows, suite="E1", layout="scattered", delete_mode="cow")[0]
    e2m = _lh(lh_rows, suite="E2", layout="scattered", delete_mode="mor")[0]
    o_none_b = _lh(lh_rows, suite="O-store", policy="baseline", store="none")[0]
    o_v7_b = _lh(lh_rows, suite="O-store", policy="baseline", store="versioned", noncurrent_days=7)[0]
    o_v7_d = _lh(lh_rows, suite="O-store", policy="deadline", store="versioned", noncurrent_days=7)[0]
    o_v10_b = _lh(lh_rows, suite="O-store", policy="baseline", store="versioned", noncurrent_days=10)[0]
    seeds_b = _lh(lh_rows, suite="V-seed", policy="baseline")
    seeds_d = _lh(lh_rows, suite="V-seed", policy="deadline")
    dirty = {r["dirty_threshold"]: r for r in _lh(lh_rows, suite="V-dirty")}
    bud_b = {r["budget"]: r for r in _lh(lh_rows, suite="V-budget", policy="baseline")}
    bud_d = {r["budget"]: r for r in _lh(lh_rows, suite="V-budget", policy="deadline")}
    unv = next(r for r in er["runs"] if r["policy"] == "A-default" and r["store"]["mode"] == "unversioned")
    a_st = A["stage_mean_days"]

    out = []
    out.append({"claim": "Headline residency, cadence-driven maintenance, against a 30-day window",
                "lhbench": f"{e1m['p50']} d median (E1, retention 20 d, orphan job 15 d, weekly compaction/expiry, "
                           f"dirty threshold {e1m['dirty_threshold']}, 100-day horizon); {o_none_b['p50']} d for the same "
                           f"configuration on a 160-day horizon (O-store); seeds at threshold 0.0: "
                           f"{[r['p50'] for r in seeds_b]}",
                "erasure": f"{A['median_days_mean']:.1f} d median (A-default, retention 5 d, weekly window + monthly full, "
                           f"NoncurrentDays 7, 5 seeds: {A['median_days_min']:.1f}-{A['median_days_max']:.1f}); "
                           f"{unv['days']['median']:.1f} d on an unversioned bucket",
                "verdict": "operating",
                "note": "Same physics. lhbench's days sit in snapshot retention (20 d); the erasure harness's in the "
                        "compaction calendar (5 d retention, lazy weekly window). Put the operating point on the slide; "
                        "the combined harness runs both as presets (lhbench-baseline, A-default)."})
    out.append({"claim": "Where the days go (stage decomposition)",
                "lhbench": f"rewrite {e1m['to_rewritten']} / expiry {e1m['to_unreferenced'] - e1m['to_rewritten']} / "
                           f"orphan {e1m['to_physical'] - e1m['to_unreferenced']} = {e1m['to_physical']} d (E1 medians to stage); "
                           f"{o_none_b['to_rewritten']:.0f} / {o_none_b['to_unreferenced'] - o_none_b['to_rewritten']:.0f} / "
                           f"{o_none_b['to_physical'] - o_none_b['to_unreferenced']:.0f} = {o_none_b['to_physical']:.0f} d (O-store)",
                "erasure": f"commit {a_st['commit']:.1f} / rewrite {a_st['rewrite']:.1f} / unlink {a_st['unlink']:.1f} / "
                           f"bucket {a_st['expire']:.1f} d (A-default stage means)",
                "verdict": "operating",
                "note": "Two decompositions of the same chain: lhbench splits expiry from orphan cleanup because its "
                        "expiry rewrites metadata only (PyIceberg native) and the orphan job reclaims bytes; the erasure "
                        "harness applies the Java action (expiry deletes what it unreferences), so orphan cleanup removed 0 "
                        "data files there. The combined ledger records both instants; `expiry_deletes_files` is a policy flag."})
    out.append({"claim": "Compaction's share of the wait",
                "lhbench": f"{e1m['to_rewritten'] / e1m['to_physical']:.0%} ({e1m['to_rewritten']} of {e1m['to_physical']} d): "
                           f"'compaction is 12%, not 34%'",
                "erasure": f"{a_st['rewrite'] / (a_st['commit'] + a_st['rewrite'] + a_st['unlink'] + a_st['expire']):.0%} under "
                           f"the lazy calendar; 0% once the rewrite runs daily (B, E)",
                "verdict": "operating",
                "note": "Both are true for their calendar. lhbench's baseline compacts every file holding a pending subject "
                        "weekly; the erasure A-default touches only the last 7 partitions weekly and everything monthly."})
    out.append({"claim": "Delete mode: copy-on-write vs merge-on-read (deletion vectors)",
                "lhbench": f"{e1c['p50']} d CoW vs {e1m['p50']} d MoR (1 day); bytes rewritten CoW/MoR = "
                           f"{e1c['bytes'] / e1m['bytes']:.2f}x (E1 scattered) -- the 2.9x on the slide is measured, not the "
                           f"model's 13.2/4.6",
                "erasure": f"{D['median_days_mean']:.1f} d CoW vs {B['median_days_mean']:.1f} d MoR at equal daily cadence (tie); "
                           f"bytes CoW/lazy-MoR = {D['rewrite_bytes_mean'] / A['rewrite_bytes_mean']:.2f}x, "
                           f"CoW/daily-MoR = {D['rewrite_bytes_mean'] / B['rewrite_bytes_mean']:.2f}x",
                "verdict": "agree",
                "note": "Delete mode does not move the deadline; the byte saving comes from the lazier calendar MoR ships "
                        "with (2.9x under lhbench's weekly file-FIFO, 1.7x under the erasure weekly window). Say 'MoR under a "
                        "weekly calendar' on the slide, not 'MoR'."})
    out.append({"claim": "Data layout (scattered / clustered / bucketed)",
                "lhbench": "0 days of residency difference; bytes differ (bucketed 4.1x scattered under CoW)",
                "erasure": "not varied (one layout)", "verdict": "agree",
                "note": "Layout is a cost lever, not a time lever. Combined harness carries all three layouts."})
    out.append({"claim": "Bucket versioning adds NoncurrentDays additively",
                "lhbench": f"+7 -> {o_v7_b['p50']:.0f} d, +10 -> {o_v10_b['p50']:.0f} d from {o_none_b['p50']:.0f} d: exact on a day clock",
                "erasure": f"N=7: {G[('A-default', n7)]['median_days_mean']:.1f}, 14: {G[('A-default', 'S3 · versioned + NoncurrentDays 14')]['median_days_mean']:.1f}, "
                           f"21: {G[('A-default', 'S3 · versioned + NoncurrentDays 21')]['median_days_mean']:.1f}, "
                           f"30: {G[('A-default', 'S3 · versioned + NoncurrentDays 30')]['median_days_mean']:.1f} d; bucket stage = N + 0.83 d "
                           f"(midnight-UTC rounding)",
                "verdict": "agree",
                "note": "Additive in both. The hour clock shows the rounding AWS documents; a day clock cannot."})
    out.append({"claim": "Coordinated scheduling",
                "lhbench": f"{e1m['p50']} -> {e2m['p50']} d (the retention floor), 100% met; zero variance across 5 seeds "
                           f"({[r['p50'] for r in seeds_d]}); holds at 25 KB/day (met {bud_d[25000]['met']:.0%} vs baseline "
                           f"{bud_b[25000]['met']:.0%}); amplification {e2m['amp'] / e1m['amp']:.2f}x the baseline's",
                "erasure": f"C-coordinated (defer the rewrite to the latest safe pass): {C['median_days_mean']:.1f} d vs "
                           f"E-asap-daily {E['median_days_mean']:.1f} d, same zero-miss edge at NoncurrentDays 23, bytes within "
                           f"{100 * (C['rewrite_bytes_mean'] / E['rewrite_bytes_mean'] - 1):.1f}%",
                "verdict": "operating",
                "note": "Two different mechanisms share a name. lhbench's deadline policy is EDF file selection under a byte "
                        "budget plus pulling expiry/cleanup forward; the erasure C policy defers the rewrite and runs the tail "
                        "daily. Both find the tail (retention + D_nc) is the floor. The combined preset "
                        "'combined-coordinated' is EDF + budget + daily tail + Iceberg defaults."})
    out.append({"claim": "Feasibility test",
                "lhbench": "retention + D_nc <= window; slide: 8 d floor (5 + 3) + 7 d + 15 d headroom = 30",
                "erasure": f"commit + retention + expiry cadence + N + 1 <= window; max N = "
                           f"{er['raw']['guaranteed_max_noncurrent_days_30d']} d with daily expiry, "
                           f"{er['raw']['guaranteed_max_noncurrent_days_30d_weekly_expiry']} with weekly; empirical edge "
                           f"{er['raw']['empirical_max_noncurrent_days_30d']} d",
                "verdict": "agree",
                "note": "Same inequality; the erasure form carries the expiry cadence and the rounding day, and the combined "
                        "`feasibility.floor_days` adds the orphan cadence when expiry does not delete files. Slide 3 (20 d measured) "
                        "vs slide 14 (5 + 3 d floors) is not a contradiction once the slide says 'documented minimums; our runs used 20 d'."})
    if model is not None:
        rv = model["rule_value"]
        out.append({"claim": "Cost of having no lifecycle rule (modelled)",
                    "lhbench": "slides 12/14 quote $135,048 / $12,646 / $8,643 (2.4 PiB, 4% churn)",
                    "erasure": f"current results/model.json: no rule ${rv['no_rule_first_year']['total_usd']:,.0f} year one, "
                               f"${rv['rule_7d']['total_usd']:,.0f} with a 7-day rule, ${rv['no_rule_year_two']['total_usd']:,.0f} "
                               f"in year two; residency {rv['residency_days']} d, write amp {rv['write_amp']}",
                    "verdict": "stale",
                    "note": "The slide numbers come from an earlier model revision. `residency.costmodel` takes residency and "
                            "amplification from a measured run, prints every assumption, and rounds to two significant figures."})
    out.append({"claim": "Delta Lake and Apache Hudi floors",
                "lhbench": "modelled: 7 d VACUUM; 10 commits (0.4 d hourly, 10 d daily)",
                "erasure": "modelled from the same documentation; Hudi KEEP_LATEST_BY_HOURS as the fix",
                "verdict": "agree", "note": "Neither harness runs Delta or Hudi. `formats.py` quotes the sources."})
    out.append({"claim": "Storage ladder (which bucket configurations are on the slide)",
                "lhbench": "S3 off / S3 + rule / GCS / AWS S3 Tables 10 d / S3 no rule / Object Lock (figure); table lists Azure",
                "erasure": "S3 off / S3 + 7 / GCS 7 / S3 no rule / Object Lock (executed); Azure in the table only",
                "verdict": "conflict",
                "note": "Pick one set. `stores.PRESETS` has all of them (plus MinIO count-only and Azure soft delete) so figure "
                        "and table can be generated from the same list."})
    out.append({"claim": "Provenance of the headline runs",
                "lhbench": f"E1/E2, S-* and O-store carry dirty_threshold {e1m['dirty_threshold']} and predate the file-granular "
                           f"executor: V-dirty at 0.05 gives {dirty[0.05]['p50']} d / met {dirty[0.05]['met']:.0%} while E1 at "
                           f"0.05 gives {e1m['p50']} d / met {e1m['met']:.0%}",
                "erasure": "154 runs from one code version; backtest reproduces one seed exactly",
                "verdict": "conflict",
                "note": "Re-run the lhbench main suite with the committed code (or the combined harness) before the deck "
                        "quotes 34 d; state the threshold used."})
    return out


def render_markdown(cl: list[dict], title: str = "Reconciliation: lhbench (Abhishek) vs erasure harness (Nilanjan)") -> str:
    L = [f"# {title}", "",
         "Both harnesses drive a real PyIceberg table in virtual time with a synthetic workload. "
         "They agree on the physics; they measured different operating points and reported different "
         "decompositions. Verdicts: **agree** / **operating** (same physics, different operating point) / "
         "**stale** / **conflict**.", ""]
    for c in cl:
        L += [f"## {c['claim']}", "", f"- **Verdict:** {c['verdict']}", f"- **lhbench:** {c['lhbench']}",
              f"- **erasure harness:** {c['erasure']}", f"- **Resolution:** {c['note']}", ""]
    return "\n".join(L)
