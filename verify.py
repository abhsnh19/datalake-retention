#!/usr/bin/env python3
"""Sanity checks on the experiment output.

Every check here corresponds to a way the headline numbers could be wrong or
unfair. Run it before believing any figure.

    python3 verify.py
"""

from __future__ import annotations

import json
import statistics
from pathlib import Path

RESULTS = Path(__file__).parent / "results"


def load():
    out = []
    for p in sorted(RESULTS.glob("*.json")):
        if p.name == "combined.json":
            continue
        out.append(json.loads(p.read_text()))
    return out


def check_stage_monotonicity(runs) -> bool:
    """Stages must complete in order. An out-of-order timestamp means the
    ledger is mis-attributing, and every residency number is suspect."""
    bad = 0
    total = 0
    for r in runs:
        for o in r["obligations"]:
            seq = [o["logical"], o["rewritten"], o["unreferenced"], o["physical"]]
            seq = [s for s in seq if s is not None]
            total += 1
            if seq != sorted(seq):
                bad += 1
            if o["physical"] is not None and o["physical"] < o["issued"]:
                bad += 1
    print(f"  stage ordering: {total - bad}/{total} obligations well-ordered")
    return bad == 0


def check_budget_respected(runs) -> bool:
    """Neither policy may exceed the daily rewrite budget.

    NOTE the precise claim: the budget is an equal CAP, not equal spend. The
    deadline policy legitimately uses more of its allowance because it works
    every day while the baseline works on a 7-day cadence. The paper must say
    'same budget' and then report utilisation -- claiming 'same cost' would be
    false."""
    ok = True
    for r in runs:
        budget = r["config"]["daily_rewrite_budget_bytes"]
        mode = r["config"]["delete_mode"]
        over = [
            d for d in r["timeline"] if d["bytes_rewritten_today"] > budget
        ]
        # Copy-on-write pays its rewrite at delete time, outside the scheduler's
        # control, so an overshoot there is expected and is a property of CoW.
        if over and mode != "cow":
            ok = False
            print(
                f"  BUDGET EXCEEDED in {r['tag']}: {len(over)} days over cap "
                f"(max {max(d['bytes_rewritten_today'] for d in over):,})"
            )
    if ok:
        print("  budget: no merge-on-read run exceeded its daily cap")
    return ok


def report_utilisation(runs) -> None:
    """How much of the allowance each policy actually used."""
    for pol in ("baseline", "deadline"):
        rs = [
            r for r in runs
            if r["config"]["policy"] == pol and r["config"]["name"] in ("E1", "E2")
            and r["config"]["delete_mode"] == "mor"
        ]
        if not rs:
            continue
        utils, active = [], []
        for r in rs:
            budget = r["config"]["daily_rewrite_budget_bytes"]
            days = r["timeline"]
            utils.append(
                sum(d["bytes_rewritten_today"] for d in days) / (budget * len(days))
            )
            active.append(
                sum(1 for d in days if d["bytes_rewritten_today"] > 0) / len(days)
            )
        print(
            f"  {pol:9s} budget utilisation {statistics.fmean(utils):5.1%}   "
            f"days with any rewrite {statistics.fmean(active):5.1%}"
        )


def check_residency_rule(runs) -> None:
    """Test the claim: baseline residency tracks retention + orphan interval."""
    print("  baseline residency vs (retention + orphan interval):")
    rows = []
    for r in runs:
        c, s = r["config"], r["summary"]
        if c["policy"] != "baseline" or not s.get("drt_days"):
            continue
        if c["delete_mode"] != "mor" or c["layout"] != "scattered":
            continue
        if c.get("object_store", "none") != "none":
            continue  # stage 5 adds D_nc on top; checked separately below
        pred = c["retention_days"] + c["orphan_interval_days"]
        rows.append((pred, s["drt_days"]["p50"], c["retention_days"],
                     c["orphan_interval_days"]))
    for pred, obs, ret, orph in sorted(set(rows)):
        print(f"    retention {ret:2d} + orphan {orph:2d} = {pred:3d}  ->  "
              f"observed p50 {obs:3.0f}   (delta {obs - pred:+.0f})")


def check_mechanism_floor(runs) -> None:
    """Test the claim: the deadline policy attains the retention floor."""
    print("  deadline residency vs retention window (the floor):")
    rows = []
    for r in runs:
        c, s = r["config"], r["summary"]
        if c["policy"] != "deadline" or not s.get("drt_days"):
            continue
        if c["delete_mode"] != "mor" or c["layout"] != "scattered":
            continue
        if c.get("object_store", "none") != "none":
            continue
        rows.append((c["retention_days"], s["drt_days"]["p50"]))
    for ret, obs in sorted(set(rows)):
        flag = "= floor" if abs(obs - ret) <= 1 else "ABOVE floor"
        print(f"    retention {ret:2d}  ->  observed p50 {obs:3.0f}   {flag}")


def check_objstore_additivity(runs) -> None:
    """Test the claim: bucket versioning adds D_nc to residency, additively.

    An exact additive law is a strong claim, so it is checked per-configuration
    rather than eyeballed off a chart."""
    rs = [r for r in runs if r["config"].get("name") == "O-store"]
    if not rs:
        return
    base = {}
    for r in rs:
        c, s = r["config"], r["summary"]
        if c["object_store"] == "none":
            base[c["policy"]] = s["drt_days"]["p50"]
    print("  residency vs (no-versioning baseline + D_nc):")
    exact = off = 0
    for r in sorted(rs, key=lambda r: (r["config"]["policy"],
                                       r["config"]["noncurrent_expiration_days"])):
        c, s = r["config"], r["summary"]
        if c["object_store"] == "none" or c["policy"] not in base:
            continue
        dnc = c["noncurrent_expiration_days"]
        pred = base[c["policy"]] + dnc
        obs = s["drt_days"]["p50"]
        ok = abs(pred - obs) < 0.5
        exact += ok
        off += not ok
        print(f"    {c['policy']:9s} D_nc={dnc:2d}  predicted {pred:3.0f}  "
              f"observed {obs:3.0f}  {'exact' if ok else f'OFF {obs-pred:+.0f}'}")
    print(f"  -> {exact} exact, {off} off")


def check_cohorts(runs) -> bool:
    """A cohort too small to be meaningful invalidates the percentiles."""
    small = [r["tag"] for r in runs if r["summary"].get("n_obligations", 0) < 20]
    if small:
        print(f"  WARNING: {len(small)} runs have cohorts under 20 obligations")
        for t in small[:5]:
            print(f"    {t}")
        return False
    sizes = [r["summary"].get("n_obligations", 0) for r in runs]
    print(f"  cohort sizes: min {min(sizes)}, median {statistics.median(sizes):.0f}, "
          f"max {max(sizes)}")
    return True


def check_censoring(runs) -> None:
    """High censoring means the horizon is too short and results are optimistic."""
    worst = sorted(
        runs,
        key=lambda r: -(r["summary"].get("n_censored", 0)
                        / max(1, r["summary"].get("n_obligations", 1))),
    )[:3]
    print("  highest censoring rates (unclosed obligations in cohort):")
    for r in worst:
        s = r["summary"]
        frac = s.get("n_censored", 0) / max(1, s.get("n_obligations", 1))
        print(f"    {frac:5.1%}  {r['tag']}")


def main() -> None:
    runs = load()
    if not runs:
        print("no results")
        return
    print(f"verifying {len(runs)} runs\n")

    print("CORRECTNESS")
    ok1 = check_stage_monotonicity(runs)
    ok2 = check_budget_respected(runs)
    ok3 = check_cohorts(runs)
    check_censoring(runs)

    print("\nFAIRNESS")
    report_utilisation(runs)

    print("\nCLAIMS")
    check_residency_rule(runs)
    check_mechanism_floor(runs)
    check_objstore_additivity(runs)

    print("\n" + ("ALL CORRECTNESS CHECKS PASSED" if (ok1 and ok2 and ok3)
                  else "SOME CHECKS FAILED -- see above"))


if __name__ == "__main__":
    main()
