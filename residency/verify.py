"""Correctness, fairness and claim checks on a sweep (lhbench's verify.py,
extended for the hour clock and the probe).  Run it before believing a
figure.  Every check returns (ok, lines); ``main`` prints them.
"""
from __future__ import annotations

import json
import os
import statistics
from pathlib import Path

from .vclock import HOURS_PER_DAY


def load(results_dir: str) -> list[dict]:
    out = []
    for p in sorted(Path(results_dir).glob("*.json")):
        if p.name in ("combined.json", "summary.json"):
            continue
        with open(p) as fh:
            out.append(json.load(fh))
    return out


def _cfg_key(r: dict, drop=("horizon_days", "warmup_days", "tail_days", "name", "seed")) -> tuple:
    c = {k: v for k, v in r["config"].items() if k not in drop}
    return (r["policy"], r["store"]["label"], tuple(sorted(c.items())))


def check_stage_monotonicity(runs):
    bad = total = 0
    for r in runs:
        for o in r["obligations"]:
            seq = [o[k] for k in ("commit", "rewritten", "unreferenced", "unlinked", "physical") if o[k] is not None]
            total += 1
            if seq != sorted(seq) or (o["commit"] is not None and o["commit"] < o["arrival"]):
                bad += 1
    return bad == 0, [f"stage ordering: {total - bad}/{total} obligations well-ordered"]


def check_budget_respected(runs):
    ok, lines = True, []
    for r in runs:
        cap = r["budget"]["cap_bytes"]
        if cap is None or r["policy_spec"]["delete_mode"] == "cow":
            continue
        over = [d for d in r["timeline"] if d["bytes_rewritten_today"] > cap]
        forced = r["budget"]["forced_days"]
        if len(over) > forced:
            ok = False
            lines.append(f"BUDGET EXCEEDED {r['policy']} {r['store']['label']} seed {r['seed']}: "
                         f"{len(over)} days over cap, {forced} explained by single-file forward progress")
        elif over:
            lines.append(f"budget: {r['policy']} seed {r['seed']} exceeded the cap on {len(over)} day(s) only because "
                         f"one file was larger than the whole budget (forward-progress rule)")
    if ok and not lines:
        lines.append("budget: no merge-on-read run exceeded its daily cap")
    return ok, lines


def check_cohorts(runs, minimum: int = 20):
    small = [f"{r['policy']} {r['store']['label']} seed {r['seed']}" for r in runs
             if r["summary"]["n_obligations"] < minimum]
    sizes = [r["summary"]["n_obligations"] for r in runs]
    lines = [f"cohort sizes: min {min(sizes)}, median {statistics.median(sizes):.0f}, max {max(sizes)}"] if sizes else []
    if small:
        lines.append(f"WARNING: {len(small)} run(s) have cohorts under {minimum} obligations: " + "; ".join(small[:5]))
    return not small, lines


def check_censoring(runs, top: int = 3):
    rows = sorted(runs, key=lambda r: -(r["summary"]["n_censored"] / max(1, r["summary"]["n_obligations"])))[:top]
    lines = ["highest censoring (unclosed obligations in cohort):"]
    for r in rows:
        s = r["summary"]
        frac = s["n_censored"] / max(1, s["n_obligations"])
        bounded = r["store"]["bounded"]
        lines.append(f"  {frac:5.1%}  {r['policy']} {r['store']['label']} seed {r['seed']}"
                     + ("" if bounded or frac == 0 else "  (unbounded store: expected)"))
    return True, lines


def check_probe(runs):
    checks = sum(r["probe"]["checks"] for r in runs)
    dis = sum(r["probe"]["disagreements"] for r in runs)
    return dis == 0, [f"probe: {checks} checks, {dis} disagreements between the ledger and ListObjectVersions + read-back"]


def check_residency_rule(runs):
    """lhbench claim: baseline residency tracks retention + orphan interval
    when expiry does not delete files."""
    lines = ["baseline residency vs (retention + orphan interval), metadata-only expiry:"]
    for r in runs:
        ps, s = r["policy_spec"], r["summary"]
        if ps["rewrite"] != "cadence_fifo" or ps["expiry_deletes_files"] or not s.get("lhbench"):
            continue
        if r["store"]["mode"] != "unversioned":
            continue
        pred = ps["snapshot_retention_days"] + ps["orphan_interval_days"]
        obs = s["lhbench"]["drt_days"]["p50"]
        lines.append(f"  retention {ps['snapshot_retention_days']:2d} + orphan {ps['orphan_interval_days']:2d} = {pred:3d}"
                     f"  ->  observed p50 {obs:5.1f}  (delta {obs - pred:+.1f})  seed {r['seed']}")
    return True, lines


def check_mechanism_floor(runs):
    """lhbench claim: the deadline policy attains the retention floor (+ D_nc)."""
    lines = ["deadline residency vs floor (retention + store tail):"]
    for r in runs:
        ps, s = r["policy_spec"], r["summary"]
        if ps["rewrite"] != "deadline_edf" or not s.get("lhbench"):
            continue
        floor = ps["snapshot_retention_days"] + r["store"]["tail_days"]
        obs = s["lhbench"]["drt_days"]["p50"]
        flag = "= floor" if obs - floor <= 1.0 + r["store"].get("rounding_days", 1) else "ABOVE floor"
        lines.append(f"  {r['policy']} {r['store']['label']} seed {r['seed']}: floor {floor}  observed p50 {obs:.1f}  {flag}")
    return True, lines


def check_objstore_additivity(runs):
    """Both claims: a versioned bucket adds its NoncurrentDays to residency.
    lhbench: exactly (day clock).  erasure: N .. N + 1 (midnight rounding)."""
    base: dict = {}
    for r in runs:
        if r["store"]["mode"] == "unversioned":
            base[(r["policy"], r["seed"], r["config"]["layout"], r["config"]["horizon_days"])] = r["summary"]["lhbench"]["drt_days"]["p50"]
    lines, exact, off = ["residency vs (unversioned baseline + N):"], 0, 0
    for r in sorted(runs, key=lambda r: (r["policy"], r["seed"], r["store"]["tail_days"])):
        if r["store"]["mode"] not in ("versioned", "s3tables") or not r["store"]["bounded"]:
            continue
        k = (r["policy"], r["seed"], r["config"]["layout"], r["config"]["horizon_days"])
        if k not in base:
            continue
        n = r["store"]["tail_days"]
        obs = r["summary"]["lhbench"]["drt_days"]["p50"]
        pred = base[k] + n
        ok = -0.01 <= obs - pred <= 1.0 + 1e-9
        exact += ok
        off += not ok
        lines.append(f"  {r['policy']} seed {r['seed']} N={n:2d}: predicted {pred:.1f}..{pred + 1:.1f}  observed {obs:.1f}  "
                     f"{'within rounding' if ok else f'OFF {obs - pred:+.1f}'}")
    lines.append(f"  -> {exact} within one day of rounding, {off} off")
    return off == 0, lines


def check_cohort_sensitivity(runs, tolerance_days: float = 1.0):
    """Same policy, store, seed and workload, different horizon/cohort: the
    medians should agree.  lhbench's deck quoted 34 d from a 100-day run and
    32 d from a 160-day run of the same configuration."""
    groups: dict = {}
    for r in runs:
        groups.setdefault(_cfg_key(r) + (r["seed"],), []).append(r)
    lines, flagged = ["same configuration under different horizons/cohorts:"], 0
    for key, rs in groups.items():
        if len(rs) < 2:
            continue
        meds = [(r["config"]["horizon_days"], r["config"]["tail_days"], r["summary"]["lhbench"]["drt_days"]["p50"]) for r in rs]
        spread = max(m[2] for m in meds) - min(m[2] for m in meds)
        if spread > tolerance_days:
            flagged += 1
            lines.append(f"  {rs[0]['policy']} {rs[0]['store']['label']} seed {rs[0]['seed']}: "
                         + ", ".join(f"horizon {h}/tail {t} -> {p:.1f} d" for h, t, p in meds) + f"  (spread {spread:.1f} d)")
    if flagged == 0:
        lines.append("  none differ by more than a day")
    return flagged == 0, lines


def run_all(runs: list[dict]) -> tuple[bool, list[str]]:
    out, ok_all = [], True
    for title, fn, hard in (("CORRECTNESS", check_stage_monotonicity, True), ("", check_budget_respected, True),
                            ("", check_cohorts, False), ("", check_censoring, False), ("", check_probe, True),
                            ("CLAIMS", check_residency_rule, False), ("", check_mechanism_floor, False),
                            ("", check_objstore_additivity, True), ("", check_cohort_sensitivity, False)):
        if title:
            out.append(title)
        ok, lines = fn(runs)
        out += ["  " + l for l in lines]
        if hard and not ok:
            ok_all = False
    out.append("ALL HARD CHECKS PASSED" if ok_all else "SOME HARD CHECKS FAILED -- see above")
    return ok_all, out


def main(results_dir: str) -> int:
    runs = load(results_dir)
    if not runs:
        print("no results")
        return 2
    print(f"verifying {len(runs)} runs\n")
    ok, lines = run_all(runs)
    print("\n".join(lines))
    return 0 if ok else 1
