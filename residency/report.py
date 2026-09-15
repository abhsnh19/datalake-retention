"""Grouped means over a sweep: what a slide quotes.

Groups runs by (policy, store label, layout) across seeds and reports the
median residency (mean / min / max across seeds), p95, deadline-met
fraction, the lhbench to-stage medians, the erasure per-stage means, bytes
rewritten and amplification.  Both views come from the same runs.
"""
from __future__ import annotations

import statistics


def _mean(vals):
    vals = [v for v in vals if v is not None]
    return statistics.fmean(vals) if vals else None


def group(runs: list[dict]) -> list[dict]:
    groups: dict = {}
    for r in runs:
        groups.setdefault((r["policy"], r["store"]["label"], r["config"]["layout"]), []).append(r)
    out = []
    for (policy, label, layout), rs in sorted(groups.items()):
        lh = [r["summary"].get("lhbench") or {} for r in rs]
        er = [r["summary"].get("erasure") or {} for r in rs]
        p50 = [(x.get("drt_days") or {}).get("p50") for x in lh]
        p50 = [v for v in p50 if v is not None]
        stage_keys = ("commit", "rewrite", "unlink", "expire")
        out.append({
            "policy": policy, "store": label, "layout": layout, "seeds": sorted(r["seed"] for r in rs), "runs": len(rs),
            "bounded": rs[0]["store"]["bounded"],
            "n_obligations": sum(r["summary"]["n_obligations"] for r in rs),
            "n_censored": sum(r["summary"]["n_censored"] for r in rs),
            "p50_mean": _mean(p50), "p50_min": min(p50) if p50 else None, "p50_max": max(p50) if p50 else None,
            "p95_mean": _mean([(x.get("drt_days") or {}).get("p95") for x in lh]),
            "met_mean": _mean([x.get("deadline_met_fraction") for x in lh]),
            "miss_rate_mean": _mean([x.get("miss_rate") for x in er]),
            "median_completed_mean": _mean([(x.get("days") or {}).get("median") for x in er]),
            "to_stage_mean": {k: _mean([x.get(f"median_days_to_{k}") for x in lh])
                              for k in ("logical", "rewritten", "unreferenced", "delete_marker", "physical")},
            "stage_mean_days": {k: _mean([(x.get("stage_mean_days") or {}).get(k) for x in er]) for k in stage_keys},
            "bytes_policy_mean": _mean([r["rewrite"]["bytes_policy"] for r in rs]),
            "bytes_cow_mean": _mean([r["rewrite"]["bytes_cow"] for r in rs]),
            "amplification_mean": _mean([x.get("deletion_amplification") for x in lh]),
            "budget_utilisation_mean": _mean([r["budget"]["utilisation"] for r in rs]),
            "probe_checks": sum(r["probe"]["checks"] for r in rs),
            "probe_disagreements": sum(r["probe"]["disagreements"] for r in rs),
        })
    return out


def _f(v, w=6, d=1):
    return f"{v:{w}.{d}f}" if isinstance(v, (int, float)) else f"{'-':>{w}}"


def render(groups: list[dict]) -> str:
    lines = [f"{'policy':22s} {'store':46s} {'layout':9s} {'n':>3s} {'p50':>6s} {'min':>6s} {'max':>6s} {'p95':>6s} {'met':>5s} "
             f"{'rewr':>5s} {'unref':>6s} {'mark':>6s} {'phys':>6s} | {'commit':>6s} {'rewrite':>7s} {'unlink':>6s} {'bucket':>6s} "
             f"{'MB_pol':>7s} {'MB_cow':>7s} {'amp':>6s} {'util':>5s}"]
    for g in groups:
        ts, st = g["to_stage_mean"], g["stage_mean_days"]
        lines.append(f"{g['policy']:22s} {g['store']:46s} {g['layout']:9s} {g['runs']:3d} {_f(g['p50_mean'])} {_f(g['p50_min'])} "
                     f"{_f(g['p50_max'])} {_f(g['p95_mean'])} {_f(g['met_mean'], 5, 2)} {_f(ts['rewritten'], 5)} {_f(ts['unreferenced'])} "
                     f"{_f(ts['delete_marker'])} {_f(ts['physical'])} | {_f(st['commit'])} {_f(st['rewrite'], 7)} {_f(st['unlink'])} "
                     f"{_f(st['expire'])} {_f((g['bytes_policy_mean'] or 0) / 1e6, 7, 2)} {_f((g['bytes_cow_mean'] or 0) / 1e6, 7, 2)} "
                     f"{_f(g['amplification_mean'], 6, 1)} {_f(g['budget_utilisation_mean'], 5, 2)}")
    return "\n".join(lines)
