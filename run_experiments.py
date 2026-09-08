#!/usr/bin/env python3
"""Experiment runner.

Defines the experiment grid, runs it in parallel, writes one JSON per run plus
a combined results file.

    python3 run_experiments.py --suite main     # the paper's core figures
    python3 run_experiments.py --suite sensitivity
    python3 run_experiments.py --suite all --jobs 4
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import time
from dataclasses import replace
from pathlib import Path

from lhbench.config import Config
from lhbench.sim import Simulation

OUT = Path(__file__).parent / "results"

# Shared base: small enough to run many configurations, large enough that the
# residency stack has room to express itself (horizon > retention + orphan).
BASE = Config(
    horizon_days=100,
    warmup_days=10,
    tail_days=45,
    rows_per_day=900,
    n_subjects=1500,
    dsar_rate_per_day=2.0,
    retention_days=20,
    orphan_interval_days=15,
    compact_interval_days=7,
    expire_interval_days=7,
)


def main_suite() -> list[Config]:
    """The four core comparisons the paper's figures rest on."""
    cfgs = []
    # E1/E2: does the residency stack dominate, and does the mechanism fix it?
    # Same grid under both policies, at equal rewrite budget.
    for policy, name in (("baseline", "E1"), ("deadline", "E2")):
        for layout in ("scattered", "clustered", "bucketed"):
            for mode in ("cow", "mor"):
                cfgs.append(
                    replace(BASE, name=name, layout=layout, delete_mode=mode,
                            policy=policy)
                )
    return cfgs


def sensitivity_suite() -> list[Config]:
    """Sweeps that convert 'we chose these parameters' into 'the effect holds
    across the plausible space'. This section is what defends a synthetic
    workload from the obvious reviewer attack."""
    cfgs = []
    for pol in ("baseline", "deadline"):
        for r in (7, 20, 45):                      # retention window
            cfgs.append(replace(BASE, name="S-retention", policy=pol,
                                retention_days=r, delete_mode="mor"))
        for o in (1, 15, 30):                      # orphan cleanup cadence
            cfgs.append(replace(BASE, name="S-orphan", policy=pol,
                                orphan_interval_days=o, delete_mode="mor"))
        for a in (0.8, 1.2, 1.8):                  # subject skew
            cfgs.append(replace(BASE, name="S-alpha", policy=pol,
                                subject_alpha=a, delete_mode="mor"))
        for d in (1.0, 4.0, 10.0):                 # erasure demand
            cfgs.append(replace(BASE, name="S-demand", policy=pol,
                                dsar_rate_per_day=d, delete_mode="mor"))

    # Bucket count vs erasure demand. Hypothesis: bucketing only prunes
    # rewrites while a batch touches few buckets, so the bucket count needed
    # to keep amplification down scales with erasure demand. If true, "just
    # partition on the erasure key" is not a standalone fix -- which is the
    # argument for scheduling.
    for nb in (16, 64, 256):
        for d in (2.0, 10.0):
            cfgs.append(replace(BASE, name="S-buckets", policy="baseline",
                                layout="bucketed", n_buckets=nb,
                                dsar_rate_per_day=d, delete_mode="mor"))
    return cfgs


def objstore_suite() -> list[Config]:
    """Stage 5: what bucket versioning adds beneath the table format.

    HORIZON NOTE: these runs use a much longer horizon than the other suites.
    At D_nc=30 a 100-day horizon censored 69% of the cohort and reported a p50
    of 56 days when the true value is 62. Residency has to fit inside the
    horizon or every number is optimistic.

    D_nc values are documented provider behaviour, not spacing:
      0   MinIO count-only ILM -- excess noncurrent versions removed with no
          waiting period.
      1   AWS S3 lifecycle minimum (NoncurrentDays must be a positive int).
      7   THE DEFAULT. GCS soft delete is ON by default at 7 days and cannot be
          bypassed; Azure portal-provisioned accounts default to 7; Databricks'
          documented fallback when S3 versioning cannot be disabled is "7 days
          or less". Four independent sources converge here.
      10  AWS S3 Tables (managed Iceberg) nonCurrentDays default -- AWS's own
          chosen lakehouse setting.
      30  a common conservative enterprise lifecycle rule.
    """
    base = replace(
        BASE, horizon_days=160, warmup_days=10, tail_days=90,
        delete_mode="mor", layout="scattered",
        retention_days=20, orphan_interval_days=15,
    )
    cfgs = []
    for pol in ("baseline", "deadline"):
        cfgs.append(replace(base, name="O-store", policy=pol,
                            object_store="none"))
        for dnc in (0, 1, 7, 10, 30):
            cfgs.append(replace(base, name="O-store", policy=pol,
                                object_store="versioned",
                                noncurrent_expiration_days=dnc))
    return cfgs


def validation_suite() -> list[Config]:
    """Three things the paper claims but has never actually tested.

    V1 BUDGET SWEEP. The headline mechanism claim is "wins at the same budget
    cap". But verify.py showed utilisation of 1.6% (baseline) / 2.9%
    (deadline) -- the cap was NEVER BINDING, so the scheduler has never been
    stress-tested. Sweep the budget down until it bites. Either the mechanism
    degrades gracefully and we find its breaking point (a result), or it holds
    at every budget, which would mean the rewrite work is trivially small and
    the whole fairness framing is vacuous (also a result, and one we would have
    to report against ourselves).

    V2 SEED VARIANCE. Every number in the paper comes from one seed. If the
    findings move across seeds, none of them hold. Five seeds, both policies.

    V3 CENSORING FIX. The r45 baseline run had 69% of its cohort censored and
    reported a p50 of 56 days. Re-run with a horizon long enough to close it.
    """
    cfgs = []
    mor = dict(delete_mode="mor", layout="scattered")

    # V1: with the file-granular executor the cap is genuinely enforceable, so
    # this sweep now measures graceful degradation rather than harness overshoot.
    for pol in ("baseline", "deadline"):
        for budget in (5_000, 10_000, 25_000, 50_000, 100_000, 2_000_000):
            cfgs.append(replace(BASE, name="V-budget", policy=pol,
                                daily_rewrite_budget_bytes=budget, **mor))

    # V2: same configuration as the E1/E2 headline, five seeds.
    for pol in ("baseline", "deadline"):
        for seed in (1, 2, 3, 4, 5):
            cfgs.append(replace(BASE, name="V-seed", policy=pol, seed=seed, **mor))

    # V4: does the delete-ratio heuristic cripple the baseline? Reported so the
    # headline can be quoted against the strongest baseline, not a hobbled one.
    for dt in (0.0, 0.01, 0.05, 0.10):
        cfgs.append(replace(BASE, name="V-dirty", policy="baseline",
                            dirty_threshold=dt, **mor))

    # V3: the censored run, with room to complete.
    for pol in ("baseline", "deadline"):
        cfgs.append(replace(BASE, name="V-r45", policy=pol, retention_days=45,
                            horizon_days=200, warmup_days=10, tail_days=120,
                            **mor))
    return cfgs


def run_one(cfg: Config) -> dict:
    # seed and budget MUST be in the tag: without them the seed-variance and
    # budget-sweep runs all write to the same filename and silently overwrite
    # each other, leaving one survivor that looks like a legitimate result.
    tag = (cfg.tag() + f"-a{cfg.subject_alpha}-d{cfg.dsar_rate_per_day}"
           f"-s{cfg.seed}-B{cfg.daily_rewrite_budget_bytes}"
           f"-t{cfg.dirty_threshold}")
    workdir = Path("/tmp/lhbench") / tag
    t0 = time.time()
    result = Simulation(cfg, workdir).run()
    elapsed = time.time() - t0
    payload = {
        "tag": tag,
        "elapsed_s": elapsed,
        "config": result.config,
        "summary": result.summary,
        "timeline": result.timeline,
        "obligations": result.obligations,
    }
    OUT.mkdir(exist_ok=True)
    (OUT / f"{tag}.json").write_text(json.dumps(payload, default=str))
    s = result.summary
    print(
        f"  done {tag:58s} {elapsed:5.0f}s  "
        f"DRT p50={s.get('drt_days',{}).get('p50')} "
        f"met={s.get('deadline_met_fraction', 0):.0%} "
        f"amp={s.get('deletion_amplification') and round(s['deletion_amplification'],1)}",
        flush=True,
    )
    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite",
                    choices=["main", "sensitivity", "objstore", "validation",
                             "all"],
                    default="main")
    ap.add_argument("--jobs", type=int, default=4)
    args = ap.parse_args()

    cfgs = []
    if args.suite in ("main", "all"):
        cfgs += main_suite()
    if args.suite in ("sensitivity", "all"):
        cfgs += sensitivity_suite()
    if args.suite in ("objstore", "all"):
        cfgs += objstore_suite()
    if args.suite in ("validation", "all"):
        cfgs += validation_suite()

    # Cheapest first: copy-on-write runs rewrite the table on every delete and
    # cost several times a merge-on-read run. Front-loading the cheap ones
    # means partial results are usable long before the sweep finishes.
    cfgs.sort(key=lambda c: (c.delete_mode == "cow", c.policy == "deadline",
                             c.horizon_days, c.dsar_rate_per_day))

    print(f"running {len(cfgs)} configurations on {args.jobs} workers", flush=True)
    t0 = time.time()
    with mp.Pool(args.jobs) as pool:
        results = pool.map(run_one, cfgs, chunksize=1)

    OUT.mkdir(exist_ok=True)
    (OUT / "combined.json").write_text(
        json.dumps(
            [{"tag": r["tag"], "config": r["config"], "summary": r["summary"]}
             for r in results],
            indent=2, default=str,
        )
    )
    print(f"\nall done in {time.time()-t0:.0f}s -> {OUT}/combined.json")


if __name__ == "__main__":
    main()
