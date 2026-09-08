#!/usr/bin/env python3
"""Budget sweep: where does the deadline-aware scheduler break?

verify.py showed the daily rewrite cap was never binding (1.6%/2.9% utilisation),
so the mechanism has never been stress-tested. This tightens the cap until the
scheduler can no longer meet deadlines. The breaking point is the result.
"""
import json, multiprocessing as mp, time
from dataclasses import replace
from pathlib import Path
from lhbench.config import Config
from lhbench.sim import Simulation

OUT = Path(__file__).parent / "results_budget"
BASE = Config(horizon_days=100, warmup_days=10, tail_days=45, rows_per_day=900,
              n_subjects=1500, dsar_rate_per_day=2.0, retention_days=20,
              orphan_interval_days=15, delete_mode="mor", layout="scattered")
BUDGETS = [5_000, 10_000, 20_000, 40_000, 80_000, 160_000, 2_000_000]

def one(cfg):
    OUT.mkdir(exist_ok=True)
    tag = f"B{cfg.daily_rewrite_budget_bytes}-{cfg.policy}"
    t0 = time.time()
    r = Simulation(cfg, Path("/tmp/lhb_budget") / tag).run()
    s = r.summary
    used = sum(d["bytes_rewritten_today"] for d in r.timeline)
    cap = cfg.daily_rewrite_budget_bytes * cfg.horizon_days
    payload = {"tag": tag, "config": r.config, "summary": s,
               "timeline": r.timeline, "utilisation": used / cap}
    (OUT / f"{tag}.json").write_text(json.dumps(payload, default=str))
    print(f"  {tag:26s} {time.time()-t0:5.0f}s  DRT p50={s['drt_days']['p50']:3.0f} "
          f"met={s['deadline_met_fraction']:6.1%} util={used/cap:6.1%}", flush=True)
    return payload

if __name__ == "__main__":
    cfgs = [replace(BASE, name="BUD", policy=p, daily_rewrite_budget_bytes=b)
            for p in ("baseline", "deadline") for b in BUDGETS]
    cfgs.sort(key=lambda c: c.daily_rewrite_budget_bytes)
    print(f"{len(cfgs)} configs", flush=True)
    with mp.Pool(2) as pool:
        res = pool.map(one, cfgs, chunksize=1)
    (OUT / "combined.json").write_text(json.dumps(
        [{"tag": r["tag"], "config": r["config"], "summary": r["summary"],
          "utilisation": r["utilisation"]} for r in res], indent=1, default=str))
    print("done")
