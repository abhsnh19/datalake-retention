#!/usr/bin/env python3
"""Does the theory hold? Three checks the paper has never actually run."""
import json, glob, statistics

runs = []
for p in glob.glob('results/V-*.json'):
    runs.append(json.load(open(p)))

def sel(name, pol=None):
    return [r for r in runs if r['config']['name'] == name
            and (pol is None or r['config']['policy'] == pol)]

print("="*74)
print("V1  BUDGET SWEEP -- is the mechanism winning by scheduling, or by spending?")
print("="*74)
print(f"{'budget/day':>12} | {'baseline':^26} | {'deadline':^26}")
print(f"{'':>12} | {'p50':>5}{'met%':>7}{'util%':>7}{'over':>6} | {'p50':>5}{'met%':>7}{'util%':>7}{'over':>6}")
budgets = sorted({r['config']['daily_rewrite_budget_bytes'] for r in sel('V-budget')})
for b in budgets:
    cells = []
    for pol in ('baseline', 'deadline'):
        m = [r for r in sel('V-budget', pol)
             if r['config']['daily_rewrite_budget_bytes'] == b]
        if not m:
            cells.append(f"{'-':>25}"); continue
        r = m[0]; s = r['summary']
        util = sum(d['bytes_rewritten_today'] for d in r['timeline']) / (b*len(r['timeline']))
        over = sum(1 for d in r['timeline'] if d['bytes_rewritten_today'] > b)
        amp = s.get('deletion_amplification') or 0
        cells.append(f"{s['drt_days']['p50']:5.0f}{100*s['deadline_met_fraction']:7.1f}"
                     f"{100*util:7.1f}{over:6d}")
    print(f"{b:>12,} | {cells[0]} | {cells[1]}")

print()
print("="*74)
print("V2  SEED VARIANCE -- do the findings survive a different random draw?")
print("="*74)
for pol in ('baseline', 'deadline'):
    rs = sel('V-seed', pol)
    if not rs: continue
    p50 = [r['summary']['drt_days']['p50'] for r in rs]
    met = [100*r['summary']['deadline_met_fraction'] for r in rs]
    amp = [r['summary']['deletion_amplification'] or 0 for r in rs]
    def fmt(v):
        return (f"mean {statistics.fmean(v):6.1f}  sd {statistics.pstdev(v):5.1f}  "
                f"range [{min(v):.0f}, {max(v):.0f}]")
    print(f"  {pol:9s} n={len(rs)}")
    print(f"    DRT p50    {fmt(p50)}")
    print(f"    met %      {fmt(met)}")
    print(f"    amp        {fmt(amp)}")

print()
print("="*74)
print("V4  BASELINE STRENGTH -- is the baseline hobbled by its delete-ratio gate?")
print("="*74)
rs = sorted(sel('V-dirty'), key=lambda r: r['config']['dirty_threshold'])
for r in rs:
    s_ = r['summary']
    print(f"  dirty_threshold={r['config']['dirty_threshold']:<5} "
          f"DRT p50={s_['drt_days']['p50']:3.0f}  "
          f"met={100*s_['deadline_met_fraction']:5.1f}%  "
          f"amp={s_['deletion_amplification']:5.1f}")
print("  (headline must quote dt=0.0: the STRONGEST baseline. Real systems ship")
print("   a non-zero threshold, which makes the baseline worse, not better.)")

print()
print("="*74)
print("V3  CENSORING FIX -- the r45 run, with a horizon long enough to close it")
print("="*74)
for r in sorted(sel('V-r45'), key=lambda r: r['config']['policy']):
    s = r['summary']
    frac = s['n_censored']/max(1, s['n_obligations'])
    print(f"  {r['config']['policy']:9s} retention=45 horizon={r['config']['horizon_days']}  "
          f"censored={frac:5.1%}  DRT p50={s['drt_days']['p50']:3.0f} "
          f"p95={s['drt_days']['p95']:3.0f}  met={100*s['deadline_met_fraction']:5.1f}%")
print("  (previously: baseline p50=56 with 69% censored -- optimistic and unusable)")
