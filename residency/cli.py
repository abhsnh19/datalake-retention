"""Command line.

    python -m residency.cli run --suite smoke|lhbench-main|erasure-grid|ladder|combined [--workers N] [--out DIR]
    python -m residency.cli cell --policy A-default --store s3-versioned-7d --seed 1
    python -m residency.cli verify --results DIR
    python -m residency.cli reconcile --lhbench results --erasure residency_data/erasure_harness.json
    python -m residency.cli backtest --results DIR [--seed S]
    python -m residency.cli demo
    python -m residency.cli plan --window 30 --retention 5 --expire-cadence 1
    python -m residency.cli cost --results DIR --policy lhbench-baseline
    python -m residency.cli report --results DIR [--json groups.json]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import replace

from . import DEFAULT_SEEDS, __version__
from .feasibility import max_noncurrent_days, plan, render
from .policies import PRESETS as POLICIES
from .runner import run_cell
from .stores import PRESETS as STORES, StoreSpec, preset as store_preset
from .workload import Config

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DEFAULT_OUT = os.path.join(ROOT, "results_residency")

BASE = Config(horizon_days=100, warmup_days=10, tail_days=45, rows_per_day=900, n_subjects=1500,
              dsar_rate_per_day=2.0)
LONG = replace(BASE, horizon_days=160, tail_days=90)
SMOKE = Config(horizon_days=70, warmup_days=3, tail_days=45, rows_per_day=150, n_subjects=200,
               dsar_rate_per_day=1.5, files_per_day=1)


def suite(name: str, seeds) -> list[tuple[Config, str, StoreSpec]]:
    cells = []
    if name == "smoke":
        for seed in seeds[:1]:
            c = replace(SMOKE, seed=seed)
            cells += [(c, "lhbench-baseline", store_preset("s3-unversioned")),
                      (c, "lhbench-deadline", store_preset("s3-versioned-7d")),
                      (c, "A-default", store_preset("s3-versioned-7d")),
                      (c, "D-cow-default", store_preset("s3-unversioned"))]
    elif name == "lhbench-main":
        for seed in seeds:
            for pol in ("lhbench-baseline", "lhbench-deadline"):
                for layout in ("scattered", "clustered", "bucketed"):
                    for mode in ("mor", "cow"):
                        p = POLICIES[pol] if mode == "mor" else replace(POLICIES[pol], delete_mode="cow", rewrite="none",
                                                                       name=pol + "-cow")
                        cells.append((replace(BASE, seed=seed, layout=layout, name="E"), p, store_preset("s3-unversioned")))
    elif name == "erasure-grid":
        for seed in seeds:
            for n in (7, 14, 21, 30):
                for pol in ("A-default", "B-compactor-only", "C-coordinated", "D-cow-default", "E-asap-daily", "F-cow-daily-expiry"):
                    cells.append((replace(LONG, seed=seed, name="grid"), pol, StoreSpec("versioned", noncurrent_days=n)))
    elif name == "ladder":
        for pol in ("lhbench-baseline", "A-default", "combined-coordinated"):
            for st in ("s3-unversioned", "minio-count-only", "s3-versioned-1d", "s3-versioned-7d", "gcs-default",
                       "azure-portal-default", "s3tables-default", "s3-versioned-no-rule", "s3-object-lock-compliance"):
                cells.append((replace(LONG, seed=seeds[0], name="ladder"),
                              "s3tables-managed" if st == "s3tables-default" and pol == "A-default" else pol,
                              store_preset(st)))
    elif name == "combined":
        for seed in seeds:
            for pol in ("lhbench-baseline", "lhbench-deadline", "A-default", "E-asap-daily", "D-cow-default", "combined-coordinated"):
                for st in ("s3-unversioned", "s3-versioned-7d", "s3tables-default"):
                    cells.append((replace(LONG, seed=seed, name="combined"), pol, store_preset(st)))
    else:
        raise KeyError(f"unknown suite {name!r}")
    return cells


def _tag(cfg: Config, pol, store: StoreSpec) -> str:
    pname = pol if isinstance(pol, str) else pol.name
    lab = store.label.replace(" ", "").replace("·", "-").replace("+", "").replace("/", "")
    return f"{cfg.name}-{pname}-{cfg.layout}-{lab}-h{cfg.horizon_days}-s{cfg.seed}"


def sweep(cells, workers: int, out_dir: str, progress=True) -> list[dict]:
    """Every cell in its own child process (no process pool: sandboxed hosts
    refuse the semaphore sysconf the stdlib pool makes)."""
    os.makedirs(out_dir, exist_ok=True)
    work_root = tempfile.mkdtemp(prefix="residency-")
    jobs = []
    for i, (cfg, pol, store) in enumerate(cells):
        pol_d = POLICIES[pol].as_dict() if isinstance(pol, str) else pol.as_dict()
        job = {"config": cfg.as_dict(), "policy": pol_d, "store": store.as_dict(),
               "work": os.path.join(work_root, f"cell_{i:03d}"), "tag": _tag(cfg, pol, store)}
        jp = os.path.join(work_root, f"job_{i:03d}.json")
        with open(jp, "w") as fh:
            json.dump(job, fh)
        jobs.append((jp, os.path.join(out_dir, job["tag"] + ".json")))
    out, running, pending = [], [], list(jobs)
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    t0 = time.time()
    while pending or running:
        while pending and len(running) < max(1, workers):
            jp, op = pending.pop(0)
            proc = subprocess.Popen([sys.executable, "-m", "residency.cli", "cell", "--job", jp, "--out", op],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env)
            running.append((proc, jp, op))
        time.sleep(0.2)
        still = []
        for proc, jp, op in running:
            if proc.poll() is None:
                still.append((proc, jp, op))
                continue
            err = proc.stderr.read().decode(errors="replace") if proc.stderr else ""
            if proc.returncode != 0 or not os.path.exists(op):
                raise RuntimeError(f"cell {jp} failed (exit {proc.returncode}):\n{err[-4000:]}")
            with open(op) as fh:
                r = json.load(fh)
            out.append(r)
            if progress:
                s = r["summary"]
                lh = s.get("lhbench") or {}
                p50 = (lh.get("drt_days") or {}).get("p50")
                print(f"  [{len(out):>3}/{len(jobs)}] {r['policy']:<22} {r['store']['label']:<48} seed={r['seed']} "
                      f"p50={p50 if p50 is not None else float('nan'):6.1f} d met={100 * lh.get('deadline_met_fraction', 0):5.1f}% "
                      f"({r['wall_seconds']:.0f}s)", flush=True)
        running = still
    print(f"  {len(out)} runs in {time.time() - t0:.0f}s -> {out_dir}", flush=True)
    return out


def write_summary(runs: list[dict], out_dir: str):
    with open(os.path.join(out_dir, "combined.json"), "w") as fh:
        json.dump([{k: r[k] for k in ("config", "policy", "policy_spec", "store", "seed", "summary", "rewrite",
                                       "budget", "table", "probe", "wall_seconds")} for r in runs], fh, indent=1)
    with open(os.path.join(out_dir, "summary.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["policy", "store", "layout", "seed", "horizon", "n", "censored", "p50", "p95", "met", "median_completed",
                    "miss_rate", "to_rewritten", "to_unreferenced", "to_marker", "to_physical", "commit_d", "rewrite_d",
                    "unlink_d", "expire_d", "bytes_policy", "bytes_cow", "amp", "probe_checks", "probe_disagreements", "wall_s"])
        for r in runs:
            s = r["summary"]; lh = s.get("lhbench") or {}; er = s.get("erasure") or {}
            st = er.get("stage_mean_days") or {}
            w.writerow([r["policy"], r["store"]["label"], r["config"]["layout"], r["seed"], r["config"]["horizon_days"],
                        s["n_obligations"], s["n_censored"], (lh.get("drt_days") or {}).get("p50"),
                        (lh.get("drt_days") or {}).get("p95"), lh.get("deadline_met_fraction"),
                        (er.get("days") or {}).get("median"), er.get("miss_rate"), lh.get("median_days_to_rewritten"),
                        lh.get("median_days_to_unreferenced"), lh.get("median_days_to_delete_marker"),
                        lh.get("median_days_to_physical"), st.get("commit"), st.get("rewrite"), st.get("unlink"),
                        st.get("expire"), r["rewrite"]["bytes_policy"], r["rewrite"]["bytes_cow"],
                        lh.get("deletion_amplification"), r["probe"]["checks"], r["probe"]["disagreements"], r["wall_seconds"]])


def cmd_run(args):
    seeds = tuple(args.seeds) if args.seeds else DEFAULT_SEEDS
    cells = suite(args.suite, seeds)
    out_dir = args.out or os.path.join(DEFAULT_OUT, args.suite)
    runs = sweep(cells, args.workers, out_dir)
    write_summary(runs, out_dir)
    from .verify import run_all
    ok, lines = run_all(runs)
    print("\n".join(lines))
    return 0 if ok else 1


def cmd_cell(args):
    if args.job:
        with open(args.job) as fh:
            job = json.load(fh)
        from .policies import Policy
        r = run_cell(Config(**job["config"]), Policy(**job["policy"]), StoreSpec.from_dict(job["store"]), job["work"], keep=args.keep)
    else:
        cfg = replace(SMOKE if args.smoke else BASE, seed=args.seed, layout=args.layout)
        work = args.work or os.path.join(tempfile.mkdtemp(prefix="residency-cell-"), "cell")
        r = run_cell(cfg, args.policy, store_preset(args.store), work, keep=args.keep)
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(r, fh)
    else:
        print(json.dumps({k: v for k, v in r.items() if k not in ("timeline", "obligations", "config", "policy_spec")}, indent=1))
    return 0


def cmd_verify(args):
    from .verify import main as vmain
    return vmain(args.results)


def cmd_reconcile(args):
    from .reconcile import claims, load_erasure, load_lhbench, render_markdown
    lh = load_lhbench(args.lhbench)
    er = load_erasure(args.erasure)
    model = None
    if args.model and os.path.exists(args.model):
        with open(args.model) as fh:
            model = json.load(fh)
    cl = claims(lh, er, model)
    md = render_markdown(cl)
    if args.out:
        with open(args.out, "w") as fh:
            fh.write(md)
        print(f"-> {args.out}")
    else:
        print(md)
    return 0


def cmd_backtest(args):
    from .verify import load
    runs = load(args.results)
    if not runs:
        print("nothing to back-test against")
        return 2
    seed = args.seed or runs[0]["seed"]
    from .policies import Policy
    want = {(r["policy"], r["store"]["label"], r["config"]["layout"]): r for r in runs if r["seed"] == seed}
    cells = [(Config(**r["config"]), Policy(**r["policy_spec"]), StoreSpec.from_dict(r["store"])) for r in want.values()]
    got = sweep(cells, args.workers, tempfile.mkdtemp(prefix="residency-backtest-"))
    bad = []
    for r in got:
        ref = want[(r["policy"], r["store"]["label"], r["config"]["layout"])]
        a, b = r["summary"], ref["summary"]
        for path in (("lhbench", "drt_days", "p50"), ("lhbench", "drt_days", "p95"), ("lhbench", "deadline_met_fraction"),
                     ("erasure", "miss_rate")):
            x, y = a, b
            for k in path:
                x, y = (x or {}).get(k), (y or {}).get(k)
            if (x is None) != (y is None) or (x is not None and abs(x - y) > 1e-9):
                bad.append(f"{r['policy']} {r['store']['label']} {'.'.join(path)}: got {x} want {y}")
        if r["rewrite"]["commits"] != ref["rewrite"]["commits"]:
            bad.append(f"{r['policy']} {r['store']['label']} rewrite commits: got {r['rewrite']['commits']} want {ref['rewrite']['commits']}")
    for b in bad:
        print("FAIL", b)
    print(f"BACKTEST {'OK' if not bad else 'FAILED'}: {len(got)} cells for seed {seed}" + ("" if not bad else f", {len(bad)} deviations"))
    return 1 if bad else 0


def cmd_demo(args):
    """One real table, one subject, two answers."""
    from pyiceberg.expressions import EqualTo
    from . import probe as P
    from .stores import ObjectStore
    from .table import HarnessTable
    from .workload import KEY, Workload
    cfg = replace(SMOKE, seed=args.seed, horizon_days=12, warmup_days=3, tail_days=5)
    work = tempfile.mkdtemp(prefix="residency-demo-")
    wh = os.path.join(work, "warehouse")
    wl = Workload(cfg)
    table = HarnessTable(wh).create()
    store = ObjectStore(wh, StoreSpec("versioned", noncurrent_days=7))
    h = 0
    for day in range(6):
        for b in wl.ingest(day):
            h = b.commit_hour
            table.append(wl.arrow(b), h)
            store.sync(h)
    subject = sorted(wl.seen)[len(wl.seen) // 2]
    led = P.ledger_before(table.table, subject, store, store.key_of, h, read_back=P.read_back_local(subject))
    n_before = table.table.scan(row_filter=EqualTo(KEY, subject)).to_arrow().num_rows
    h += 1
    res = table.delete_subjects([subject], h)
    store.sync(h)
    obs = P.observe(led, store, h, table=table.table, read_back=P.read_back_local(subject))
    print(f"subject {subject}: rows visible before DELETE = {n_before}")
    print(f"  what the table says : {'0 rows' if obs['logically_deleted'] else 'rows still visible'} "
          f"(snapshot {table.table.current_snapshot().snapshot_id})")
    print(f"  what the bucket says: {sum(f['versions_remaining'] for f in obs['files'])} object version(s), "
          f"{obs['bytes_remaining']:,} bytes, {obs['subject_rows_readable']} of the subject's rows readable by opening them")
    for f in obs["files"]:
        print(f"    {f['key']}  versions={f['versions_remaining']} bytes={f['bytes_remaining']:,} rows_readable={f['subject_rows_readable']}")
    print(f"  files rewritten by the DELETE: {len(res['removed'])} removed, {len(res['added'])} added")
    import shutil
    shutil.rmtree(work, ignore_errors=True)
    return 0


def cmd_plan(args):
    try:
        p = plan(window_days=args.window, snapshot_retention_days=args.retention, expire_cadence_days=args.expire_cadence,
                 orphan_cadence_days=args.orphan_cadence, orphan_older_than_days=args.orphan_older_than)
    except ValueError as e:
        print(f"INFEASIBLE: {e}")
        return 1
    print(render(p, args.prefix))
    print(f"guaranteed max NoncurrentDays for a {args.window}-day window: "
          f"{max_noncurrent_days(args.window, snapshot_retention_days=args.retention, expire_cadence_days=args.expire_cadence, orphan_cadence_days=args.orphan_cadence, orphan_older_than_days=args.orphan_older_than)}")
    return 0


def cmd_report(args):
    from .report import group, render
    from .verify import load
    runs = load(args.results)
    if not runs:
        print("no results")
        return 2
    g = group(runs)
    print(render(g))
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(g, fh, indent=1)
        print(f"-> {args.json}")
    return 0


def cmd_cost(args):
    from .costmodel import Assumptions, from_run, rule_value
    from .verify import load
    a = None
    if args.results:
        for r in load(args.results):
            if r["policy"] == args.policy and (args.store is None or r["store"]["label"] == args.store):
                a = from_run(r)
                break
        if a is None:
            print(f"no run for policy {args.policy!r} in {args.results}")
            return 2
    else:
        a = Assumptions()
    print(json.dumps(rule_value(a, args.rule_days), indent=1))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog="residency", description=f"residency harness {__version__}")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run"); r.add_argument("--suite", default="smoke"); r.add_argument("--seeds", type=int, nargs="*")
    r.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) // 2)); r.add_argument("--out"); r.set_defaults(fn=cmd_run)
    c = sub.add_parser("cell"); c.add_argument("--policy", default="A-default", choices=sorted(POLICIES))
    c.add_argument("--store", default="s3-versioned-7d", choices=sorted(STORES)); c.add_argument("--seed", type=int, default=DEFAULT_SEEDS[0])
    c.add_argument("--layout", default="scattered"); c.add_argument("--smoke", action="store_true"); c.add_argument("--work")
    c.add_argument("--keep", action="store_true"); c.add_argument("--job"); c.add_argument("--out"); c.set_defaults(fn=cmd_cell)
    v = sub.add_parser("verify"); v.add_argument("--results", default=os.path.join(DEFAULT_OUT, "combined")); v.set_defaults(fn=cmd_verify)
    rc = sub.add_parser("reconcile"); rc.add_argument("--lhbench", default=os.path.join(ROOT, "results"))
    rc.add_argument("--erasure", default=os.path.join(ROOT, "residency_data", "erasure_harness.json"))
    rc.add_argument("--model", default=os.path.join(ROOT, "residency_data", "erasure_model.json")); rc.add_argument("--out"); rc.set_defaults(fn=cmd_reconcile)
    b = sub.add_parser("backtest"); b.add_argument("--results", required=True); b.add_argument("--seed", type=int)
    b.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) // 2)); b.set_defaults(fn=cmd_backtest)
    d = sub.add_parser("demo"); d.add_argument("--seed", type=int, default=DEFAULT_SEEDS[0]); d.set_defaults(fn=cmd_demo)
    p = sub.add_parser("plan"); p.add_argument("--window", type=int, default=30); p.add_argument("--retention", type=int, default=5)
    p.add_argument("--expire-cadence", type=int, default=1); p.add_argument("--orphan-cadence", type=int, default=0)
    p.add_argument("--orphan-older-than", type=int, default=0); p.add_argument("--prefix", default="warehouse/prod/events/"); p.set_defaults(fn=cmd_plan)
    rp = sub.add_parser("report"); rp.add_argument("--results", required=True); rp.add_argument("--json"); rp.set_defaults(fn=cmd_report)
    k = sub.add_parser("cost"); k.add_argument("--results"); k.add_argument("--policy", default="lhbench-baseline"); k.add_argument("--store")
    k.add_argument("--rule-days", type=float, default=7.0); k.set_defaults(fn=cmd_cost)
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
