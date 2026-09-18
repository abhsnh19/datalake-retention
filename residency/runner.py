"""The virtual-time event loop: one cell = (config, policy, store, seed).

Hour by hour: ingest batches commit at their hours; erasure requests arrive
at a random hour and are committed in the nightly batch (S0); once a day the
policy decides (rewrite files, run expiry, run orphan cleanup) and the
runner executes the decision on the real table; every hour the bucket runs
its own lifecycle.  For every data file ever written the runner keeps the
exact subject set (read back from the Parquet object) and the four instants
that matter -- superseded, unreferenced, unlinked, freed -- and an
obligation advances a stage only when EVERY file that ever held its rows has
passed it.  A sample of requests is verified by ``probe.py`` (list object
versions + open the surviving objects), so the ledger and an independent
observer must agree.
"""
from __future__ import annotations

import os
import shutil
import statistics
import time
from dataclasses import dataclass, field

import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import probe as P
from .ledger import Ledger
from .policies import Policy, PRESETS, coordinated_slack_hours, decide, rewrite_due_pass
from .stores import ObjectStore, StoreSpec
from .table import HarnessTable, _strip
from .vclock import HOURS_PER_DAY, at, calendar, day_of, hour_of_day
from .workload import KEY, Config, Workload, calibration


@dataclass
class FileState:
    path: str
    put_at: int
    size: int
    subjects: set
    newest_day: int
    superseded_at: int | None = None
    unreferenced_at: int | None = None
    unlinked_at: int | None = None
    freed_at: int | None = None


def _read_file(path: str) -> tuple[set, int]:
    t = pq.read_table(_strip(path), columns=[KEY, "event_day"])
    subs = set(pc.unique(t[KEY]).to_pylist())
    newest = pc.max(t["event_day"]).as_py() if t.num_rows else -1
    return subs, int(newest if newest is not None else -1)


def run_cell(cfg: Config, policy: Policy | str, store: StoreSpec, work_dir: str,
             probe_every: int = 8, keep: bool = False) -> dict:
    if isinstance(policy, str):
        policy = PRESETS[policy]
    t_start = time.time()
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
    os.makedirs(work_dir)
    warehouse = os.path.join(work_dir, "warehouse")

    wl = Workload(cfg)
    table = HarnessTable(warehouse, layout=cfg.layout, n_buckets=cfg.n_buckets).create()
    bucket = ObjectStore(warehouse, store)
    prefix = bucket.key_of(table.location()) + os.sep
    ledger = Ledger(cfg.deadline_days * HOURS_PER_DAY)

    files: dict[str, FileState] = {}
    subject_paths: dict[int, set] = {}
    rows_per_subject: dict[int, int] = {}
    pending_commit: list = []                 # requests arrived, not yet in a nightly batch
    due_pass: dict[int, int] = {}
    daily_passes = calendar(lambda d: True, policy.maintenance_hour, cfg.horizon_days)
    retention_h = policy.snapshot_retention_days * HOURS_PER_DAY
    older_h = policy.orphan_older_than_days * HOURS_PER_DAY
    slack_h = coordinated_slack_hours(policy, store.tail_days, store.rounding_days)

    stats = {"rewrite_commits": 0, "rewrite_files": 0, "bytes_policy": 0, "bytes_cow": 0, "bytes_added": 0,
             "expire_runs": 0, "orphan_runs": 0, "snapshots_expired": 0, "files_unlinked_by_expiry": 0,
             "files_unlinked_by_orphan": 0, "forced_over_budget_days": 0, "max_live_snapshots": 0}
    probe_stats = {"checks": 0, "rows_readable_after_delete": 0, "disagreements": 0, "erased_confirmed": 0}
    ledgers: dict[int, dict] = {}
    timeline: list[dict] = []
    today = {"policy": 0, "cow": 0, "expired": False, "orphaned": False, "freed": 0}

    def register(path: str, now: int, size: int):
        subs, newest = _read_file(path)
        files[path] = FileState(path, now, size, subs, newest)
        for s in subs:
            subject_paths.setdefault(s, set()).add(path)
            ob = ledger.by_subject.get(s)
            if ob is not None and not ob.closed:
                ob.files.add(path)

    def apply(res: dict, now: int, cow: bool):
        if not res["committed"]:
            return
        stats["rewrite_commits"] += 1
        for p, size in res["removed"].items():
            fs = files.get(p)
            if fs is not None and fs.superseded_at is None:
                fs.superseded_at = now
            stats["rewrite_files"] += 1
            if cow:
                stats["bytes_cow"] += size
                ledger.bytes_rewritten_cow += size
                today["cow"] += size
            else:
                stats["bytes_policy"] += size
                ledger.bytes_rewritten += size
                today["policy"] += size
        bucket.sync(now)
        for p, size in res["added"].items():
            register(p, now, size)
            stats["bytes_added"] += size

    def current_view() -> dict:
        out = {}
        for p, size in table.current_data_files().items():
            fs = files.get(p)
            if fs is None:                      # a file the runner never saw (should not happen)
                register(p, 0, size)
                fs = files[p]
            out[p] = (fs.subjects, size, fs.newest_day)
        return out

    def mark_unreferenced(now: int):
        refs = table.referenced()
        for p, fs in files.items():
            if fs.unreferenced_at is None and p not in refs:
                fs.unreferenced_at = now
        return refs

    def unlink(now: int, refs: set, older_than_h: int | None, by: str) -> int:
        n = 0
        for key in bucket.live_keys(prefix):
            path = bucket.path_of(key)
            if path in refs:
                continue
            v = bucket.versions[key]
            if older_than_h is not None and v.put_at > now - older_than_h:
                continue
            bucket.delete(key, now)
            n += 1
            fs = files.get(path)
            if fs is not None:
                if fs.unreferenced_at is None:
                    fs.unreferenced_at = now
                if fs.unlinked_at is None:
                    fs.unlinked_at = now
                if v.expired_at is not None and fs.freed_at is None:
                    fs.freed_at = v.expired_at
        stats[f"files_unlinked_by_{by}"] += n
        return n

    def settle(now: int):
        for ob in ledger.pending():
            if ob.commit is None:
                continue
            fl = [files[p] for p in ob.files]
            if ob.rewritten is None:
                if any(f.superseded_at is None for f in fl):
                    continue
                ob.rewritten = max([ob.commit] + [f.superseded_at for f in fl])
            if ob.unreferenced is None:
                if any(f.unreferenced_at is None for f in fl):
                    continue
                ob.unreferenced = max([ob.rewritten] + [f.unreferenced_at for f in fl])
            if ob.unlinked is None:
                if any(f.unlinked_at is None for f in fl):
                    continue
                ob.unlinked = max([ob.unreferenced] + [f.unlinked_at for f in fl])
            if any(f.freed_at is None for f in fl):
                continue
            ob.physical = max([ob.unlinked] + [f.freed_at for f in fl])
            probe_end(ob.rid, now)

    def probe_before(rid: int, subject: int, now: int):
        if probe_every <= 0 or rid % probe_every != 0:
            return
        ledgers[rid] = P.ledger_before(table.table, subject, bucket, bucket.key_of, now,
                                       read_back=P.read_back_local(subject))

    def probe_after_commit(rid: int, subject: int, now: int, logical: bool):
        led = ledgers.get(rid)
        if led is None:
            return
        obs = P.observe(led, bucket, now, table=table.table if logical else None, read_back=P.read_back_local(subject))
        probe_stats["checks"] += 1
        if logical and obs["logically_deleted"] is not True:
            probe_stats["disagreements"] += 1
        if obs["physically_erased"]:
            probe_stats["disagreements"] += 1          # bytes cannot be gone at commit
        if (obs["subject_rows_readable"] or 0) > 0:
            probe_stats["rows_readable_after_delete"] += 1

    def probe_end(rid: int, now: int):
        led = ledgers.get(rid)
        if led is None:
            return
        obs = P.observe(led, bucket, now, read_back=P.read_back_local(ledger.obligations[rid].subject))
        probe_stats["checks"] += 1
        if obs["physically_erased"]:
            probe_stats["erased_confirmed"] += 1
        else:
            probe_stats["disagreements"] += 1

    horizon_h = cfg.horizon_days * HOURS_PER_DAY
    batches_by_hour: dict[int, list] = {}
    arrivals_by_hour: dict[int, list] = {}
    h = 0
    while h < horizon_h:
        day, hod = day_of(h), hour_of_day(h)
        if hod == 0:
            today = {"policy": 0, "cow": 0, "expired": False, "orphaned": False, "freed": 0}
            for r in wl.dsars(day):
                arrivals_by_hour.setdefault(r.arrival, []).append(r)
            for b in wl.ingest(day):
                batches_by_hour.setdefault(b.commit_hour, []).append(b)
        # ---- ingest
        for b in batches_by_hour.pop(h, ()):
            for s in b.subjects:
                rows_per_subject[s] = rows_per_subject.get(s, 0) + 1
            added = table.append(wl.arrow(b), h)
            bucket.sync(h)
            for p, size in added.items():
                register(p, h, size)
        # ---- arrivals
        for r in arrivals_by_hour.pop(h, ()):
            ob = ledger.open(r.rid, r.subject, r.arrival, r.burst)
            ob.rows = rows_per_subject.get(r.subject, 0)
            ob.files.update(p for p in subject_paths.get(r.subject, ()) if files[p].freed_at is None)
            pending_commit.append(ob)
        # ---- nightly batch: S0 commit
        if hod == cfg.dsar_batch_hour and pending_commit:
            batch = [ob for ob in pending_commit if ob.arrival < h]
            pending_commit = [ob for ob in pending_commit if ob not in batch]
            for ob in batch:
                probe_before(ob.rid, ob.subject, h)
                ob.commit = h
                ob.files.update(p for p in subject_paths.get(ob.subject, ()) if files[p].freed_at is None)
            if policy.delete_mode == "cow":
                res = table.delete_subjects([ob.subject for ob in batch], h)
                apply(res, h, cow=True)
                for ob in batch:
                    probe_after_commit(ob.rid, ob.subject, h, logical=True)
            else:
                for ob in batch:
                    if policy.rewrite == "deadline_deferred":
                        due_pass[ob.rid] = rewrite_due_pass(ob.arrival, h, slack_h, cfg.deadline_days, daily_passes)
                    probe_after_commit(ob.rid, ob.subject, h, logical=False)
        # ---- the daily maintenance decision
        if hod == policy.maintenance_hour:
            decision = decide(policy, day, ledger, current_view(), h, due_pass)
            if decision.files_to_rewrite:
                drop = {o.subject for o in ledger.pending_rewrite()}
                res = table.rewrite_files(decision.files_to_rewrite, drop, h)
                apply(res, h, cow=False)
                if getattr(decision, "forced", False):
                    stats["forced_over_budget_days"] += 1
            today["do_expire"], today["do_orphan"] = decision.do_expire, decision.do_orphan
        # ---- snapshot expiry
        if hod == policy.expire_hour and today.get("do_expire"):
            ids = table.expire_snapshots(h, retention_h)
            stats["expire_runs"] += 1
            if ids:
                stats["snapshots_expired"] += len(ids)
                today["expired"] = True
                refs = mark_unreferenced(h)
                if policy.expiry_deletes_files:
                    unlink(h, refs, None, "expiry")
        # ---- orphan cleanup
        if hod == policy.orphan_hour and today.get("do_orphan"):
            stats["orphan_runs"] += 1
            refs = mark_unreferenced(h)
            n = unlink(h, refs, older_h if policy.orphan_older_than_days > 0 else None, "orphan")
            today["orphaned"] = n > 0
        # ---- the bucket's own policy
        for key in bucket.run_lifecycle(h):
            fs = files.get(bucket.path_of(key))
            if fs is not None and fs.freed_at is None:
                fs.freed_at = h
            today["freed"] += 1
        settle(h)
        if hod == HOURS_PER_DAY - 1:
            stats["max_live_snapshots"] = max(stats["max_live_snapshots"], len(table.live_snapshots()))
            cur = table.current_data_files()
            open_obs = ledger.pending()
            timeline.append({"day": day, "n_data_files": len(cur), "table_bytes": sum(cur.values()),
                             "on_disk_bytes": bucket.bytes_on_disk(prefix), "open_obligations": len(open_obs),
                             "overdue_obligations": sum(1 for o in open_obs if h > o.deadline),
                             "bytes_rewritten_today": today["policy"], "bytes_cow_today": today["cow"],
                             "expired": today["expired"], "orphaned": today["orphaned"], "freed": today["freed"]})
        h += 1

    # ---- denominator for amplification (lhbench definition)
    total_rows = sum(rows_per_subject.values())
    on_disk = sum(f.size for f in files.values() if f.freed_at is None)
    bytes_per_row = max(1.0, on_disk / max(1, total_rows))
    ledger.subject_bytes_erased = int(sum(rows_per_subject.get(o.subject, 0) for o in ledger.obligations.values()) * bytes_per_row)

    summary = ledger.summary(horizon_h, cfg.warmup_days, cfg.tail_days)
    cap = policy.daily_rewrite_budget_bytes
    res = {
        "config": cfg.as_dict(), "policy": policy.name, "policy_spec": policy.as_dict(),
        "store": store.as_dict(), "seed": cfg.seed,
        "summary": summary,
        "calibration": calibration(wl.requests, rows_per_subject),
        "rewrite": {"commits": stats["rewrite_commits"], "files": stats["rewrite_files"],
                    "bytes_policy": stats["bytes_policy"], "bytes_cow": stats["bytes_cow"],
                    "bytes": stats["bytes_policy"] + stats["bytes_cow"], "bytes_added": stats["bytes_added"]},
        "budget": {"cap_bytes": cap,
                   "days_over_cap": (sum(1 for d in timeline if cap is not None and d["bytes_rewritten_today"] > cap)),
                   "forced_days": stats["forced_over_budget_days"],
                   "max_bytes_day": max([d["bytes_rewritten_today"] for d in timeline] or [0]),
                   "utilisation": ((sum(d["bytes_rewritten_today"] for d in timeline) / (cap * len(timeline)))
                                   if cap and timeline else None)},
        "table": {"commits": table.commits, "data_files_written": len(files), "expire_runs": stats["expire_runs"],
                  "orphan_runs": stats["orphan_runs"], "snapshots_expired": stats["snapshots_expired"],
                  "files_unlinked_by_expiry": stats["files_unlinked_by_expiry"],
                  "files_unlinked_by_orphan": stats["files_unlinked_by_orphan"],
                  "max_live_snapshots": stats["max_live_snapshots"]},
        "store_summary": bucket.summary(), "probe": probe_stats,
        "timeline": timeline,
        "obligations": [o.as_dict() for o in ledger.obligations.values()],
        "virtual_hours": h, "wall_seconds": round(time.time() - t_start, 2),
    }
    if not keep:
        shutil.rmtree(work_dir, ignore_errors=True)
    return res
