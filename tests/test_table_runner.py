"""Real-table tests: PyIceberg on a SQLite catalog, no network.  Kept small."""
import os

import pytest
from pyiceberg.expressions import EqualTo

from residency import probe as P
from residency.policies import PRESETS, Policy
from residency.runner import run_cell
from residency.stores import ObjectStore, StoreSpec
from residency.table import HarnessTable, subjects_in
from residency.vclock import HOURS_PER_DAY, at
from residency.workload import KEY, Config, Workload

TINY = dict(horizon_days=40, warmup_days=3, tail_days=22, rows_per_day=120, n_subjects=150, dsar_rate_per_day=1.5)


@pytest.fixture
def small_table(tmp_path):
    cfg = Config(seed=7, **TINY)
    wl = Workload(cfg)
    wh = str(tmp_path / "wh")
    table = HarnessTable(wh).create()
    store = ObjectStore(wh, StoreSpec("versioned", noncurrent_days=7))
    h = 0
    for day in range(4):
        for b in wl.ingest(day):
            h = b.commit_hour
            table.append(wl.arrow(b), h)
            store.sync(h)
    return cfg, wl, table, store, h


def test_cow_delete_keeps_old_file_referenced_and_readable(small_table):
    cfg, wl, table, store, h = small_table
    subject = sorted(wl.seen)[0]
    led = P.ledger_before(table.table, subject, store, store.key_of, h, read_back=P.read_back_local(subject))
    assert led["entries"]
    res = table.delete_subjects([subject], h + 1)
    store.sync(h + 1)
    assert res["committed"] and res["removed"] and res["added"]
    obs = P.observe(led, store, h + 1, table=table.table, read_back=P.read_back_local(subject))
    assert obs["logically_deleted"] is True and obs["physically_erased"] is False and obs["subject_rows_readable"] > 0
    refs = table.referenced()
    assert all(p in refs for p in res["removed"])
    assert table.delete_subjects([10 ** 9], h + 2)["committed"] is False      # nothing matched: no commit
    assert table.delete_subjects([], h + 2)["committed"] is False


def test_file_granular_rewrite_drops_only_chosen_files(small_table):
    cfg, wl, table, store, h = small_table
    cur = table.current_data_files()
    target = sorted(cur)[0]
    victims = sorted(subjects_in(target))[:3]
    res = table.rewrite_files([target], victims, h + 1)
    store.sync(h + 1)
    assert set(res["removed"]) == {target}
    new = table.current_data_files()
    assert target not in new and len(new) == len(cur)
    for p in new:
        assert not (subjects_in(p) & set(victims)) or p in cur      # dropped from the rewritten file only
    assert table.rewrite_files(["/nowhere.parquet"], victims, h + 2)["committed"] is False
    # drop everything -> file removed with no replacement
    t2 = sorted(table.current_data_files())[0]
    res2 = table.rewrite_files([t2], subjects_in(t2), h + 3)
    assert res2["removed"] and not res2["added"]


def test_expiry_keeps_current_and_unlink_is_the_callers_job(small_table):
    cfg, wl, table, store, h = small_table
    subject = sorted(wl.seen)[1]
    res = table.delete_subjects([subject], h + 1)
    old = next(iter(res["removed"]))
    assert table.expire_snapshots(h + 2, 5 * HOURS_PER_DAY) == [] or old in table.referenced()
    later = h + 1 + 6 * HOURS_PER_DAY
    ids = table.expire_snapshots(later, 5 * HOURS_PER_DAY)
    assert ids and len(table.live_snapshots()) == 1
    assert old not in table.referenced() and os.path.exists(old)              # metadata only
    store.sync(later)
    store.delete(store.key_of(old), later)
    assert os.path.exists(old)                                                 # delete marker
    freed = []
    for hh in range(later, later + 9 * HOURS_PER_DAY):
        freed += store.run_lifecycle(hh)
    assert store.key_of(old) in freed and not os.path.exists(old)
    assert table.expire_snapshots(later + 1, 5 * HOURS_PER_DAY) == []          # nothing left to expire


def test_bucketed_layout_partitions_by_subject(tmp_path):
    cfg = Config(seed=2, layout="bucketed", n_buckets=4, **TINY)
    wl = Workload(cfg)
    table = HarnessTable(str(tmp_path / "wh"), layout="bucketed", n_buckets=4).create()
    for b in wl.ingest(0):
        table.append(wl.arrow(b), b.commit_hour)
    files = table.current_data_files()
    assert 2 <= len(files) <= 4
    assert all("subject_bucket=" in p for p in files)


def _run(policy, spec, tmp_path, seed=11, **over):
    cfg = Config(seed=seed, **{**TINY, **over})
    return run_cell(cfg, policy, spec, str(tmp_path / "cell"), probe_every=2)


def test_cow_unversioned_cell_completes_with_no_bucket_stage(tmp_path):
    r = _run("D-cow-default", StoreSpec("unversioned"), tmp_path)
    s = r["summary"]
    assert s["n_obligations"] > 0 and s["n_censored"] == 0 and s["ordered"]
    assert s["erasure"]["stage_mean_days"]["rewrite"] == 0.0
    assert s["erasure"]["stage_mean_days"]["expire"] == 0.0
    assert s["erasure"]["stage_mean_days"]["unlink"] >= 5
    assert 0 <= s["erasure"]["stage_mean_days"]["commit"] <= 1.0
    assert r["probe"]["disagreements"] == 0 and r["probe"]["checks"] > 0
    assert r["rewrite"]["bytes_policy"] == 0 and r["rewrite"]["bytes_cow"] > 0
    assert s["lhbench"]["median_days_to_logical"] < 1.5
    assert r["table"]["files_unlinked_by_expiry"] > 0


def test_versioned_no_rule_never_completes(tmp_path):
    r = _run("D-cow-default", StoreSpec("versioned", noncurrent_days=None), tmp_path)
    s = r["summary"]
    assert s["n_closed"] == 0 and s["n_censored"] == s["n_obligations"]
    assert s["erasure"]["miss_rate"] == 1.0 and s["lhbench"]["deadline_met_fraction"] == 0.0
    assert s["lhbench"]["median_days_to_delete_marker"] is not None      # unlinked, never freed


def test_lhbench_baseline_defers_and_orphan_job_reclaims(tmp_path):
    pol = Policy("lh", "mor", "cadence_fifo", compact_interval_days=3, daily_rewrite_budget_bytes=10 ** 9,
                 snapshot_retention_days=4, expire_interval_days=3, orphan_interval_days=4, orphan_older_than_days=0,
                 expiry_deletes_files=False)
    r = _run(pol, StoreSpec("unversioned"), tmp_path)
    s = r["summary"]
    assert s["n_censored"] == 0 and s["ordered"]
    assert 0 < s["erasure"]["stage_mean_days"]["rewrite"] <= 3
    assert r["table"]["files_unlinked_by_expiry"] == 0 and r["table"]["files_unlinked_by_orphan"] > 0
    assert s["lhbench"]["median_days_to_unreferenced"] < s["lhbench"]["median_days_to_delete_marker"]
    assert r["probe"]["disagreements"] == 0


def test_budget_is_enforced_and_reported(tmp_path):
    pol = Policy("tight", "mor", "cadence_fifo", compact_interval_days=1, daily_rewrite_budget_bytes=2000,
                 snapshot_retention_days=2, expire_interval_days=1, orphan_interval_days=1, orphan_older_than_days=0,
                 expiry_deletes_files=True)
    r = _run(pol, StoreSpec("unversioned"), tmp_path)
    over = [d for d in r["timeline"] if d["bytes_rewritten_today"] > 2000]
    assert len(over) <= r["budget"]["forced_days"]
    assert r["budget"]["utilisation"] is not None


def test_deadline_edf_meets_floor_and_versioning_adds_n(tmp_path):
    pol = Policy("dl", "mor", "deadline_edf", daily_rewrite_budget_bytes=10 ** 9, snapshot_retention_days=4,
                 expire_interval_days=3, orphan_interval_days=4, orphan_older_than_days=0, expiry_deletes_files=False,
                 pull_forward=True, urgency_horizon_days=10)
    base = _run(pol, StoreSpec("unversioned"), tmp_path)
    p50 = base["summary"]["lhbench"]["drt_days"]["p50"]
    assert 4 <= p50 <= 4 + 3 + 1.5                              # retention floor plus the cadence it waits for
    v3 = _run(pol, StoreSpec("versioned", noncurrent_days=3), tmp_path)
    delta = v3["summary"]["lhbench"]["drt_days"]["p50"] - p50
    assert 3 - 0.01 <= delta <= 4 + 0.01                        # N .. N+1 (midnight rounding)


def test_request_on_last_cohort_day_is_not_censored_and_zero_demand(tmp_path):
    r = _run("E-asap-daily", StoreSpec("versioned", noncurrent_days=1), tmp_path, dsar_rate_per_day=0.0)
    assert r["summary"]["n_obligations"] == 0 and r["summary"]["lhbench"] == {}
    r2 = _run("E-asap-daily", StoreSpec("versioned", noncurrent_days=1), tmp_path, tail_days=20)
    assert r2["summary"]["n_censored"] == 0
