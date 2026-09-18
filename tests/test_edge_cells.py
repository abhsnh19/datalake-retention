"""Edge cases through the full runner, plus verify / reconcile / report / cost on real data."""
import os

import pytest

from residency import verify as V
from residency.costmodel import from_run, rule_value
from residency.policies import PRESETS, Policy
from residency.report import group, render
from residency.runner import run_cell
from residency.stores import StoreSpec, preset
from residency.workload import Config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TINY = dict(horizon_days=40, warmup_days=3, tail_days=22, rows_per_day=120, n_subjects=150, dsar_rate_per_day=1.5)


def _run(policy, spec, tmp_path, seed=11, **over):
    return run_cell(Config(seed=seed, **{**TINY, **over}), policy, spec, str(tmp_path / "cell"), probe_every=3)


def test_expiry_that_never_runs_leaves_everything_referenced(tmp_path):
    never = Policy("never", "mor", "daily_full", expire_interval_days=0, orphan_interval_days=1, orphan_older_than_days=0)
    r = _run(never, StoreSpec("unversioned"), tmp_path)
    s = r["summary"]
    assert r["table"]["expire_runs"] == 0 and r["table"]["snapshots_expired"] == 0
    assert s["n_closed"] == 0 and s["n_censored"] == s["n_obligations"] > 0
    assert s["lhbench"]["median_days_to_rewritten"] is not None and s["lhbench"]["median_days_to_unreferenced"] is None


def test_object_lock_and_no_rule_never_free_but_unlink_happens(tmp_path):
    r = _run("E-asap-daily", preset("s3-object-lock-compliance"), tmp_path)
    s = r["summary"]
    assert s["n_closed"] == 0 and s["lhbench"]["median_days_to_delete_marker"] is not None
    assert r["store_summary"]["objects_noncurrent_on_disk"] > 0 and r["store_summary"]["expired"] == 0


def test_s3tables_managed_policy_gates_on_unreferenced_days(tmp_path):
    r = _run("s3tables-managed", preset("s3tables-default"), tmp_path, horizon_days=60, tail_days=35)
    s = r["summary"]
    assert s["n_censored"] == 0 and r["probe"]["disagreements"] == 0
    st = s["erasure"]["stage_mean_days"]
    assert st["unlink"] >= 3.0                          # unreferencedDays gate before the DELETE
    assert 10.0 <= st["expire"] <= 11.0                 # nonCurrentDays with midnight rounding
    assert r["table"]["files_unlinked_by_expiry"] == 0 and r["table"]["files_unlinked_by_orphan"] > 0


@pytest.mark.parametrize("layout", ["clustered", "bucketed"])
def test_layouts_run_end_to_end(tmp_path, layout):
    r = _run("lhbench-deadline", StoreSpec("unversioned"), tmp_path, layout=layout, n_buckets=4, horizon_days=60, tail_days=35)
    s = r["summary"]
    assert s["n_closed"] > 0 and s["ordered"] and r["probe"]["disagreements"] == 0
    if layout == "bucketed":
        assert r["rewrite"]["bytes_policy"] > 0


def test_retention_longer_than_horizon_censors_everything(tmp_path):
    long_ret = Policy("long", "mor", "daily_full", snapshot_retention_days=300, expire_interval_days=1, orphan_interval_days=1)
    r = _run(long_ret, StoreSpec("unversioned"), tmp_path)
    s = r["summary"]
    assert s["n_closed"] == 0 and s["n_censored"] == s["n_obligations"]
    assert s["lhbench"]["drt_days"]["p50"] <= r["config"]["horizon_days"]


def test_verify_report_and_cost_on_real_cells(tmp_path):
    a = _run("lhbench-deadline", StoreSpec("unversioned"), tmp_path / "a", horizon_days=60, tail_days=35)
    b = _run("lhbench-deadline", StoreSpec("versioned", noncurrent_days=2), tmp_path / "b", horizon_days=60, tail_days=35)
    ok, lines = V.run_all([a, b])
    assert ok, "\n".join(lines)
    assert any("within rounding" in l for l in lines)
    g = {x["store"]: x for x in group([a, b])}
    assert len(g) == 2
    assert g["S3 · versioned + NoncurrentDays 2"]["p50_mean"] - g["S3 · versioning off"]["p50_mean"] >= 2 - 0.01
    assert "lhbench-deadline" in render(list(g.values()))
    rv = rule_value(from_run(a), 7)
    assert rv["assumptions"]["residency_source"].startswith("run lhbench-deadline")
    assert rv["slide"]["no_rule_first_year"].startswith("$")


def test_reconcile_runs_on_the_committed_data():
    from residency.reconcile import claims, load_erasure, load_lhbench, render_markdown
    lh = load_lhbench(os.path.join(ROOT, "results"))
    er = load_erasure(os.path.join(ROOT, "residency_data", "erasure_harness.json"))
    assert len(lh) == 82 and len(er["runs"]) == 154
    cl = claims(lh, er)
    verdicts = {c["verdict"] for c in cl}
    assert verdicts <= {"agree", "operating", "stale", "conflict"} and len(cl) >= 10
    md = render_markdown(cl)
    assert "Provenance" in md and "2.89x" in md


def test_cli_smoke_entry_points(tmp_path, capsys):
    from residency.cli import main
    assert main(["plan", "--window", "30", "--retention", "20", "--expire-cadence", "7", "--orphan-cadence", "15"]) == 1
    assert "INFEASIBLE" in capsys.readouterr().out                    # 1 + 20 + 7 + 15 + 1 + 1 = 45 > 30
    assert main(["plan", "--window", "30", "--retention", "5", "--expire-cadence", "1"]) == 0
    out = capsys.readouterr().out
    assert "guaranteed max NoncurrentDays for a 30-day window: 22" in out
    with pytest.raises(SystemExit):
        main(["cell", "--policy", "nope"])
