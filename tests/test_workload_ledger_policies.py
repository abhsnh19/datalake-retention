"""Generator determinism and rules, ledger stage logic, policy decisions."""
import pytest

from residency.ledger import Ledger, Obligation
from residency.policies import (PRESETS, Policy, coordinated_slack_hours, decide, preset,
                                rewrite_due_pass)
from residency.vclock import HOURS_PER_DAY, at, calendar
from residency.workload import Config, Workload, calibration


# -------------------------------------------------------------- workload
def test_config_validation():
    with pytest.raises(ValueError):
        Config(layout="sorted")
    with pytest.raises(ValueError):
        Config(horizon_days=30, warmup_days=10, tail_days=20)     # no cohort day left
    with pytest.raises(ValueError):
        Config(files_per_day=0)
    with pytest.raises(ValueError):
        Config(dsar_burst_prob=1.5)
    with pytest.raises(ValueError):
        Config(dsar_batch_hour=24)
    assert Config().cohort_days() == (10, 55)


def test_workload_is_deterministic_and_obeys_the_two_rules():
    cfg = Config(seed=3, horizon_days=30, warmup_days=2, tail_days=10, rows_per_day=200, n_subjects=1000, dsar_rate_per_day=3.0)
    a, b = Workload(cfg), Workload(cfg)
    ra = rb = []
    for day in range(cfg.horizon_days):
        ra = ra + [(r.subject, r.arrival) for r in a.dsars(day)]
        rb = rb + [(r.subject, r.arrival) for r in b.dsars(day)]
        ia = [x.subjects for x in a.ingest(day)]
        ib = [x.subjects for x in b.ingest(day)]
        assert ia == ib
    assert ra == rb and len(ra) > 0
    subjects = [s for s, _ in ra]
    assert len(set(subjects)) == len(subjects)          # requested at most once
    days_with_requests = {h // HOURS_PER_DAY for _, h in ra}
    assert min(days_with_requests) < cfg.warmup_days                      # requests on every day, cohort applied later
    assert max(days_with_requests) > cfg.horizon_days - cfg.tail_days
    # never re-ingested after the request
    c = Workload(cfg)
    requested_by_day = {}
    for day in range(cfg.horizon_days):
        for r in c.dsars(day):
            requested_by_day[r.subject] = day
        for batch in c.ingest(day):
            for s in batch.subjects:
                assert s not in requested_by_day


def test_layouts_and_files_per_day():
    base = dict(seed=5, horizon_days=20, warmup_days=1, tail_days=5, rows_per_day=90, n_subjects=50)
    clustered = Workload(Config(layout="clustered", **base)).ingest(0)[0].subjects
    assert clustered == sorted(clustered)
    scattered = Workload(Config(layout="scattered", **base)).ingest(0)[0].subjects
    assert scattered != sorted(scattered)
    multi = Workload(Config(files_per_day=3, **base)).ingest(0)
    assert len(multi) == 3 and sum(len(b.subjects) for b in multi) == len(scattered)
    hours = [b.commit_hour for b in multi]
    assert hours == sorted(hours) and len(set(hours)) == 3


def test_requests_only_name_subjects_with_rows_and_bursts_multiply():
    cfg = Config(seed=8, horizon_days=40, warmup_days=0, tail_days=5, rows_per_day=50, n_subjects=400,
                 dsar_rate_per_day=1.0, dsar_burst_prob=1.0, dsar_burst_multiplier=5.0)
    w = Workload(cfg)
    assert w.dsars(0) == []                             # nothing has rows yet
    w.ingest(0)
    got = []
    for day in range(1, 40):
        got += w.dsars(day)
        w.ingest(day)
    assert all(r.burst for r in got)
    assert len(got) > 40                                # 5 x 1 per day on average, on every day
    cal = calibration(got, {r.subject: 3 for r in got})
    assert cal["requests"] == len(got) and cal["burst_requests"] == len(got)


def test_everyone_erased_keeps_the_table_alive():
    cfg = Config(seed=1, horizon_days=10, warmup_days=0, tail_days=1, rows_per_day=5, n_subjects=2, dsar_rate_per_day=50)
    w = Workload(cfg)
    w.ingest(0)
    w.dsars(1)
    assert len(w.erased) == 2
    later = w.ingest(2)
    assert all(len(b.subjects) >= 1 for b in later)
    assert all(s == -1 for b in later for s in b.subjects)      # keep-alive rows, never a real subject
    assert w.dsars(3) == []                                     # and never requestable


# ---------------------------------------------------------------- ledger
def test_ledger_stage_logic_and_summary_views():
    L = Ledger(30 * HOURS_PER_DAY)
    o = L.open(0, 7, at(10, 5))
    with pytest.raises(ValueError):
        L.open(1, 7, at(10, 6))
    L.mark("commit", [7], at(11, 2))
    L.mark("rewritten", [7], at(12, 3))
    L.mark("unreferenced", [7], at(17, 4))
    L.mark("unlinked", [7], at(17, 4))
    L.mark("physical", [7], at(25, 0))
    assert o.ordered() and o.closed and o.met_deadline()
    assert o.drt_hours() == at(25, 0) - at(10, 5)
    p = L.open(1, 8, at(20, 0))
    L.mark("commit", [8], at(21, 2))
    L.mark("physical", [8], at(40, 0))         # same-tick backfill
    assert p.rewritten == p.unreferenced == p.unlinked == at(40, 0) and p.ordered()
    q = L.open(2, 9, at(30, 0))
    L.mark("commit", [9], at(31, 2))           # never closed -> censored
    with pytest.raises(KeyError):
        L.mark("vanished", [9], 0)
    s = L.summary(at(60, 0), warmup_days=0, tail_days=0)
    assert s["n_obligations"] == 3 and s["n_closed"] == 2 and s["n_censored"] == 1 and s["ordered"]
    lh, er = s["lhbench"], s["erasure"]
    assert lh["deadline_met_fraction"] == pytest.approx(2 / 3)                # two closed inside 30 d, one censored
    assert er["requests"] == 3 and er["completed"] == 2 and er["miss_rate"] == pytest.approx(1 / 3)   # the censored one
    assert er["stage_mean_days"]["commit"] == pytest.approx(((at(11, 2) - at(10, 5)) + (at(21, 2) - at(20, 0))) / 2 / 24)
    assert lh["stage_increments_days"]["rewrite"] == lh["median_days_to_rewritten"]
    # cohort exclusion: only the first obligation falls inside warmup..horizon-tail
    s2 = L.summary(at(60, 0), warmup_days=5, tail_days=45)
    assert s2["n_obligations"] == 1
    assert L.summary(at(60, 0), warmup_days=59)["n_obligations"] == 0


def test_obligation_out_of_order_detected():
    o = Obligation(0, 1, at(5, 0), at(35, 0), commit=at(6, 2), rewritten=at(5, 0))
    assert o.ordered() is False
    o2 = Obligation(0, 1, at(5, 0), at(35, 0), commit=at(4, 0))
    assert o2.ordered() is False


# -------------------------------------------------------------- policies
def test_policy_validation_and_presets():
    with pytest.raises(ValueError):
        Policy("x", "cow", "daily_full")
    with pytest.raises(ValueError):
        Policy("x", "mor", "none")
    with pytest.raises(ValueError):
        Policy("x", dirty_threshold=1.5)
    with pytest.raises(ValueError):
        Policy("x", daily_rewrite_budget_bytes=0)
    with pytest.raises(ValueError):
        Policy("x", maintenance_hour=5, expire_hour=4)
    with pytest.raises(KeyError):
        preset("nope")
    for name, p in PRESETS.items():
        assert p.name == name and Policy(**p.as_dict()) == p
    assert PRESETS["lhbench-baseline"].orphan_tail_days() == 15
    assert PRESETS["A-default"].orphan_tail_days() == 0
    p = Policy("never", expire_interval_days=0, orphan_interval_days=0)
    assert not p.expire_due(0) and not p.orphan_due(30)
    assert PRESETS["E-asap-daily"].expire_due(11) and PRESETS["A-default"].expire_due(14) and not PRESETS["A-default"].expire_due(15)


class _FakeLedger:
    def __init__(self, obs):
        self.obs = obs

    def pending_rewrite(self):
        return [o for o in self.obs if o.commit is not None and o.rewritten is None]

    def pending(self):
        return [o for o in self.obs if o.physical is None]


def _ob(rid, subject, arrival_day, deadline_days=30, **kw):
    o = Obligation(rid, subject, at(arrival_day, 0), at(arrival_day + deadline_days, 0), commit=at(arrival_day + 1, 2), **kw)
    return o


def test_cadence_fifo_respects_cadence_threshold_and_budget():
    pol = Policy("b", "mor", "cadence_fifo", compact_interval_days=7, dirty_threshold=0.5, daily_rewrite_budget_bytes=150)
    L = _FakeLedger([_ob(0, 1, 1), _ob(1, 2, 3)])
    current = {"f1": ({1, 5, 6, 7}, 100, 1), "f2": ({2, 9}, 100, 3), "f3": ({3}, 100, 3), "f4": ({1, 2}, 100, 4)}
    d = decide(pol, 8, L, current, at(8, 3))
    assert d.files_to_rewrite == set() and d.reason == "off-cadence"
    d = decide(pol, 7, L, current, at(7, 3))
    assert "f3" not in d.files_to_rewrite                       # clean file never selected
    assert "f1" not in d.files_to_rewrite                       # 1/4 dirty < 0.5
    assert d.files_to_rewrite == {"f4"} or d.files_to_rewrite == {"f2"}   # budget 150: one file
    assert d.forced is False
    pol0 = Policy("b0", "mor", "cadence_fifo", compact_interval_days=7, dirty_threshold=0.0, daily_rewrite_budget_bytes=10)
    d = decide(pol0, 7, L, current, at(7, 3))
    assert len(d.files_to_rewrite) == 1 and d.forced             # forward progress: one file over the whole budget
    assert d.files_to_rewrite == {"f1"} or d.files_to_rewrite == {"f4"}   # oldest obligation (subject 1) first


def test_deadline_edf_ranks_by_deadline_then_free_rides_and_pulls_forward():
    pol = Policy("d", "mor", "deadline_edf", daily_rewrite_budget_bytes=200, expire_interval_days=7, orphan_interval_days=15,
                 pull_forward=True, urgency_horizon_days=10)
    urgent = _ob(0, 1, 0)                     # deadline day 30
    later = _ob(1, 2, 15)                     # deadline day 45
    L = _FakeLedger([urgent, later])
    current = {"a": ({2, 3, 4}, 100, 0), "b": ({1}, 100, 0), "c": ({1, 2}, 100, 0)}
    d = decide(pol, 22, L, current, at(22, 3))
    assert d.files_to_rewrite == {"b", "c"}                     # both carry the urgent deadline; a (later) loses the budget
    assert d.do_expire is True                                   # within 10 days of a deadline: expiry pulled forward
    assert d.do_orphan is False                                  # nothing rewritten yet
    urgent.rewritten = at(22, 3)
    d = decide(pol, 23, L, {"a": ({2, 3, 4}, 100, 0)}, at(23, 3))
    assert d.do_orphan is True                                   # near + awaiting
    d = decide(pol, 3, _FakeLedger([later]), current, at(3, 3))
    assert d.do_expire is False and d.do_orphan is False         # nothing near, off cadence


def test_weekly_window_daily_full_and_deferred():
    L = _FakeLedger([_ob(0, 1, 2), _ob(1, 2, 2)])
    current = {"old": ({1}, 100, 1), "new": ({2}, 100, 13)}
    A = PRESETS["A-default"]
    assert decide(A, 13, L, current, at(13, 3)).files_to_rewrite == set()           # off cadence (13 % 7)
    assert decide(A, 14, L, current, at(14, 3)).files_to_rewrite == {"new"}          # weekly window: newest >= 7
    assert decide(A, 30, L, current, at(30, 3)).files_to_rewrite == {"old", "new"}   # monthly full
    assert decide(PRESETS["E-asap-daily"], 5, L, current, at(5, 3)).files_to_rewrite == {"old", "new"}
    C = PRESETS["C-coordinated"]
    d = decide(C, 5, L, current, at(5, 3), due_pass={0: at(4, 3), 1: at(9, 3)})
    assert d.files_to_rewrite == {"old"}
    assert decide(PRESETS["D-cow-default"], 7, L, current, at(7, 3)).files_to_rewrite == set()


def test_deferred_due_pass_math():
    pol = PRESETS["C-coordinated"]
    slack = coordinated_slack_hours(pol, 21, 1)
    assert slack == (5 + 1 + 0 + 21 + 1 + 1) * HOURS_PER_DAY
    daily = calendar(lambda d: True, 3, 200)
    arrival, commit = at(40, 9), at(41, 2)
    due = arrival + 30 * HOURS_PER_DAY - slack
    p = rewrite_due_pass(arrival, commit, slack, 30, daily)
    assert commit <= p <= due and p + HOURS_PER_DAY > due
    assert rewrite_due_pass(arrival, commit, coordinated_slack_hours(pol, 30, 1), 30, daily) == at(41, 3)   # too tight: next pass
    assert rewrite_due_pass(arrival, commit, slack, 30, []) == float("inf")
    lh = PRESETS["lhbench-baseline"]
    assert coordinated_slack_hours(lh, 7, 0) == (20 + 7 + 15 + 7 + 0 + 1) * HOURS_PER_DAY     # metadata-only expiry adds the orphan job
