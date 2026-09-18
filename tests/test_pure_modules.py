"""Clock, RNG, documented floors, feasibility, cost model: no Iceberg needed."""
import pytest

from residency import feasibility as F
from residency import formats as FM
from residency.costmodel import Assumptions, annual_cost, rounded, rule_value
from residency.rng import LCG, cumulative, power_law_weights
from residency.vclock import HOURS_PER_DAY, at, ceil_to_midnight, day_of, hour_of_day


# ---------------------------------------------------------------- vclock
def test_midnight_rounding_matches_the_aws_example():
    assert ceil_to_midnight(at(15, 10) + 3 * HOURS_PER_DAY) == at(19, 0)   # 1/15 10:30 + 3 d -> 1/19 00:00
    assert ceil_to_midnight(at(18, 0)) == at(18, 0)                         # a midnight is a ceiling of itself
    assert ceil_to_midnight(at(18, 0) + 1) == at(19, 0)
    assert ceil_to_midnight(0) == 0
    assert day_of(at(3, 23)) == 3 and hour_of_day(at(3, 23)) == 23


# ------------------------------------------------------------------- rng
def test_lcg_is_the_specified_stream_and_deterministic():
    g = LCG(20260823)
    first = g.u()
    assert abs(first - ((1664525 * 20260823 + 1013904223) % 2 ** 32) / 2 ** 32) < 1e-15
    a, b = LCG(5), LCG(5)
    assert [a.u() for _ in range(50)] == [b.u() for _ in range(50)]


def test_rng_edge_cases():
    g = LCG(1)
    with pytest.raises(ValueError):
        g.below_int(0)
    assert g.poisson(0) == 0 and g.poisson(-1) == 0
    assert g.sample([1, 2, 3], 10) and len(g.sample([1, 2, 3], 10)) == 3
    assert g.sample([], 3) == []
    with pytest.raises(ValueError):
        power_law_weights(0, 1.2)
    with pytest.raises(ValueError):
        power_law_weights(3, -1)
    w = power_law_weights(4, 1.0)
    assert w == [1.0, 0.5, 1 / 3, 0.25]
    cum = cumulative(w)
    idx = [g.weighted_index(cum) for _ in range(2000)]
    assert idx.count(0) > idx.count(3)                                    # heavy head
    assert set(idx) <= {0, 1, 2, 3}


def test_poisson_mean_is_lambda():
    g = LCG(9)
    n = 4000
    mean = sum(g.poisson(2.0) for _ in range(n)) / n
    assert abs(mean - 2.0) < 0.1


# --------------------------------------------------------------- formats
def test_hudi_floor_is_a_count_divided_by_cadence():
    assert FM.hudi_floor_days(10, 24) == pytest.approx(10 / 24)
    assert FM.hudi_floor_days(10, 1) == 10
    with pytest.raises(ValueError):
        FM.hudi_floor_days(10, 0)
    with pytest.raises(ValueError):
        FM.hudi_floor_days(-1, 1)
    assert FM.table_floor("hudi", 24)["floor_days"] == pytest.approx(10 / 24)
    assert FM.table_floor("iceberg")["floor_days"] == 5 and FM.table_floor("iceberg")["second_floor_days"] == 3
    assert FM.table_floor("delta")["floor_days"] == 7
    assert FM.table_floor("s3tables")["store_days"] == 10
    with pytest.raises(KeyError):
        FM.table_floor("orc")


# ----------------------------------------------------------- feasibility
def test_floor_days_carries_every_term():
    assert F.floor_days(22) == 1 + 5 + 1 + 22 + 1 == 30                         # erasure form, Java expiry
    assert F.floor_days(7, snapshot_retention_days=20, expire_cadence_days=7, orphan_cadence_days=15) == 1 + 20 + 7 + 15 + 7 + 1
    assert F.floor_days(7, orphan_cadence_days=1, orphan_older_than_days=3) == 1 + 5 + 1 + 3 + 7 + 1    # max(cadence, age)
    assert F.floor_days(7, rounding_days=0) == 1 + 5 + 1 + 7                        # a soft-delete duration has no midnight
    with pytest.raises(ValueError):
        F.floor_days(-1)
    with pytest.raises(ValueError):
        F.floor_days(7, snapshot_retention_days=None)


def test_max_noncurrent_days_and_zero_when_nothing_fits():
    assert F.max_noncurrent_days(30) == 22
    assert F.max_noncurrent_days(30, expire_cadence_days=7) == 16
    assert F.max_noncurrent_days(30, snapshot_retention_days=20, expire_cadence_days=7, orphan_cadence_days=15) == 0
    assert F.max_noncurrent_days(7) == 0


def test_thirty_second_check_and_lhbench_form_agree():
    ok = F.check(30, 8, 7)
    assert ok["feasible"] and ok["headroom_days"] == 15
    assert F.check(30, 8, None)["feasible"] is False
    assert F.check(30, 5 + 3, 23)["feasible"] is False
    with pytest.raises(ValueError):
        F.check(0, 8, 7)
    assert F.lhbench_check(30, 20, 7)["feasible"] and F.lhbench_check(30, 20, 7)["floor_days"] == 27
    assert F.lhbench_check(30, 20, 11)["feasible"] is False
    assert F.lhbench_check(30, 20, None)["feasible"] is False


def test_plan_sums_to_window_and_rejects_impossible():
    p = F.plan(30)
    assert (p["commit_days"] + p["rewrite_budget_days"] + p["snapshot_retention_days"] + p["expire_cadence_days"]
            + p["orphan_days"] + p["noncurrent_days"] + p["midnight_batch_days"] + p["margin_days"]) == 30
    with pytest.raises(ValueError):
        F.plan(7)
    with pytest.raises(ValueError):
        F.plan(30, snapshot_retention_days=20, expire_cadence_days=7, orphan_cadence_days=15)


def test_lifecycle_documents():
    rule = F.s3_lifecycle_document("warehouse/", 21)["Rules"][0]
    assert rule["NoncurrentVersionExpiration"] == {"NoncurrentDays": 21}
    assert "NewerNoncurrentVersions" not in rule["NoncurrentVersionExpiration"]
    assert rule["Expiration"]["ExpiredObjectDeleteMarker"] is True
    with pytest.raises(ValueError):
        F.s3_lifecycle_document("w/", 0)                    # AWS: positive integer
    doc = F.s3tables_maintenance_document()
    assert doc["settings"]["icebergUnreferencedFileRemoval"] == {"unreferencedDays": 3, "nonCurrentDays": 10}
    assert F.iceberg_properties(5)["history.expire.max-snapshot-age-ms"] == "432000000"


# ------------------------------------------------------------- costmodel
def test_cost_model_rounds_and_states_assumptions():
    a = Assumptions(residency_days=34, write_amp=3.2)
    rv = rule_value(a, 7)
    assert rv["assumptions"]["table_tb"] == 2400.0
    assert rv["no_rule_first_year"]["total_usd"] > rv["rule"]["total_usd"]
    assert rv["no_rule_year_two"]["total_usd"] > rv["no_rule_first_year"]["total_usd"]
    assert rv["slide"]["rule"].startswith("$")
    assert rounded(135048) == "$140,000" and rounded(8643) == "$8,600" and rounded(0) == "$0"
    assert annual_cost(a, None, 2)["noncurrent_tb"] == pytest.approx(3 * annual_cost(a, None, 1)["noncurrent_tb"])
    with pytest.raises(ValueError):
        Assumptions(annual_churn=2.0)
    with pytest.raises(ValueError):
        annual_cost(a, -1)
