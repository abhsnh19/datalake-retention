"""Synthetic table and erasure demand: the union of both generators.

From lhbench: rows spread over subjects by a power law (alpha), Poisson
erasure arrivals with bursts, three physical layouts (scattered = arrival
order, clustered = each day's batch sorted by subject, bucketed = partitioned
by bucket(subject_id)), an analysis cohort (warmup, tail).

From the erasure harness: a deterministic LCG, arrivals at a random hour of
the day so the nightly commit batch is measured, several commits per day.

Two rules both harnesses agreed on: a subject is requested at most once, and
a requested subject is never ingested again (re-ingestion is a different
problem from residency).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from .rng import LCG, cumulative, power_law_weights
from .vclock import HOURS_PER_DAY, at

LAYOUTS = ("scattered", "clustered", "bucketed")
KEY = "subject_id"
KEEPALIVE_SUBJECT = -1        # a row written only when every real subject is erased; never requestable

ARROW_SCHEMA = pa.schema([
    pa.field("subject_id", pa.int64(), nullable=False),
    pa.field("event_day", pa.int64(), nullable=False),
    pa.field("payload", pa.string(), nullable=False),
])
ICEBERG_SCHEMA = Schema(
    NestedField(1, "subject_id", LongType(), required=True),
    NestedField(2, "event_day", LongType(), required=True),
    NestedField(3, "payload", StringType(), required=True),
)
SUBJECT_FIELD_ID = 1


@dataclass
class Config:
    name: str = "default"
    seed: int = 20260823
    # horizon and cohort (lhbench)
    horizon_days: int = 100
    warmup_days: int = 10
    tail_days: int = 45
    # ingest
    rows_per_day: int = 900
    n_subjects: int = 1500
    subject_alpha: float = 1.2
    layout: str = "scattered"
    n_buckets: int = 16
    files_per_day: int = 1            # commits per day (erasure harness)
    ingest_hour: int = 1
    payload_chars: int = 48
    # erasure demand
    dsar_rate_per_day: float = 2.0
    dsar_burst_prob: float = 0.05
    dsar_burst_multiplier: float = 20.0
    dsar_batch_hour: int = 2          # nightly commit batch (S0)
    deadline_days: int = 30

    def __post_init__(self):
        if self.layout not in LAYOUTS:
            raise ValueError(f"layout must be one of {LAYOUTS}")
        if self.horizon_days <= 0 or self.rows_per_day <= 0 or self.n_subjects <= 0:
            raise ValueError("horizon_days, rows_per_day and n_subjects must be positive")
        if self.files_per_day <= 0:
            raise ValueError("files_per_day must be positive")
        if self.warmup_days < 0 or self.tail_days < 0 or self.warmup_days + self.tail_days >= self.horizon_days:
            raise ValueError("warmup_days + tail_days must leave at least one cohort day inside the horizon")
        if self.deadline_days <= 0:
            raise ValueError("deadline_days must be positive")
        if not 0 <= self.dsar_batch_hour < HOURS_PER_DAY or not 0 <= self.ingest_hour < HOURS_PER_DAY:
            raise ValueError("hours must be within the day")
        if self.n_buckets <= 0:
            raise ValueError("n_buckets must be positive")
        if self.dsar_rate_per_day < 0 or not 0 <= self.dsar_burst_prob <= 1 or self.dsar_burst_multiplier < 0:
            raise ValueError("erasure demand parameters out of range")

    def as_dict(self) -> dict:
        return dict(self.__dict__)

    def cohort_days(self) -> tuple[int, int]:
        """Inclusive day range of the analysis cohort: requests issued before
        ``warmup_days`` saw an unrepresentatively small table, and those
        issued within ``tail_days`` of the horizon cannot complete, so neither
        is counted (lhbench).  Requests are still generated on every day."""
        return self.warmup_days, self.horizon_days - self.tail_days


@dataclass
class Batch:
    day: int
    idx: int
    commit_hour: int
    subjects: list[int]               # row order


@dataclass
class Request:
    rid: int
    subject: int
    arrival: int                      # virtual hour
    burst: bool


class Workload:
    """Day-by-day generator.  Ingest for a day is drawn before that day's
    requests so a request can only name a subject that already has rows."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.rng = LCG(cfg.seed)
        self.cum = cumulative(power_law_weights(cfg.n_subjects, cfg.subject_alpha))
        self.erased: set[int] = set()
        self.seen: set[int] = set()   # subjects that have at least one row
        self.requests: list[Request] = []

    def ingest(self, day: int) -> list[Batch]:
        cfg = self.cfg
        n = cfg.rows_per_day
        subjects = []
        for _ in range(n):
            s = self.rng.weighted_index(self.cum)
            if s in self.erased:
                continue
            subjects.append(s)
        if not subjects:                                 # everyone erased: keep the table alive
            live = [s for s in range(cfg.n_subjects) if s not in self.erased]
            subjects = [live[self.rng.below_int(len(live))]] if live else [KEEPALIVE_SUBJECT]
        if cfg.layout == "clustered":
            subjects.sort()
        self.seen.update(s for s in subjects if s != KEEPALIVE_SUBJECT)
        per = -(-len(subjects) // cfg.files_per_day)
        out = []
        for i in range(cfg.files_per_day):
            chunk = subjects[i * per:(i + 1) * per]
            if not chunk:
                break
            hour = at(day, cfg.ingest_hour) + (i * (HOURS_PER_DAY - cfg.ingest_hour - 1)) // cfg.files_per_day
            out.append(Batch(day, i, hour, chunk))
        return out

    def dsars(self, day: int) -> list[Request]:
        """Requests arrive on every day of the run (lhbench); the cohort is an
        analysis window applied by ``Ledger.summary``, not a generation gate,
        so maintenance sees a steady erasure load to the end of the horizon."""
        cfg = self.cfg
        rate = cfg.dsar_rate_per_day
        burst = self.rng.below(cfg.dsar_burst_prob)
        if burst:
            rate *= cfg.dsar_burst_multiplier
        n = self.rng.poisson(rate)
        candidates = sorted(self.seen - self.erased)
        if not candidates or n == 0:
            return []
        chosen = self.rng.sample(candidates, n)
        out = []
        for s in chosen:
            arrival = at(day, 0) + self.rng.below_int(HOURS_PER_DAY)
            r = Request(len(self.requests), s, arrival, burst)
            self.requests.append(r)
            self.erased.add(s)
            out.append(r)
        return out

    def arrow(self, batch: Batch) -> pa.Table:
        cfg = self.cfg
        payload = ["".join(chr(97 + self.rng.below_int(26)) for _ in range(cfg.payload_chars))
                   for _ in batch.subjects]
        return pa.table({"subject_id": pa.array(batch.subjects, pa.int64()),
                         "event_day": pa.array([batch.day] * len(batch.subjects), pa.int64()),
                         "payload": pa.array(payload, pa.string())}, schema=ARROW_SCHEMA)


def calibration(requests: list[Request], rows_per_subject: dict) -> dict:
    sizes = sorted((rows_per_subject.get(r.subject, 0) for r in requests), reverse=True)
    total = sum(sizes)
    top1 = sum(sizes[:max(1, round(len(sizes) * 0.01))]) / total if total else 0.0
    return {"requests": len(sizes), "rows_requested": total,
            "mean_rows_per_request": (total / len(sizes)) if sizes else 0.0,
            "max_rows_per_request": sizes[0] if sizes else 0, "top1pct_share": top1,
            "burst_requests": sum(1 for r in requests if r.burst)}
