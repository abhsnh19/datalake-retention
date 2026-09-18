"""One ledger, two views.

Every erasure obligation records the virtual hour each stage completed:

    arrival       issued
    commit        the delete landed (lhbench "logical"; erasure t0)
    rewritten     no current data file holds the rows      (t1)
    unreferenced  no live snapshot references such a file
    unlinked      every such file received its DELETE      (t2; lhbench "delete_marker")
    physical      the bucket freed the last byte           (t3)

``summary`` returns the lhbench view (median days-to-stage from arrival, DRT
percentiles over closed + censored, deadline_met_fraction, amplification)
and the erasure view (per-stage durations, median residency over completed
requests, miss rate = late + censored) from the same numbers, so both decks
can be rebuilt from one run and cannot disagree with each other.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass, field

from .vclock import HOURS_PER_DAY

STAGES = ("commit", "rewritten", "unreferenced", "unlinked", "physical")


@dataclass
class Obligation:
    rid: int
    subject: int
    arrival: int
    deadline: int
    burst: bool = False
    commit: int | None = None
    rewritten: int | None = None
    unreferenced: int | None = None
    unlinked: int | None = None
    physical: int | None = None
    files: set = field(default_factory=set)      # every path that ever held the rows
    rows: int = 0

    @property
    def closed(self) -> bool:
        return self.physical is not None

    def drt_hours(self) -> int | None:
        return None if self.physical is None else self.physical - self.arrival

    def met_deadline(self) -> bool | None:
        return None if self.physical is None else self.physical <= self.deadline

    def ordered(self) -> bool:
        seq = [getattr(self, s) for s in STAGES]
        seq = [x for x in seq if x is not None]
        return seq == sorted(seq) and (self.commit is None or self.commit >= self.arrival)

    def as_dict(self) -> dict:
        return {"rid": self.rid, "subject": self.subject, "arrival": self.arrival, "deadline": self.deadline,
                "burst": self.burst, **{s: getattr(self, s) for s in STAGES},
                "files": len(self.files), "rows": self.rows}


def _pct(vals, q):
    if not vals:
        return None
    s = sorted(vals)
    return s[min(len(s) - 1, int(q * len(s)))]


@dataclass
class Ledger:
    deadline_hours: int
    obligations: dict[int, Obligation] = field(default_factory=dict)
    by_subject: dict[int, Obligation] = field(default_factory=dict)
    bytes_rewritten: int = 0            # by the policy's rewrites (merge-on-read)
    bytes_rewritten_cow: int = 0        # by copy-on-write deletes at commit
    subject_bytes_erased: int = 0

    def open(self, rid: int, subject: int, arrival: int, burst: bool = False) -> Obligation:
        if subject in self.by_subject:
            raise ValueError(f"subject {subject} already has an obligation")
        ob = Obligation(rid, subject, arrival, arrival + self.deadline_hours, burst)
        self.obligations[rid] = ob
        self.by_subject[subject] = ob
        return ob

    def pending(self) -> list[Obligation]:
        return [o for o in self.obligations.values() if not o.closed]

    def pending_rewrite(self) -> list[Obligation]:
        return [o for o in self.obligations.values() if o.commit is not None and o.rewritten is None]

    def mark(self, stage: str, subjects, hour: int) -> None:
        if stage not in STAGES:
            raise KeyError(stage)
        for s in subjects:
            ob = self.by_subject.get(s)
            if ob is not None and getattr(ob, stage) is None:
                setattr(ob, stage, hour)
                if stage == "physical":         # stages that fired in the same tick
                    for prev in STAGES[:-1]:
                        if getattr(ob, prev) is None:
                            setattr(ob, prev, hour)

    def summary(self, horizon_hours: int, warmup_days: int = 0, tail_days: int = 0) -> dict:
        lo, hi = warmup_days * HOURS_PER_DAY, horizon_hours - tail_days * HOURS_PER_DAY
        obs = [o for o in self.obligations.values() if lo <= o.arrival <= hi]
        n = len(obs)
        if n == 0:
            return {"n_obligations": 0, "n_closed": 0, "n_censored": 0, "lhbench": {}, "erasure": {},
                    "bytes_rewritten": self.bytes_rewritten, "bytes_rewritten_cow": self.bytes_rewritten_cow}
        closed = [o for o in obs if o.closed]
        censored = [o for o in obs if not o.closed]
        drt_closed = [o.drt_hours() / HOURS_PER_DAY for o in closed]
        drt_all = drt_closed + [(horizon_hours - o.arrival) / HOURS_PER_DAY for o in censored]

        def to_stage(stage):
            vals = [(getattr(o, stage) - o.arrival) / HOURS_PER_DAY for o in obs if getattr(o, stage) is not None]
            return statistics.median(vals) if vals else None

        lh = {
            "cohort": {"warmup_days": warmup_days, "tail_days": tail_days},
            "drt_days": {"p50": _pct(drt_all, 0.5), "p95": _pct(drt_all, 0.95),
                         "max": max(drt_all), "mean": statistics.fmean(drt_all)},
            "median_days_to_logical": to_stage("commit"),
            "median_days_to_rewritten": to_stage("rewritten"),
            "median_days_to_unreferenced": to_stage("unreferenced"),
            "median_days_to_delete_marker": to_stage("unlinked"),
            "median_days_to_physical": to_stage("physical"),
            "deadline_met_fraction": sum(1 for o in obs if o.met_deadline()) / n,
            "deletion_amplification": ((self.bytes_rewritten + self.bytes_rewritten_cow) / self.subject_bytes_erased
                                       if self.subject_bytes_erased else None),
        }
        r = lh["median_days_to_rewritten"]; u = lh["median_days_to_unreferenced"]
        m = lh["median_days_to_delete_marker"]; p = lh["median_days_to_physical"]
        lh["stage_increments_days"] = {
            "rewrite": r, "snapshot_expiry": (u - r) if None not in (u, r) else None,
            "unlink": (m - u) if None not in (m, u) else None,
            "store_expiry": (p - m) if None not in (p, m) else None}

        stage = {"commit": [], "rewrite": [], "unlink": [], "expire": []}
        for o in closed:
            stage["commit"].append((o.commit - o.arrival) / HOURS_PER_DAY)
            stage["rewrite"].append((o.rewritten - o.commit) / HOURS_PER_DAY)
            stage["unlink"].append((o.unlinked - o.rewritten) / HOURS_PER_DAY)
            stage["expire"].append((o.physical - o.unlinked) / HOURS_PER_DAY)
        late = sum(1 for d in drt_closed if d > self.deadline_hours / HOURS_PER_DAY)
        er = {
            "requests": n, "completed": len(closed), "censored_at_horizon": len(censored),
            "days": {"median": _pct(drt_closed, 0.5), "p90": _pct(drt_closed, 0.9), "p95": _pct(drt_closed, 0.95),
                     "mean": statistics.fmean(drt_closed) if drt_closed else None,
                     "max": max(drt_closed) if drt_closed else None, "min": min(drt_closed) if drt_closed else None},
            "miss_rate": (late + len(censored)) / n,
            "stage_mean_days": {k: (statistics.fmean(v) if v else None) for k, v in stage.items()},
            "stage_median_days": {k: (statistics.median(v) if v else None) for k, v in stage.items()},
        }
        return {"n_obligations": n, "n_closed": len(closed), "n_censored": len(censored),
                "ordered": all(o.ordered() for o in obs),
                "lhbench": lh, "erasure": er,
                "bytes_rewritten": self.bytes_rewritten, "bytes_rewritten_cow": self.bytes_rewritten_cow,
                "subject_bytes_erased": self.subject_bytes_erased}
