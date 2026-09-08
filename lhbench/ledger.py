"""The residency ledger.

Tracks every erasure obligation through the stages of the residency stack and
records the virtual day each stage completed. This is what turns "deletion is
slow" into a measured distribution.

Stages tracked
--------------
  issued          the erasure request arrives
  logical         a reader of the live table no longer sees the rows. Under
                  merge-on-read this is immediate (a delete file masks them);
                  under copy-on-write it coincides with the rewrite.
  rewritten       the rows are gone from the CURRENT snapshot's data files
  unreferenced    no retained snapshot references any file containing them
                  (time travel can no longer surface them)
  physical        the last object containing them is gone from storage

Only the last one is erasure. The gap between `logical` and `physical` is the
paper's subject: the interval in which a system reports the data as deleted
while the bytes are still on disk and readable by anyone who bypasses the
table format.

Deletion residency time (DRT) = physical - issued. Everything before that is
a partial result that compliance cannot rely on.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field


@dataclass
class Obligation:
    subject: int
    issued_day: int
    deadline_day: int
    logical_day: int | None = None
    rewritten_day: int | None = None
    unreferenced_day: int | None = None
    delete_marker_day: int | None = None
    physical_day: int | None = None

    @property
    def closed(self) -> bool:
        return self.physical_day is not None

    def drt(self) -> int | None:
        if self.physical_day is None:
            return None
        return self.physical_day - self.issued_day

    def met_deadline(self) -> bool | None:
        if self.physical_day is None:
            return None
        return self.physical_day <= self.deadline_day


@dataclass
class Ledger:
    deadline_days: int
    obligations: dict[int, Obligation] = field(default_factory=dict)
    bytes_rewritten: int = 0
    """Total bytes rewritten by compaction over the run -- the cost side of
    the tradeoff curve."""
    subject_bytes_erased: int = 0
    """Approximate bytes of the subjects' own rows -- the denominator of
    deletion amplification."""

    def open(self, subject: int, day: int) -> Obligation:
        ob = Obligation(subject, day, day + self.deadline_days)
        self.obligations[subject] = ob
        return ob

    def pending(self) -> list[Obligation]:
        return [o for o in self.obligations.values() if not o.closed]

    def pending_rewrite(self) -> list[Obligation]:
        """Obligations whose bytes are still in the current snapshot's data
        files -- i.e. the work a compaction scheduler has left to do."""
        return [o for o in self.obligations.values() if o.rewritten_day is None]

    # -- stage transitions -------------------------------------------------

    def mark_logical(self, subjects: set[int], day: int) -> None:
        for s in subjects:
            ob = self.obligations.get(s)
            if ob and ob.logical_day is None:
                ob.logical_day = day

    def mark_rewritten(self, subjects: set[int], day: int) -> None:
        for s in subjects:
            ob = self.obligations.get(s)
            if ob and ob.rewritten_day is None:
                ob.rewritten_day = day
                if ob.logical_day is None:
                    ob.logical_day = day

    def mark_delete_marker(self, subjects: set[int], day: int) -> None:
        """The live object was deleted. Under bucket versioning this creates a
        delete marker; the bytes live on as a noncurrent version."""
        for s in subjects:
            ob = self.obligations.get(s)
            if ob and ob.delete_marker_day is None:
                ob.delete_marker_day = day

    def mark_unreferenced(self, subjects: set[int], day: int) -> None:
        for s in subjects:
            ob = self.obligations.get(s)
            if ob and ob.unreferenced_day is None:
                ob.unreferenced_day = day

    def mark_physical(self, subjects: set[int], day: int) -> None:
        for s in subjects:
            ob = self.obligations.get(s)
            if ob and ob.physical_day is None:
                ob.physical_day = day
                # Several stages can fire on the same day; backfill so the
                # stage-gap medians stay well defined.
                for attr in ("logical_day", "rewritten_day", "unreferenced_day",
                             "delete_marker_day"):
                    if getattr(ob, attr) is None:
                        setattr(ob, attr, day)

    # -- summary -----------------------------------------------------------

    def summary(self, horizon: int, warmup: int = 0, tail: int = 0) -> dict:
        """Summarise the run.

        `warmup` and `tail` define an analysis COHORT: obligations issued
        before `warmup` see an unrepresentatively empty table, and those issued
        within `tail` days of the horizon cannot possibly complete, so counting
        them as deadline misses would overstate the problem. Reporting on a
        cohort that had a fair chance is the honest construction, and a
        reviewer will look for it.
        """
        obs = [
            o
            for o in self.obligations.values()
            if warmup <= o.issued_day <= horizon - tail
        ]
        if not obs:
            return {"n_obligations": 0}

        closed = [o for o in obs if o.closed]
        drts = [o.drt() for o in closed]
        # Unclosed obligations are censored at the horizon; reporting only
        # closed ones would silently drop the worst cases.
        censored = [horizon - o.issued_day for o in obs if not o.closed]
        all_drt = drts + censored

        def pct(v: list[int], p: float) -> float:
            if not v:
                return float("nan")
            s = sorted(v)
            return s[min(len(s) - 1, int(p * len(s)))]

        def stage_gap(attr: str) -> float:
            vals = [
                getattr(o, attr) - o.issued_day
                for o in obs
                if getattr(o, attr) is not None
            ]
            return statistics.median(vals) if vals else float("nan")

        met = [o.met_deadline() for o in obs]
        n_met = sum(1 for m in met if m is True)

        amp = None
        if self.subject_bytes_erased > 0:
            amp = self.bytes_rewritten / self.subject_bytes_erased

        return {
            "cohort": {"warmup": warmup, "tail": tail},
            "n_obligations": len(obs),
            "n_closed": len(closed),
            "n_censored": len(censored),
            "drt_days": {
                "p50": pct(all_drt, 0.50),
                "p95": pct(all_drt, 0.95),
                "max": max(all_drt) if all_drt else None,
                "mean": statistics.fmean(all_drt) if all_drt else None,
            },
            "median_days_to_logical": stage_gap("logical_day"),
            "median_days_to_rewritten": stage_gap("rewritten_day"),
            "median_days_to_unreferenced": stage_gap("unreferenced_day"),
            "median_days_to_delete_marker": stage_gap("delete_marker_day"),
            "median_days_to_physical": stage_gap("physical_day"),
            "deadline_met_fraction": n_met / len(obs),
            "bytes_rewritten": self.bytes_rewritten,
            "subject_bytes_erased": self.subject_bytes_erased,
            "deletion_amplification": amp,
        }
