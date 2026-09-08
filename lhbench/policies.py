"""Maintenance policies -- the paper's mechanism, and its baseline.

BaselinePolicy reproduces how maintenance is run today: three independent jobs
on fixed cadences, each with a local heuristic, none aware that an erasure
obligation has a deadline.

DeadlinePolicy is the contribution. Two ideas:

  1. FREE RIDES. Rewriting a data file discharges the rows of EVERY pending
     obligation living in it, not just the one that triggered the rewrite. With
     file-granular selection this is automatic and exact: choose the file, and
     every subject inside it is swept out at zero marginal cost.

  2. CROSS-STAGE COORDINATION. A rewrite alone erases nothing -- the old file
     survives until snapshot expiry releases it and orphan cleanup deletes it,
     and under bucket versioning the bytes survive further still. So expiry and
     cleanup are pulled forward on demand when an obligation approaches its
     deadline, rather than waiting for a cadence that may be a month away.
     This is the part with no analogue in LSM deadline-aware compaction: those
     schedulers own the whole path to physical erasure, whereas here three of
     the stages sit outside the compactor and two sit in a different system.

WHY POLICIES SELECT FILES, NOT SUBJECTS
---------------------------------------
Earlier versions selected *subjects* and executed `delete(In(key, subjects))`,
which rewrites every file containing them. Under a scattered layout one subject
touches nearly the whole table, so the cheapest available action already
exceeded any small budget and the policy overshot its cap by up to 2.6x --
invalidating the equal-budget comparison exactly where it mattered.

Selecting files makes the file the unit of work. An obligation is discharged
incrementally as its files are rewritten, and the budget becomes enforceable.
Both policies are given the SAME daily budget, so the mechanism must win by
scheduling better, not by spending more.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .config import Config
from .ledger import Ledger


@dataclass
class Decision:
    """What a policy wants done on a given day."""

    files_to_rewrite: set[str] = field(default_factory=set)
    do_expire: bool = False
    do_orphan_cleanup: bool = False
    reason: str = ""


class Policy:
    name = "abstract"

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def decide(self, day, ledger, file_subjects_current, file_bytes) -> Decision:
        raise NotImplementedError

    def _select(self, ranked: list[str], file_bytes: dict[str, int]) -> set[str]:
        """Take files in the given priority order until the budget is spent.

        If the single highest-priority file exceeds the whole budget we take it
        anyway -- otherwise the policy deadlocks and never makes progress. That
        case is reported by verify.py rather than hidden.
        """
        chosen: set[str] = set()
        spent = 0
        for p in ranked:
            cost = file_bytes.get(p, 0)
            if spent + cost > self.cfg.daily_rewrite_budget_bytes:
                if chosen:
                    break
                chosen.add(p)          # guarantee forward progress
                break
            chosen.add(p)
            spent += cost
        return chosen


class BaselinePolicy(Policy):
    """Fixed cadences, local heuristics, no deadline awareness."""

    name = "baseline"

    def decide(self, day, ledger, file_subjects_current, file_bytes) -> Decision:
        cfg = self.cfg
        do_expire = day % cfg.expire_interval_days == 0
        do_orphan = day % cfg.orphan_interval_days == 0

        if day % cfg.compact_interval_days != 0:
            return Decision(set(), do_expire, do_orphan, "off-cadence")

        pending = ledger.pending_rewrite()
        if not pending:
            return Decision(set(), do_expire, do_orphan, "nothing pending")

        pending_ids = {o.subject for o in pending}
        issued = {o.subject: o.issued_day for o in pending}

        # Dirty-threshold gate: only touch files where enough rows are pending
        # deletion -- the file-level analogue of shipped delete-ratio heuristics.
        eligible = []
        for path, subs in file_subjects_current.items():
            if not subs:
                continue
            hit = subs & pending_ids
            if not hit:
                continue          # a threshold of 0 must not select clean files
            if len(hit) / max(1, len(subs)) >= cfg.dirty_threshold:
                eligible.append((min(issued[s] for s in hit), path))

        # FIFO by oldest obligation in the file. Note the flaw this models: no
        # notion of a deadline, and no preference for files that discharge many
        # obligations at once.
        eligible.sort()
        ranked = [p for _, p in eligible]
        return Decision(self._select(ranked, file_bytes), do_expire, do_orphan,
                        "cadence compaction")


class DeadlinePolicy(Policy):
    """Deadline-driven scheduling with free rides and cross-stage coordination."""

    name = "deadline"

    #: Obligations within this many days of their deadline are urgent, and
    #: drive both rewrite selection and cleanup scheduling.
    URGENCY_HORIZON = 10

    def decide(self, day, ledger, file_subjects_current, file_bytes) -> Decision:
        cfg = self.cfg
        pending = ledger.pending_rewrite()
        all_pending = ledger.pending()

        chosen: set[str] = set()
        if pending:
            deadline_of = {o.subject: o.deadline_day for o in pending}
            pending_ids = set(deadline_of)

            # Rank files by (earliest deadline they carry, then most
            # obligations discharged per byte). The second term is the free-ride
            # effect made explicit: a file holding many pending subjects is
            # worth more than one holding a single subject at the same cost.
            ranked = []
            for path, subs in file_subjects_current.items():
                hit = subs & pending_ids
                if not hit:
                    continue
                urgency = min(deadline_of[s] for s in hit)
                value = len(hit) / max(1, file_bytes.get(path, 1))
                ranked.append((urgency, -value, path))
            ranked.sort()
            chosen = self._select([p for _, _, p in ranked], file_bytes)

        # A rewrite is not an erasure. If anything is near its deadline and has
        # already left the current snapshot, pull expiry and cleanup forward.
        near = any(o.deadline_day - day <= self.URGENCY_HORIZON
                   for o in all_pending)
        awaiting = any(o.rewritten_day is not None for o in all_pending)

        do_expire = near or (day % cfg.expire_interval_days == 0)
        do_orphan = (near and awaiting) or (day % cfg.orphan_interval_days == 0)

        n_urgent = sum(1 for o in all_pending
                       if o.deadline_day - day <= self.URGENCY_HORIZON)
        return Decision(chosen, do_expire, do_orphan,
                        f"deadline-driven (urgent={n_urgent})")


def make_policy(cfg: Config) -> Policy:
    return {"baseline": BaselinePolicy, "deadline": DeadlinePolicy}[cfg.policy](cfg)
