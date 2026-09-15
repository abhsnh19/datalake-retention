"""Maintenance policies: lhbench's two and the erasure harness's six, one class.

Every policy answers the same daily question -- which current data files to
rewrite today, and whether to run snapshot expiry and orphan cleanup -- and
the runner executes the answer on the real table.  Rewrite strategies:

  none                        copy-on-write: the DELETE rewrites at commit
  cadence_fifo                lhbench baseline: every compact_interval_days,
                              files holding a pending subject, gated by the
                              dirty threshold, oldest obligation first, inside
                              the daily byte budget
  deadline_edf                lhbench deadline policy: every day, files ranked
                              by (earliest deadline they carry, most pending
                              subjects per byte) -- the free-ride effect --
                              inside the budget; expiry and orphan cleanup are
                              pulled forward when an obligation is within
                              urgency_horizon_days of its deadline
  weekly_window_monthly_full  erasure A-default: weekly, files whose newest
                              rows fall inside the last weekly_window_days;
                              monthly, everything
  daily_full                  erasure B / E: every day, every file holding a
                              pending subject
  deadline_deferred           erasure C-coordinated: every day, the files of
                              obligations whose latest safe rewrite pass has
                              arrived (rewrite as late as the deadline allows)

A byte budget (lhbench's fairness knob) applies to every merge-on-read
strategy when set; copy-on-write pays at commit, outside the scheduler.
``expiry_deletes_files`` selects the Java action's behaviour (expire and
delete what is unreferenced) or PyIceberg's native metadata-only expiry, in
which case orphan cleanup is the stage that reclaims bytes -- the single
largest source of difference between the two harnesses' decompositions.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .vclock import HOURS_PER_DAY

REWRITE_STRATEGIES = ("none", "cadence_fifo", "deadline_edf", "weekly_window_monthly_full",
                      "daily_full", "deadline_deferred")
DELETE_MODES = ("cow", "mor")


@dataclass(frozen=True)
class Policy:
    name: str
    delete_mode: str = "mor"
    rewrite: str = "cadence_fifo"
    compact_interval_days: int = 7
    dirty_threshold: float = 0.0
    daily_rewrite_budget_bytes: int | None = None
    snapshot_retention_days: int = 5
    expire_interval_days: int = 7            # 0 = the expiry job never runs
    orphan_interval_days: int = 30           # 0 = the orphan job never runs
    orphan_older_than_days: int = 3
    expiry_deletes_files: bool = True
    pull_forward: bool = False
    urgency_horizon_days: int = 10
    margin_days: int = 1
    weekly_window_days: int = 7
    maintenance_hour: int = 3
    expire_hour: int = 4
    orphan_hour: int = 5

    def __post_init__(self):
        if self.delete_mode not in DELETE_MODES:
            raise ValueError(f"delete_mode must be one of {DELETE_MODES}")
        if self.rewrite not in REWRITE_STRATEGIES:
            raise ValueError(f"rewrite must be one of {REWRITE_STRATEGIES}")
        if self.delete_mode == "cow" and self.rewrite != "none":
            raise ValueError("copy-on-write rewrites at commit; rewrite must be 'none'")
        if self.delete_mode == "mor" and self.rewrite == "none":
            raise ValueError("merge-on-read needs a rewrite strategy")
        if self.compact_interval_days <= 0:
            raise ValueError("compact_interval_days must be positive")
        if not 0.0 <= self.dirty_threshold <= 1.0:
            raise ValueError("dirty_threshold must be within [0, 1]")
        if self.daily_rewrite_budget_bytes is not None and self.daily_rewrite_budget_bytes <= 0:
            raise ValueError("daily_rewrite_budget_bytes must be positive or None")
        if self.snapshot_retention_days < 0:
            raise ValueError("snapshot_retention_days must be non-negative")
        for n in ("expire_interval_days", "orphan_interval_days", "orphan_older_than_days",
                  "urgency_horizon_days", "margin_days", "weekly_window_days"):
            if getattr(self, n) < 0:
                raise ValueError(f"{n} must be non-negative")
        if not (self.maintenance_hour < self.expire_hour < self.orphan_hour < HOURS_PER_DAY):
            raise ValueError("job hours must be ordered maintenance < expire < orphan within the day")

    def as_dict(self) -> dict:
        return dict(self.__dict__)

    # -- calendar helpers ----------------------------------------------
    def expire_due(self, day: int) -> bool:
        return self.expire_interval_days > 0 and day % self.expire_interval_days == 0

    def orphan_due(self, day: int) -> bool:
        return self.orphan_interval_days > 0 and day % self.orphan_interval_days == 0

    def orphan_tail_days(self) -> int:
        """Days the orphan job can add when expiry does not delete files."""
        if self.expiry_deletes_files:
            return 0
        return max(self.orphan_interval_days, self.orphan_older_than_days)


@dataclass
class Decision:
    files_to_rewrite: set = field(default_factory=set)
    do_expire: bool = False
    do_orphan: bool = False
    reason: str = ""


def _select(ranked: list[str], file_bytes: dict[str, int], budget: int | None) -> tuple[set, bool]:
    """Take files in priority order until the budget is spent.  If the first
    file alone exceeds the whole budget it is taken anyway (forward
    progress); the second return value says so, and verify.py reports it."""
    if budget is None:
        return set(ranked), False
    chosen: set[str] = set()
    spent = 0
    forced = False
    for p in ranked:
        cost = file_bytes.get(p, 0)
        if spent + cost > budget:
            if chosen:
                break
            chosen.add(p)
            forced = cost > budget
            break
        chosen.add(p)
        spent += cost
    return chosen, forced


def decide(policy: Policy, day: int, ledger, current: dict, now: int, due_pass: dict | None = None) -> Decision:
    """``current`` maps a current data-file path to ``(subjects, bytes, newest_event_day)``.
    ``due_pass`` (rid -> hour) is used by deadline_deferred."""
    do_expire = policy.expire_due(day)
    do_orphan = policy.orphan_due(day)
    pending = ledger.pending_rewrite()
    if policy.delete_mode == "cow" or not pending:
        if policy.pull_forward:
            do_expire, do_orphan = _pulled(policy, day, ledger, do_expire, do_orphan)
        return Decision(set(), do_expire, do_orphan, "nothing pending" if not pending else "copy-on-write")

    pending_ids = {o.subject for o in pending}
    arrival = {o.subject: o.arrival for o in pending}
    deadline = {o.subject: o.deadline for o in pending}
    file_bytes = {p: b for p, (_s, b, _d) in current.items()}
    chosen: set[str] = set()
    forced = False
    reason = ""

    if policy.rewrite == "cadence_fifo":
        if day % policy.compact_interval_days == 0:
            eligible = []
            for path, (subs, _b, _d) in current.items():
                hit = subs & pending_ids
                if not hit:
                    continue            # a threshold of 0 must never select a clean file
                if len(hit) / max(1, len(subs)) >= policy.dirty_threshold:
                    eligible.append((min(arrival[s] for s in hit), path))
            eligible.sort()
            chosen, forced = _select([p for _, p in eligible], file_bytes, policy.daily_rewrite_budget_bytes)
            reason = "cadence compaction"
        else:
            reason = "off-cadence"
    elif policy.rewrite == "deadline_edf":
        ranked = []
        for path, (subs, b, _d) in current.items():
            hit = subs & pending_ids
            if not hit:
                continue
            ranked.append((min(deadline[s] for s in hit), -len(hit) / max(1, b), path))
        ranked.sort()
        chosen, forced = _select([p for _, _, p in ranked], file_bytes, policy.daily_rewrite_budget_bytes)
        reason = f"deadline-driven (pending={len(pending)})"
    elif policy.rewrite == "weekly_window_monthly_full":
        monthly = day % 30 == 0
        weekly = day % policy.compact_interval_days == 0
        if monthly or weekly:
            ranked = []
            for path, (subs, _b, newest) in sorted(current.items()):
                if not (subs & pending_ids):
                    continue
                if monthly or newest >= day - policy.weekly_window_days:
                    ranked.append(path)
            chosen, forced = _select(ranked, file_bytes, policy.daily_rewrite_budget_bytes)
            reason = "monthly full" if monthly else "weekly window"
        else:
            reason = "off-cadence"
    elif policy.rewrite == "daily_full":
        ranked = [p for p, (subs, _b, _d) in sorted(current.items()) if subs & pending_ids]
        chosen, forced = _select(ranked, file_bytes, policy.daily_rewrite_budget_bytes)
        reason = "daily full"
    elif policy.rewrite == "deadline_deferred":
        due_ids = {o.subject for o in pending if due_pass is not None and due_pass.get(o.rid, 0) <= now}
        ranked = [p for p, (subs, _b, _d) in sorted(current.items()) if subs & due_ids]
        chosen, forced = _select(ranked, file_bytes, policy.daily_rewrite_budget_bytes)
        reason = f"deferred (due={len(due_ids)})"

    if policy.pull_forward:
        do_expire, do_orphan = _pulled(policy, day, ledger, do_expire, do_orphan)
    d = Decision(chosen, do_expire, do_orphan, reason)
    d.forced = forced
    return d


def _pulled(policy: Policy, day: int, ledger, do_expire: bool, do_orphan: bool) -> tuple[bool, bool]:
    """lhbench cross-stage coordination: a rewrite is not an erasure, so when
    anything is near its deadline, run expiry, and run cleanup once something
    has been rewritten and is waiting."""
    now_h = day * HOURS_PER_DAY
    pending = ledger.pending()
    near = any((o.deadline - now_h) <= policy.urgency_horizon_days * HOURS_PER_DAY for o in pending)
    awaiting = any(o.rewritten is not None for o in pending)
    return (do_expire or near), (do_orphan or (near and awaiting))


def coordinated_slack_hours(policy: Policy, store_tail_days: int, rounding_days: int = 1) -> int:
    """Hours the tail needs after the rewrite, for the deferred strategy:
    snapshot retention + one expiry cadence + the orphan tail when expiry
    does not delete + the store's own days + rounding + the margin."""
    expire = policy.expire_interval_days if policy.expire_interval_days > 0 else 0
    return ((policy.snapshot_retention_days + expire + policy.orphan_tail_days()
             + store_tail_days + rounding_days + policy.margin_days) * HOURS_PER_DAY)


def rewrite_due_pass(arrival: int, commit: int, slack_hours: int, deadline_days: int, daily_passes) -> int:
    """Latest daily rewrite pass that still meets the deadline; the very
    next pass when the deadline is already too tight; +inf if none remain."""
    due = arrival + deadline_days * HOURS_PER_DAY - slack_hours
    latest = None
    nxt = float("inf")
    for p in daily_passes:
        if p < commit:
            continue
        if nxt == float("inf"):
            nxt = p
        if p <= due:
            latest = p
        else:
            break
    return latest if latest is not None else nxt


PRESETS: dict[str, Policy] = {
    # lhbench (Abhishek): 20-day retention, 15-day orphan job, weekly compaction and expiry, budget 2 MB/day
    "lhbench-baseline": Policy("lhbench-baseline", "mor", "cadence_fifo", compact_interval_days=7,
                               dirty_threshold=0.0, daily_rewrite_budget_bytes=2_000_000,
                               snapshot_retention_days=20, expire_interval_days=7, orphan_interval_days=15,
                               orphan_older_than_days=0, expiry_deletes_files=False),
    "lhbench-deadline": Policy("lhbench-deadline", "mor", "deadline_edf", daily_rewrite_budget_bytes=2_000_000,
                               snapshot_retention_days=20, expire_interval_days=7, orphan_interval_days=15,
                               orphan_older_than_days=0, expiry_deletes_files=False, pull_forward=True),
    # erasure harness (Nilanjan): Iceberg defaults, expiry deletes files, no budget
    "A-default": Policy("A-default", "mor", "weekly_window_monthly_full", expire_interval_days=7, orphan_interval_days=30),
    "B-compactor-only": Policy("B-compactor-only", "mor", "daily_full", expire_interval_days=7, orphan_interval_days=30),
    "C-coordinated": Policy("C-coordinated", "mor", "deadline_deferred", expire_interval_days=1, orphan_interval_days=1),
    "D-cow-default": Policy("D-cow-default", "cow", "none", expire_interval_days=7, orphan_interval_days=30),
    "E-asap-daily": Policy("E-asap-daily", "mor", "daily_full", expire_interval_days=1, orphan_interval_days=1),
    "F-cow-daily-expiry": Policy("F-cow-daily-expiry", "cow", "none", expire_interval_days=1, orphan_interval_days=30),
    # the union: deadline-driven file selection under a budget, daily tail, Iceberg defaults, Java expiry
    "combined-coordinated": Policy("combined-coordinated", "mor", "deadline_edf", daily_rewrite_budget_bytes=2_000_000,
                                   snapshot_retention_days=5, expire_interval_days=1, orphan_interval_days=1,
                                   orphan_older_than_days=3, expiry_deletes_files=True, pull_forward=True),
    # AWS S3 Tables: managed snapshot expiry (120 h, min 1) and unreferenced-file removal (3 d gate), daily
    "s3tables-managed": Policy("s3tables-managed", "mor", "daily_full", snapshot_retention_days=5,
                               expire_interval_days=1, orphan_interval_days=1, orphan_older_than_days=3,
                               expiry_deletes_files=False),
}


def preset(name: str) -> Policy:
    try:
        return PRESETS[name]
    except KeyError:
        raise KeyError(f"unknown policy preset {name!r}; choose from {sorted(PRESETS)}") from None
