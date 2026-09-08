"""The simulator: drives a real Iceberg table through simulated months.

Design notes worth defending in the paper
-----------------------------------------
* The table is REAL. Every append, delete, snapshot expiry, and orphan removal
  operates on an actual Iceberg table with real manifests and real Parquet
  files on disk. Residency is not modelled -- it is observed, by checking
  whether the bytes are still there.

* Time is VIRTUAL. Each iteration is one simulated day. Snapshot ages are
  tracked in virtual days rather than wall-clock, so a 120-day horizon with a
  30-day retention window runs in minutes.

* Merge-on-read is modelled as a DEFERRED REWRITE. pyiceberg performs
  copy-on-write deletes only, but for residency purposes MoR is exactly
  "the rewrite happens at compaction time instead of delete time" -- which is
  what the simulator does. The limitation this leaves is read-path overhead
  from delete files, which this harness does not measure. Say so in the paper.
"""

from __future__ import annotations

import shutil
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import In

from .config import Config
from .ledger import Ledger
from .policies import make_policy
from .warehouse import (SubjectIndex, referenced_data_files, remove_orphans,
                        rewrite_files)
from .workload import ICEBERG_SCHEMA, SCHEMA, SUBJECT_FIELD_ID, Workload

KEY = "subject_id"


@dataclass
class DayRecord:
    """Per-day time series, for the residency-over-time figures."""

    day: int
    n_data_files: int
    table_bytes: int
    on_disk_bytes: int
    open_obligations: int
    overdue_obligations: int
    bytes_rewritten_today: int
    expired: bool
    orphaned: bool


@dataclass
class Result:
    config: dict
    summary: dict
    timeline: list[dict]
    obligations: list[dict] = field(default_factory=list)


class Simulation:
    def __init__(self, cfg: Config, root: Path) -> None:
        self.cfg = cfg
        self.root = Path(root)
        self.workload = Workload(cfg)
        self.ledger = Ledger(deadline_days=cfg.deadline_days)
        self.policy = make_policy(cfg)
        self.index = SubjectIndex(KEY)
        self.snapshot_day: dict[int, int] = {}
        #: path -> virtual day on which the noncurrent version expires. Only
        #: populated when object_store == "versioned". math.inf means the
        #: bucket has no lifecycle rule, which is the S3 default.
        self.noncurrent: dict[str, float] = {}
        self.rows_per_subject: Counter[int] = Counter()
        self.timeline: list[DayRecord] = []

    # -- setup -------------------------------------------------------------

    def _setup(self):
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True)
        cat = SqlCatalog(
            "sim",
            uri=f"sqlite:///{self.root}/catalog.db",
            warehouse=f"file://{self.root.resolve()}",
        )
        cat.create_namespace("bench")
        self.catalog = cat

        kwargs = {}
        if self.cfg.layout == "bucketed":
            from pyiceberg.partitioning import PartitionField, PartitionSpec
            from pyiceberg.transforms import BucketTransform

            kwargs["partition_spec"] = PartitionSpec(
                PartitionField(
                    source_id=SUBJECT_FIELD_ID,
                    field_id=1000,
                    transform=BucketTransform(self.cfg.n_buckets),
                    name="subject_bucket",
                )
            )
        self.table = cat.create_table(
            "bench.events", schema=ICEBERG_SCHEMA, **kwargs
        )
        return self.table

    # -- helpers -----------------------------------------------------------

    def _current_files(self) -> list[dict]:
        return [
            r for r in self.table.inspect.files().to_pylist() if r["content"] == 0
        ]

    def _note_snapshots(self, day: int) -> None:
        for s in self.table.snapshots():
            self.snapshot_day.setdefault(s.snapshot_id, day)

    def _sync_index(self) -> tuple[dict[str, set[int]], dict[str, int]]:
        files = self._current_files()
        self.index.observe(files)
        current_paths = {f["file_path"] for f in files}
        cur_subjects = {
            p: self.index.file_subjects.get(p, set()) for p in current_paths
        }
        cur_bytes = {p: self.index.file_bytes.get(p, 0) for p in current_paths}
        return cur_subjects, cur_bytes

    def _apply_delete(self, subjects: set[int], day: int) -> int:
        """Rewrite the data files containing `subjects`. Returns bytes rewritten."""
        if not subjects:
            return 0
        before = {f["file_path"]: f["file_size_in_bytes"] for f in self._current_files()}
        try:
            self.table.delete(In(KEY, sorted(subjects)))
        except Exception:
            # No matching rows in the current snapshot; still a discharged
            # obligation from the rewrite stage's point of view.
            self.ledger.mark_rewritten(subjects, day)
            return 0
        self.table.refresh()
        after = {f["file_path"] for f in self._current_files()}
        rewritten = set(before) - after
        cost = sum(before[p] for p in rewritten)
        self.ledger.bytes_rewritten += cost
        self.ledger.mark_rewritten(subjects, day)
        self._note_snapshots(day)
        return cost

    def _expire(self, day: int) -> bool:
        """Expire snapshots older than the retention window, in virtual days."""
        current = self.table.current_snapshot()
        current_id = current.snapshot_id if current else None
        stale = [
            sid
            for sid, d in self.snapshot_day.items()
            if day - d >= self.cfg.retention_days and sid != current_id
        ]
        live_ids = {s.snapshot_id for s in self.table.snapshots()}
        stale = [s for s in stale if s in live_ids]
        if not stale:
            return False
        try:
            self.table.maintenance.expire_snapshots().by_ids(stale).commit()
            self.table.refresh()
            return True
        except Exception:
            return False

    def _on_disk_bytes(self) -> int:
        data_dir = Path(str(self.table.location()).replace("file://", "")) / "data"
        if not data_dir.exists():
            return 0
        return sum(p.stat().st_size for p in data_dir.rglob("*.parquet"))

    # -- main loop ---------------------------------------------------------

    def run(self, verbose: bool = False) -> Result:
        cfg = self.cfg
        self._setup()

        for day in range(cfg.horizon_days):
            # 1. ingest -----------------------------------------------------
            batch, subjects = self.workload.ingest_batch(day)
            self.table.append(batch)
            self.table.refresh()
            self.rows_per_subject.update(subjects)
            self._note_snapshots(day)

            cur_subjects, cur_bytes = self._sync_index()
            spent_today = 0

            # 2. erasure requests -------------------------------------------
            live = set().union(*cur_subjects.values()) if cur_subjects else set()
            new_dsars = self.workload.dsars(day, live)
            for s in new_dsars:
                self.ledger.open(s, day)
            if new_dsars:
                self.ledger.mark_logical(set(new_dsars), day)
                if cfg.delete_mode == "cow":
                    # Copy-on-write pays the rewrite immediately, budget or not.
                    spent_today += self._apply_delete(set(new_dsars), day)
                    cur_subjects, cur_bytes = self._sync_index()

            # 3. maintenance -------------------------------------------------
            decision = self.policy.decide(day, self.ledger, cur_subjects, cur_bytes)
            if decision.files_to_rewrite:
                # Rewrite exactly the chosen files, dropping every pending
                # subject present in them. An obligation is discharged
                # incrementally: it is marked rewritten only once its rows are
                # gone from the CURRENT snapshot entirely (checked below), so a
                # subject spread over many files can be cleared across days
                # while each day stays inside the budget.
                pending_ids = {o.subject for o in self.ledger.pending_rewrite()}
                cost, _ = rewrite_files(
                    self.table, decision.files_to_rewrite, pending_ids,
                    KEY, SCHEMA,
                )
                self.ledger.bytes_rewritten += cost
                spent_today += cost
                self.table.refresh()
                self._note_snapshots(day)
                cur_subjects, cur_bytes = self._sync_index()

            # A subject is "rewritten" when no current data file holds it.
            live_now = set().union(*cur_subjects.values()) if cur_subjects else set()
            done = {o.subject for o in self.ledger.pending_rewrite()
                    if o.subject not in live_now}
            if done:
                self.ledger.mark_rewritten(done, day)

            expired = self._expire(day) if decision.do_expire else False
            orphaned = False
            if decision.do_orphan_cleanup:
                versioned = cfg.object_store == "versioned"
                removed, freed, paths = remove_orphans(
                    self.table, self.root, unlink=not versioned
                )
                orphaned = removed > 0
                if versioned:
                    # Stage 5: the object is no longer current, but the bytes
                    # remain as a noncurrent version until D_nc elapses.
                    ttl = (
                        float("inf")
                        if cfg.noncurrent_expiration_days < 0
                        else day + cfg.noncurrent_expiration_days
                    )
                    for pth in paths:
                        self.noncurrent.setdefault(pth, ttl)
                    marked = self.index.subjects_in_files(set(paths))
                    self.ledger.mark_delete_marker(
                        {o.subject for o in self.ledger.pending()
                         if o.subject in marked},
                        day,
                    )
                self.index.forget_deleted()

            # Stage 5 expiry: lifecycle rule finally removes due versions.
            if self.noncurrent:
                due = [p for p, t in self.noncurrent.items() if t <= day]
                for pth in due:
                    local = pth.replace("file://", "")
                    try:
                        Path(local).unlink()
                    except OSError:
                        pass
                    del self.noncurrent[pth]
                if due:
                    self.index.forget_deleted()

            # 4. observe which stages completed -------------------------------
            referenced = referenced_data_files(self.table)
            referenced_subjects = self.index.subjects_in_files(referenced)
            surviving = self.index.surviving_subjects()

            pending = self.ledger.pending()
            gone_from_history = {
                o.subject for o in pending if o.subject not in referenced_subjects
            }
            self.ledger.mark_unreferenced(gone_from_history, day)
            physically_gone = {
                o.subject for o in pending if o.subject not in surviving
            }
            self.ledger.mark_physical(physically_gone, day)

            # 5. record --------------------------------------------------------
            files_now = self._current_files()
            overdue = sum(
                1 for o in self.ledger.pending() if day > o.deadline_day
            )
            self.timeline.append(
                DayRecord(
                    day=day,
                    n_data_files=len(files_now),
                    table_bytes=sum(f["file_size_in_bytes"] for f in files_now),
                    on_disk_bytes=self._on_disk_bytes(),
                    open_obligations=len(self.ledger.pending()),
                    overdue_obligations=overdue,
                    bytes_rewritten_today=spent_today,
                    expired=expired,
                    orphaned=orphaned,
                )
            )

            if verbose and day % 10 == 0:
                print(
                    f"  day {day:3d}  files={len(files_now):4d}  "
                    f"open={len(self.ledger.pending()):3d}  overdue={overdue:3d}  "
                    f"disk={self._on_disk_bytes()/1e6:6.1f}MB"
                )

        # denominator for amplification
        total_rows = sum(self.rows_per_subject.values())
        total_bytes = self._on_disk_bytes() or 1
        bytes_per_row = max(1.0, total_bytes / max(1, total_rows))
        erased_rows = sum(
            self.rows_per_subject[o.subject] for o in self.ledger.obligations.values()
        )
        self.ledger.subject_bytes_erased = int(erased_rows * bytes_per_row)

        return Result(
            config=self.cfg.to_dict(),
            summary=self.ledger.summary(
                cfg.horizon_days, warmup=cfg.warmup_days, tail=cfg.tail_days
            ),
            timeline=[t.__dict__ for t in self.timeline],
            obligations=[
                {
                    "subject": o.subject,
                    "issued": o.issued_day,
                    "deadline": o.deadline_day,
                    "logical": o.logical_day,
                    "rewritten": o.rewritten_day,
                    "unreferenced": o.unreferenced_day,
                    "physical": o.physical_day,
                }
                for o in self.ledger.obligations.values()
            ],
        )
