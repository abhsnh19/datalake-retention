"""Warehouse-level operations Iceberg's Python client does not provide.

Two things live here:

  * an exact subject->file index, maintained incrementally, which lets the
    simulator answer "is this subject physically gone?" with certainty rather
    than an estimate;
  * orphan-file removal, which pyiceberg has no API for, and which is the
    stage that actually reclaims bytes.

The index reads row values. That is legitimate here and NOT a contradiction of
the metadata-only stance in scatter.py: this is the simulator's oracle, used to
establish ground truth. The analyzer is the tool you point at real tables.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow.parquet as pq


def _local(path: str) -> str:
    return path.replace("file://", "")


class SubjectIndex:
    """Maps data files to the subjects they contain, for every file that has
    ever existed and not yet been physically deleted."""

    def __init__(self, key_column: str = "subject_id") -> None:
        self.key = key_column
        self.file_subjects: dict[str, set[int]] = {}
        self.file_bytes: dict[str, int] = {}

    def observe(self, files: list[dict]) -> None:
        """Ingest the current file list; read any file not seen before."""
        for f in files:
            path = f["file_path"]
            if path in self.file_subjects:
                continue
            self.file_bytes[path] = f["file_size_in_bytes"]
            local = _local(path)
            if not os.path.exists(local):
                self.file_subjects[path] = set()
                continue
            col = pq.read_table(local, columns=[self.key])[self.key].to_pylist()
            self.file_subjects[path] = set(col)

    def forget_deleted(self) -> None:
        """Drop index entries for files no longer on disk."""
        gone = [p for p in self.file_subjects if not os.path.exists(_local(p))]
        for p in gone:
            del self.file_subjects[p]
            self.file_bytes.pop(p, None)

    def files_containing(self, subject: int) -> list[str]:
        return [p for p, s in self.file_subjects.items() if subject in s]

    def surviving_subjects(self) -> set[int]:
        """Subjects present in any file still on disk."""
        out: set[int] = set()
        for p, s in self.file_subjects.items():
            if os.path.exists(_local(p)):
                out |= s
        return out

    def subjects_in_files(self, paths: set[str]) -> set[int]:
        out: set[int] = set()
        for p in paths:
            out |= self.file_subjects.get(p, set())
        return out


def referenced_data_files(table) -> set[str]:
    """Every data file referenced by any RETAINED snapshot.

    `all_files` walks all manifests reachable from retained snapshots, which is
    exactly the set that snapshot expiry has not yet released.
    """
    try:
        rows = table.inspect.all_files().to_pylist()
    except Exception:
        rows = table.inspect.files().to_pylist()
    return {r["file_path"] for r in rows}


def remove_orphans(
    table, warehouse_root: Path, unlink: bool = True
) -> tuple[int, int, set[str]]:
    """Delete data files under the table's data dir that no retained snapshot
    references. Returns (files_removed, bytes_removed, removed_paths).

    This is the stage that actually reclaims storage, and the one most often
    deferred in practice.

    With `unlink=False` the orphans are IDENTIFIED but the bytes are left in
    place. That models an object store with bucket versioning enabled: the
    DELETE creates a delete marker and the object stops being current, but the
    bytes survive as a noncurrent version until a lifecycle rule expires them.
    The caller owns that expiry clock.
    """
    referenced = {_local(p) for p in referenced_data_files(table)}
    data_dir = Path(_local(table.location())) / "data"
    if not data_dir.exists():
        return 0, 0, set()

    removed, freed, paths = 0, 0, set()
    for p in data_dir.rglob("*.parquet"):
        sp = str(p)
        if sp in referenced:
            continue
        try:
            freed += p.stat().st_size
            if unlink:
                p.unlink()
            removed += 1
            paths.add(f"file://{sp}")
        except OSError:
            pass
    return removed, freed, paths


def rewrite_files(table, paths: set[str], drop_subjects: set[int],
                  key: str, arrow_schema) -> tuple[int, int]:
    """Rewrite exactly `paths`, dropping rows whose `key` is in `drop_subjects`.

    This is the primitive that makes a rewrite budget enforceable. The obvious
    approach -- `table.delete(In(key, subjects))` -- rewrites EVERY file
    containing those subjects, so the cheapest possible action is already the
    whole blast radius of an erasure and a small budget cannot be honoured.
    Selecting files instead makes the file the unit of work, so an obligation
    can be discharged partially, across days, within a cap.

    Returns (bytes_rewritten, files_rewritten).
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    from pyiceberg.io.pyarrow import _dataframe_to_data_files

    if not paths:
        return 0, 0
    # plan_files yields real DataFile objects; inspect.files() only gives dicts,
    # and delete_data_file needs the object.
    by_path = {t.file.file_path: t.file for t in table.scan().plan_files()}
    targets = [p for p in paths if p in by_path]
    if not targets:
        return 0, 0

    drop = pa.array(sorted(drop_subjects), type=pa.int64())
    written = 0
    with table.transaction() as tx:
        ov = tx.update_snapshot().overwrite()
        for p in targets:
            old = by_path[p]
            tbl = pq.read_table(_local(p))
            keep = pc.invert(pc.is_in(tbl[key], value_set=drop))
            new_tbl = tbl.filter(keep).cast(arrow_schema)
            ov.delete_data_file(old)
            if new_tbl.num_rows:
                for ndf in _dataframe_to_data_files(tx.table_metadata, new_tbl,
                                                    table.io):
                    ov.append_data_file(ndf)
            written += old.file_size_in_bytes
        ov.commit()
    return written, len(targets)
