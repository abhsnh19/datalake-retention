"""The real Iceberg table, wrapped for virtual time.

Genuine PyIceberg (>= 0.12) operations on a SQL (SQLite) catalog with a
``file://`` warehouse:

  append              a fast-append snapshot, one data file per batch
  delete_subjects     copy-on-write: PyIceberg rewrites every data file whose
                      bounds may hold the subject; the old file is DELETED in
                      the new manifest and stays referenced by older snapshots
  rewrite_files       file-granular rewrite (lhbench): exactly the chosen
                      files, dropping the given subjects, in one overwrite
                      transaction -- the primitive a byte budget needs
  expire_snapshots    by VIRTUAL age (PyIceberg stamps the wall clock);
                      keeps the current snapshot and every branch/tag head
                      (history.expire.min-snapshots-to-keep = 1); returns the
                      ids and leaves the unlink to the caller, which applies
                      either the Java action's rule (delete every file no
                      live snapshot needs) or PyIceberg's native behaviour
                      (metadata only; orphan cleanup deletes later)
  referenced          every path some live snapshot needs, read from the
                      manifests (cached: manifests are immutable)

lhbench's subject->file index read row values from Parquet; that oracle lives
in ``runner.py`` (``_subjects_in``) and is what makes "physically gone" exact.
"""
from __future__ import annotations

import os
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import And, GreaterThanOrEqual, In
from pyiceberg.manifest import DataFileContent, ManifestEntryStatus
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import BucketTransform

from .workload import ARROW_SCHEMA, ICEBERG_SCHEMA, KEY, SUBJECT_FIELD_ID

TABLE_PROPERTIES = {
    "format-version": "2",                       # PyIceberg cannot write v3
    "write.parquet.compression-codec": "zstd",
    "history.expire.max-snapshot-age-ms": str(5 * 86400 * 1000),   # documented default, for the audit trail
    "history.expire.min-snapshots-to-keep": "1",
    "gc.enabled": "true",
}


def _strip(uri: str) -> str:
    return uri[len("file://"):] if uri.startswith("file://") else uri


def subjects_in(path: str) -> set[int]:
    t = pq.read_table(_strip(path), columns=[KEY])
    return set(pc.unique(t[KEY]).to_pylist())


class HarnessTable:
    def __init__(self, warehouse: str, name: str = "bench.events", layout: str = "scattered",
                 n_buckets: int = 16):
        self.warehouse = os.path.abspath(warehouse)
        self.name = name
        self.layout = layout
        self.n_buckets = n_buckets
        os.makedirs(self.warehouse, exist_ok=True)
        self.catalog = SqlCatalog("residency", uri=f"sqlite:///{self.warehouse}/catalog.db",
                                  warehouse=f"file://{self.warehouse}")
        self.table = None
        self.snapshot_vtime: dict[int, int] = {}
        self._manifest_cache: dict[str, list] = {}
        self.commits = 0

    # ------------------------------------------------------------ lifecycle
    def create(self):
        ns = self.name.split(".")[0]
        self.catalog.create_namespace_if_not_exists(ns)
        kw = {}
        if self.layout == "bucketed":
            kw["partition_spec"] = PartitionSpec(PartitionField(
                source_id=SUBJECT_FIELD_ID, field_id=1000, transform=BucketTransform(self.n_buckets),
                name="subject_bucket"))
        self.table = self.catalog.create_table(self.name, schema=ICEBERG_SCHEMA, properties=TABLE_PROPERTIES, **kw)
        return self

    def location(self) -> str:
        return _strip(self.table.location())

    @property
    def io(self):
        return self.table.io

    def _stamp(self, vhour: int):
        snap = self.table.current_snapshot()
        self.snapshot_vtime[snap.snapshot_id] = vhour
        self.commits += 1
        return snap

    # -------------------------------------------------------------- writes
    def append(self, rows: pa.Table, vhour: int) -> dict[str, int]:
        """Append one batch; returns {added data-file path: size}."""
        self.table.append(rows)
        snap = self._stamp(vhour)
        return {p: size for p, st, size in self._entries(snap) if st == ManifestEntryStatus.ADDED}

    def delete_subjects(self, subjects: Iterable[int], vhour: int, min_day: int | None = None) -> dict:
        """Copy-on-write delete of every row of the given subjects.  Returns
        the files removed and added by the commit with their sizes."""
        subjects = sorted(set(subjects))
        if not subjects:
            return {"removed": {}, "added": {}, "committed": False}
        expr = In(KEY, subjects)
        if min_day is not None:
            expr = And(expr, GreaterThanOrEqual("event_day", min_day))
        before = self.table.current_snapshot().snapshot_id
        self.table.delete(expr)
        snap = self.table.current_snapshot()
        if snap.snapshot_id == before:
            return {"removed": {}, "added": {}, "committed": False}
        self._stamp(vhour)
        return self._diff(snap)

    def rewrite_files(self, paths: Iterable[str], drop_subjects: Iterable[int], vhour: int) -> dict:
        """Rewrite exactly ``paths`` (current data files), dropping rows of
        ``drop_subjects``.  One overwrite transaction; files whose every row
        is dropped are removed without a replacement."""
        from pyiceberg.io.pyarrow import _dataframe_to_data_files
        want = {_strip(p) for p in paths}
        by_path = {_strip(t.file.file_path): t.file for t in self.table.scan().plan_files()}
        targets = [p for p in sorted(by_path) if p in want]
        if not targets:
            return {"removed": {}, "added": {}, "committed": False}
        drop = pa.array(sorted(set(drop_subjects)), type=pa.int64())
        with self.table.transaction() as tx:
            ov = tx.update_snapshot().overwrite()
            for p in targets:
                old = by_path[p]
                tbl = pq.read_table(p)
                keep = pc.invert(pc.is_in(tbl[KEY], value_set=drop))
                new_tbl = tbl.filter(keep).cast(ARROW_SCHEMA)
                ov.delete_data_file(old)
                if new_tbl.num_rows:
                    for ndf in _dataframe_to_data_files(tx.table_metadata, new_tbl, self.table.io):
                        ov.append_data_file(ndf)
            ov.commit()
        self.table = self.catalog.load_table(self.name)
        snap = self._stamp(vhour)
        return self._diff(snap)

    def _diff(self, snap) -> dict:
        removed, added = {}, {}
        for p, st, size in self._entries(snap):
            if st == ManifestEntryStatus.DELETED:
                removed[p] = size
            elif st == ManifestEntryStatus.ADDED:
                added[p] = size
        return {"removed": removed, "added": added, "committed": True}

    # ---------------------------------------------------------- references
    def _manifest_entries(self, manifest):
        path = manifest.manifest_path
        if path not in self._manifest_cache:
            entries = manifest.fetch_manifest_entry(self.io, discard_deleted=False)
            self._manifest_cache[path] = [
                (_strip(e.data_file.file_path), e.status, e.data_file.file_size_in_bytes, e.data_file.content)
                for e in entries]
        return self._manifest_cache[path]

    def _entries(self, snap):
        out = []
        for m in snap.manifests(self.io):
            if m.added_snapshot_id != snap.snapshot_id:
                continue
            for p, st, size, _c in self._manifest_entries(m):
                out.append((p, st, size))
        return out

    def live_snapshots(self):
        return list(self.table.snapshots())

    def referenced(self) -> set[str]:
        """Every path some live snapshot still needs: data files ADDED or
        EXISTING in one of its manifests, the manifests, the manifest lists
        and the metadata files."""
        refs: set[str] = set()
        for snap in self.live_snapshots():
            refs.add(_strip(snap.manifest_list))
            for m in snap.manifests(self.io):
                refs.add(_strip(m.manifest_path))
                for p, st, _size, _c in self._manifest_entries(m):
                    if st != ManifestEntryStatus.DELETED:
                        refs.add(p)
        refs.add(_strip(self.table.metadata_location))
        for log in self.table.metadata.metadata_log:
            refs.add(_strip(log.metadata_file))
        return refs

    def current_data_files(self) -> dict[str, int]:
        snap = self.table.current_snapshot()
        out = {}
        if snap is None:
            return out
        for m in snap.manifests(self.io):
            for p, st, size, content in self._manifest_entries(m):
                if st != ManifestEntryStatus.DELETED and content == DataFileContent.DATA:
                    out[p] = size
        return out

    # ---------------------------------------------------------- maintenance
    def expire_snapshots(self, vhour: int, retention_hours: int) -> list[int]:
        """Expire every unprotected snapshot whose virtual age exceeds the
        retention; the current snapshot and every ref head are kept.  Returns
        the expired ids.  Files are NOT deleted here (see the runner)."""
        current = self.table.current_snapshot().snapshot_id
        protected = {ref.snapshot_id for ref in self.table.metadata.refs.values()}
        ids = [s.snapshot_id for s in self.live_snapshots()
               if s.snapshot_id != current and s.snapshot_id not in protected
               and self.snapshot_vtime.get(s.snapshot_id, vhour) <= vhour - retention_hours]
        if not ids:
            return []
        self.table.maintenance.expire_snapshots().by_ids(ids).commit()
        self.table = self.catalog.load_table(self.name)
        for i in ids:
            self.snapshot_vtime.pop(i, None)
        return ids
