"""Are the bytes still there?  (erasure harness, unchanged in substance)

Nothing here infers residency from table metadata -- that is the layer that
already reports the row as deleted.  Every answer comes from listing object
versions under the keys the planner named before the delete and, when asked,
opening the surviving Parquet objects and counting the subject's rows.
``store`` is anything with a boto3-shaped ``list_object_versions``: the
emulated bucket or ``boto3.client("s3")``.
"""
from __future__ import annotations

import json
import os
from urllib.parse import urlparse

import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.expressions import EqualTo

from .workload import KEY


def _strip(uri: str) -> str:
    return uri[len("file://"):] if uri.startswith("file://") else uri


def planner_files(table, subject) -> list[str]:
    scan = table.scan(row_filter=EqualTo(KEY, subject))
    return sorted({_strip(t.file.file_path) for t in scan.plan_files()})


def ledger_before(table, subject, store, key_of, now: int, read_back=None) -> dict:
    entries = []
    for path in planner_files(table, subject):
        rows = read_back(path) if read_back is not None else None
        if rows == 0:
            continue
        key = key_of(path)
        resp = store.list_object_versions(Bucket="", Prefix=key)
        vs = [v for v in resp.get("Versions", []) if v["Key"] == key]
        entries.append({"path": path, "key": key, "rows_before": rows,
                        "version_ids": [v["VersionId"] for v in vs],
                        "bytes": sum(v.get("Size", 0) for v in vs)})
    snap = table.current_snapshot()
    return {"subject": subject, "requested_at": now,
            "snapshot_id": snap.snapshot_id if snap else None, "entries": entries}


def observe(ledger: dict, store, now: int, table=None, read_back=None) -> dict:
    report, remaining, rows_readable = [], 0, 0
    for e in ledger["entries"]:
        resp = store.list_object_versions(Bucket="", Prefix=e["key"])
        vs = [v for v in resp.get("Versions", []) if v["Key"] == e["key"]]
        dms = [m for m in resp.get("DeleteMarkers", []) if m["Key"] == e["key"]]
        live = [v for v in vs if (not e["version_ids"]) or v["VersionId"] in e["version_ids"]]
        b = sum(v.get("Size", 0) for v in live)
        remaining += b
        n_rows = None
        if live and read_back is not None:
            n_rows = read_back(e["path"])
            rows_readable += n_rows or 0
        report.append({"key": e["key"], "versions_remaining": len(live),
                       "noncurrent": sum(1 for v in live if not v.get("IsLatest")),
                       "delete_markers": len(dms), "bytes_remaining": b, "subject_rows_readable": n_rows})
    logically_gone = None
    if table is not None:
        logically_gone = table.scan(row_filter=EqualTo(KEY, ledger["subject"])).to_arrow().num_rows == 0
    return {"subject": ledger["subject"], "observed_at": now, "elapsed_hours": now - ledger["requested_at"],
            "logically_deleted": logically_gone, "physically_erased": remaining == 0,
            "bytes_remaining": remaining, "subject_rows_readable": rows_readable if read_back else None,
            "files": report}


def read_back_local(subject):
    def _count(path: str) -> int:
        p = _strip(path)
        if not os.path.exists(p):
            return 0
        t = pq.read_table(p, columns=[KEY])
        return int(pc.sum(pc.equal(t[KEY], subject)).as_py() or 0)
    return _count


def s3_key_of(uri: str):
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")


def write_ledger(path: str, ledger: dict):
    with open(path, "w") as fh:
        json.dump(ledger, fh, indent=2)


def read_ledger(path: str) -> dict:
    with open(path) as fh:
        return json.load(fh)
