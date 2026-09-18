"""The thirty-second check, the budget, and the floors that are not in days.

Both harnesses reached the same inequality by different routes:

    lhbench      retention + D_nc <= compliance window          (day clock)
    erasure      commit batch + retention + expiry cadence
                   + NoncurrentDays + 1 (midnight rounding) <= window   (hour clock)

They are the same check with the erasure form spelling out the terms the
day clock folds away.  ``floor_days`` carries every term, including the one
that only appears when snapshot expiry does NOT delete the files it
unreferences (PyIceberg's native expire_snapshots; S3 Tables' managed job):
then the orphan job's cadence and its ``older_than`` age gate both count.
The guaranteed bound is stated separately from the empirical one the sweep
reports (the largest N with zero misses across seeds).
"""
from __future__ import annotations

import json


def floor_days(noncurrent_days: int, snapshot_retention_days: int = 5,
               expire_cadence_days: int = 1, commit_days: int = 1,
               orphan_cadence_days: int = 0, orphan_older_than_days: int = 0,
               rounding_days: int = 1) -> float:
    """Worst-case days from arrival to bytes gone for a rewrite at commit.
    ``orphan_cadence_days`` and ``orphan_older_than_days`` are 0 when expiry
    deletes files itself (the Iceberg Java action); pass the orphan job's
    interval and age gate when it does not."""
    for name, v in (("noncurrent_days", noncurrent_days), ("snapshot_retention_days", snapshot_retention_days),
                    ("expire_cadence_days", expire_cadence_days), ("commit_days", commit_days),
                    ("orphan_cadence_days", orphan_cadence_days), ("orphan_older_than_days", orphan_older_than_days),
                    ("rounding_days", rounding_days)):
        if v is None or v < 0:
            raise ValueError(f"{name} must be a non-negative number, got {v!r}")
    return (commit_days + snapshot_retention_days + expire_cadence_days
            + max(orphan_cadence_days, orphan_older_than_days) + noncurrent_days + rounding_days)


def max_noncurrent_days(window_days: int = 30, **kw) -> int:
    """Largest NoncurrentDays guaranteed to fit inside the window (0 when none does)."""
    n = 0
    while floor_days(n + 1, **kw) <= window_days:
        n += 1
    return n


def table_floor_days(snapshot_retention_days: int = 5, expire_cadence_days: int = 1,
                     commit_days: int = 1, orphan_cadence_days: int = 0,
                     orphan_older_than_days: int = 0) -> float:
    """Worst-case days on the table side, arrival to the DELETE."""
    return (commit_days + snapshot_retention_days + expire_cadence_days
            + max(orphan_cadence_days, orphan_older_than_days))


def check(window_days: int, table_floor_days: float, store_expiry_days: float | None) -> dict:
    """``store_expiry_days`` includes the store's rounding (N + 1 on S3);
    None means unbounded."""
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if store_expiry_days is None:
        return {"feasible": False, "headroom_days": None,
                "reason": "the object store never frees the bytes: the sum is unbounded"}
    total = table_floor_days + store_expiry_days
    return {"feasible": total <= window_days, "headroom_days": window_days - total,
            "reason": ("" if total <= window_days else
                       f"table side {table_floor_days:g} d + store {store_expiry_days:g} d = {total:g} d "
                       f"exceeds the {window_days}-day window before any work happens")}


def lhbench_check(window_days: int, retention_days: int, noncurrent_days: int | None) -> dict:
    """lhbench's statement of the same test: retention + D_nc <= window."""
    if noncurrent_days is None:
        return {"feasible": False, "floor_days": None, "reason": "no lifecycle rule: D_nc is unbounded"}
    floor = retention_days + noncurrent_days
    return {"feasible": floor <= window_days, "floor_days": floor,
            "reason": "" if floor <= window_days else f"retention {retention_days} + D_nc {noncurrent_days} = {floor} > {window_days}"}


def plan(window_days: int = 30, snapshot_retention_days: int = 5, expire_cadence_days: int = 1,
         commit_days: int = 1, margin_days: int = 1, orphan_cadence_days: int = 0,
         orphan_older_than_days: int = 0) -> dict:
    """Split the window across the stages, from the tail inwards."""
    tail = (commit_days + snapshot_retention_days + expire_cadence_days
            + max(orphan_cadence_days, orphan_older_than_days) + 1 + margin_days)
    n = window_days - tail
    if n < 1:
        raise ValueError(f"a {window_days}-day window is infeasible: the tail alone needs {tail} days")
    return {"window_days": window_days, "commit_days": commit_days,
            "snapshot_retention_days": snapshot_retention_days,
            "expire_cadence_days": expire_cadence_days,
            "orphan_days": max(orphan_cadence_days, orphan_older_than_days),
            "midnight_batch_days": 1, "margin_days": margin_days, "noncurrent_days": n,
            "rewrite_budget_days": window_days - (tail + n)}


def s3_lifecycle_document(prefix: str, noncurrent_days: int, rule_id: str = "erasure-deadline") -> dict:
    """PutBucketLifecycleConfiguration payload.  No NewerNoncurrentVersions:
    that element retains versions by count regardless of age and would
    silently void the deadline."""
    if noncurrent_days < 1:
        raise ValueError("AWS requires NoncurrentDays to be a positive integer")
    return {"Rules": [{
        "ID": rule_id, "Status": "Enabled", "Filter": {"Prefix": prefix},
        "NoncurrentVersionExpiration": {"NoncurrentDays": int(noncurrent_days)},
        "Expiration": {"ExpiredObjectDeleteMarker": True},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
    }]}


def s3tables_maintenance_document(unreferenced_days: int = 3, noncurrent_days: int = 10) -> dict:
    """put-table-bucket-maintenance-configuration value for unreferenced file removal."""
    if unreferenced_days < 0 or noncurrent_days < 0:
        raise ValueError("days must be non-negative")
    return {"status": "enabled", "settings": {"icebergUnreferencedFileRemoval": {
        "unreferencedDays": int(unreferenced_days), "nonCurrentDays": int(noncurrent_days)}}}


def iceberg_properties(snapshot_retention_days: int) -> dict:
    return {"history.expire.max-snapshot-age-ms": str(snapshot_retention_days * 86400 * 1000),
            "history.expire.min-snapshots-to-keep": "1", "gc.enabled": "true"}


def render(p: dict, prefix: str = "warehouse/prod/events/") -> str:
    return json.dumps({"plan": p, "iceberg": iceberg_properties(p["snapshot_retention_days"]),
                       "s3_lifecycle": s3_lifecycle_document(prefix, p["noncurrent_days"])}, indent=2)
