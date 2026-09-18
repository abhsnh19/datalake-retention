"""Documented floors, with the sentence each one comes from.

Nothing here is measured.  These are the defaults a table format or a bucket
ships with, quoted so the deck can label them "documented" and a reader can
check them.  Read 14-15 September 2026.
"""
from __future__ import annotations

ICEBERG = {
    "label": "Apache Iceberg",
    "snapshot_retention_days": 5,
    "min_snapshots_to_keep": 1,
    "orphan_older_than_days": 3,
    "delete_mode_default": "copy-on-write",
    "expiry_deletes_files": True,     # the Java/Spark action; PyIceberg's expire_snapshots rewrites metadata only
    "sources": [
        "Configuration: history.expire.max-snapshot-age-ms = 432000000 (5 days); "
        "history.expire.min-snapshots-to-keep = 1; write.delete.mode = copy-on-write; "
        "format-version defaults to 2 since 1.4.0",
        "Spark procedures: expire_snapshots older_than 'Default: 5 days ago', retain_last 'Default: 1'; "
        "remove_orphan_files older_than 'Default: 3 days ago'; rewrite_data_files min-input-files 5",
    ],
}

S3_TABLES = {
    "label": "AWS S3 Tables (managed Iceberg)",
    "snapshot_retention_days": 5,           # maxSnapshotAgeHours 120, minSnapshotsToKeep 1
    "min_snapshots_to_keep": 1,
    "unreferenced_days": 3,
    "noncurrent_days": 10,
    "sources": [
        "Maintenance for table buckets: unreferencedDays (3 days by default) and nonCurrentDays (10 days by default); "
        "'S3 marks the object as noncurrent' after unreferencedDays and 'deletes noncurrent objects after' nonCurrentDays",
    ],
}

DELTA = {
    "label": "Delta Lake",
    "deleted_file_retention_days": 7,       # delta.deletedFileRetentionDuration = interval 7 days
    "log_retention_days": 30,               # delta.logRetentionDuration = interval 30 days
    "vacuum_automatic": False,
    "sources": [
        "Table properties: delta.deletedFileRetentionDuration 'The default is interval 7 days'; "
        "delta.logRetentionDuration 'The default is interval 30 days'",
        "VACUUM: 'The default retention threshold for the files is 7 days'; 'vacuum is not triggered automatically'; "
        "'Delta Lake has a safety check to prevent you from running a dangerous VACUUM command' "
        "(spark.databricks.delta.retentionDurationCheck.enabled); 'The data files backing a Delta table are never deleted automatically'",
    ],
}

HUDI = {
    "label": "Apache Hudi",
    "cleaner_policy": "KEEP_LATEST_COMMITS",
    "commits_retained": 10,
    "clean_automatic": True,
    "sources": [
        "Cleaning: hoodie.cleaner.policy default KEEP_LATEST_COMMITS; hoodie.clean.commits.retained default 10; "
        "hoodie.clean.automatic default true ('the cleaner table service is invoked immediately after each commit'); "
        "KEEP_LATEST_BY_HOURS with hoodie.clean.hours.retained is the time-denominated alternative",
    ],
}

STORES = {
    "s3": {"default_rule": None, "noncurrent_days_min": 1, "rounding": "midnight UTC",
           "sources": ["A bucket has no lifecycle configuration until one is written (NoSuchLifecycleConfiguration); "
                       "NoncurrentVersionExpiration rounds 'to the next day at midnight UTC'; removal is asynchronous "
                       "('There may be a delay between the expiration date and the date at which Amazon S3 removes an object')"]},
    "gcs": {"soft_delete_default_days": 7, "range": (7, 90), "zero_disables": True,
            "sources": ["'Soft delete is enabled by default for all buckets that support it, with a default retention duration of 7 days'; "
                        "'between 7 to 90 days'; 'set the retention duration to 0' to disable"]},
    "azure": {"soft_delete_portal_default_days": 7, "range": (1, 365),
              "sources": ["'a retention period for deleted objects of between 1 and 365 days'; portal-created accounts enable it, "
                          "PowerShell/CLI-created accounts do not; versioning 'isn't supported for accounts that have a hierarchical namespace'"]},
    "minio": {"count_only_ilm_days": 0,
              "sources": ["lhbench objstore suite: excess noncurrent versions removed with no waiting period"]},
}


def hudi_floor_days(commits_retained: int = HUDI["commits_retained"], commits_per_day: float = 24.0) -> float:
    """The cleaner keeps a COUNT of commits; its duration is count / cadence."""
    if commits_per_day <= 0:
        raise ValueError("commits_per_day must be positive")
    if commits_retained < 0:
        raise ValueError("commits_retained must be non-negative")
    return commits_retained / commits_per_day


def table_floor(fmt: str, commits_per_day: float = 24.0) -> dict:
    """Days the table format holds a superseded file before its own cleanup
    may delete it, from the documented defaults.  Iceberg counts two floors
    (snapshot retention; orphan age) because they are configured and audited
    separately; only retention gates a data file that expiry itself deletes."""
    if fmt == "iceberg":
        return {"format": fmt, "floor_days": ICEBERG["snapshot_retention_days"],
                "second_floor_days": ICEBERG["orphan_older_than_days"], "unit": "days"}
    if fmt == "s3tables":
        return {"format": fmt, "floor_days": S3_TABLES["snapshot_retention_days"],
                "second_floor_days": S3_TABLES["unreferenced_days"], "store_days": S3_TABLES["noncurrent_days"],
                "unit": "days"}
    if fmt == "delta":
        return {"format": fmt, "floor_days": DELTA["deleted_file_retention_days"], "second_floor_days": 0, "unit": "days"}
    if fmt == "hudi":
        return {"format": fmt, "floor_days": hudi_floor_days(commits_per_day=commits_per_day),
                "second_floor_days": 0, "unit": "commits", "commits_retained": HUDI["commits_retained"],
                "commits_per_day": commits_per_day}
    raise KeyError(f"unknown format {fmt!r}")
