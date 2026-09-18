"""The bucket underneath the warehouse, emulated from the documentation.

PyIceberg writes real files into a local directory; ``ObjectStore`` is the
bucket those files live in.  Every object is a *version* with the virtual
hour it was PUT.  Physical removal is a real ``os.remove``: when the
emulation says the bytes are gone they are gone from disk, and ``probe.py``
can check independently.  ``list_object_versions`` returns the boto3
response shape so the probe runs unchanged against a real bucket.

Modes and the sentences they implement
--------------------------------------
UNVERSIONED  (``mode="unversioned"``, also a plain filesystem, lhbench "none")
    A DELETE permanently removes the object.  Adds 0 days.

VERSIONED, S3 semantics  (``mode="versioned"``, ``noncurrent_days=N | None``)
    "When versioning is enabled, a simple DELETE cannot permanently delete an
    object. ... Instead, Amazon S3 inserts a delete marker in the bucket."
        -- S3 User Guide, "Deleting object versions from a versioning-enabled bucket"
    "NoncurrentVersionExpiration ... permanently delete noncurrent versions of
    objects. ... based on a certain number of days since the objects became
    noncurrent."  Evaluated in a daily batch, rounded to midnight UTC (vclock).
        -- S3 User Guide, "Lifecycle configuration elements"
    ``noncurrent_days=None``: no rule.  A bucket has no lifecycle configuration
    until its owner writes one (GetBucketLifecycleConfiguration returns
    NoSuchLifecycleConfiguration).  Adds: unbounded.
    ``noncurrent_days=0``: MinIO's count-only ILM ("excess noncurrent versions
    removed with no waiting period", lhbench) -- freed at the next batch.
    AWS itself requires NoncurrentDays to be a positive integer; 0 is
    accepted here only to model that other store.

SOFT DELETE  (``mode="soft_delete"``, ``soft_delete_days=D``, provider gcs|azure)
    Google Cloud Storage: "Soft delete is enabled by default for all buckets
    that support it, with a default retention duration of 7 days."  "You can
    customize the retention duration to anywhere between 7 to 90 days."  "To
    disable soft delete, you set the retention duration to 0."  "Soft-deleted
    objects continue to accrue storage charges until their retention period
    expires."  -- docs.cloud.google.com/storage/docs/soft-delete
    Azure Blob Storage: "you specify a retention period for deleted objects of
    between 1 and 365 days."  "All soft-deleted data is billed at the same
    rate as active data."  Portal-created accounts enable it (7 d); accounts
    created with PowerShell or the CLI do not.
        -- learn.microsoft.com, "Soft delete for blobs"
    A duration measured from the delete; no midnight rounding.

OBJECT LOCK, compliance mode  (``mode="object_lock"``, ``lock_retain_days``)
    "In compliance mode, a protected object version can't be overwritten or
    deleted by any user, including the root user in your AWS account."
    "Amazon S3 doesn't take any action on noncurrent versions of objects that
    have the S3 Object Lock configuration applied."  ``None`` = locked for the
    whole run: adds unbounded.

S3 TABLES  (``mode="s3tables"``, ``unreferenced_days=3``, ``noncurrent_days=10``)
    "you can configure two properties: unreferencedDays (3 days by default)
    and nonCurrentDays (10 days by default).  For any object not referenced by
    your table and older than the unreferencedDays property, S3 marks the
    object as noncurrent.  S3 deletes noncurrent objects after the number of
    days specified by the nonCurrentDays property."
        -- S3 User Guide, "Maintenance for table buckets"
    The bucket's own job does the unlink (policy preset ``s3tables-managed``
    runs it daily with the unreferencedDays gate); the store then behaves as
    versioned with N = nonCurrentDays.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from .vclock import HOURS_PER_DAY, ceil_to_midnight, hour_of_day

MODES = ("unversioned", "versioned", "soft_delete", "object_lock", "s3tables")
SOFT_DELETE_RANGES = {"gcs": (7, 90), "azure": (1, 365)}   # 0 disables on both


@dataclass
class Version:
    key: str
    version_id: str
    size: int
    put_at: int
    is_latest: bool = True
    noncurrent_since: int | None = None
    expired_at: int | None = None

    @property
    def on_disk(self) -> bool:
        return self.expired_at is None


@dataclass
class DeleteMarker:
    key: str
    version_id: str
    created_at: int
    removed_at: int | None = None


@dataclass
class StoreSpec:
    mode: str
    noncurrent_days: int | None = None
    soft_delete_days: int = 7
    soft_delete_provider: str = "gcs"
    lock_retain_days: int | None = None
    unreferenced_days: int = 3
    label: str = ""

    def __post_init__(self):
        if self.mode not in MODES:
            raise ValueError(f"unknown store mode {self.mode!r}; expected one of {MODES}")
        if self.mode == "versioned" and self.noncurrent_days is not None:
            if not isinstance(self.noncurrent_days, int) or self.noncurrent_days < 0:
                raise ValueError("noncurrent_days must be a non-negative integer or None (no rule)")
        if self.mode == "s3tables":
            if self.noncurrent_days is None:
                self.noncurrent_days = 10
            if self.noncurrent_days < 0 or self.unreferenced_days < 0:
                raise ValueError("s3tables days must be non-negative")
        if self.mode == "soft_delete":
            if self.soft_delete_provider not in SOFT_DELETE_RANGES:
                raise ValueError(f"soft_delete_provider must be one of {tuple(SOFT_DELETE_RANGES)}")
            lo, hi = SOFT_DELETE_RANGES[self.soft_delete_provider]
            d = self.soft_delete_days
            if not isinstance(d, int) or d < 0 or (d != 0 and not lo <= d <= hi):
                raise ValueError(f"{self.soft_delete_provider} soft delete must be 0 (off) or {lo}..{hi} days, got {d}")
        if self.mode == "object_lock" and self.lock_retain_days is not None and self.lock_retain_days <= 0:
            raise ValueError("lock_retain_days must be positive or None")
        if not self.label:
            self.label = self._default_label()

    def _default_label(self) -> str:
        if self.mode == "unversioned":
            return "S3 · versioning off"
        if self.mode == "versioned":
            if self.noncurrent_days is None:
                return "S3 · versioned, no lifecycle rule"
            if self.noncurrent_days == 0:
                return "MinIO · count-only ILM (0 d)"
            return f"S3 · versioned + NoncurrentDays {self.noncurrent_days}"
        if self.mode == "soft_delete":
            who = "GCS" if self.soft_delete_provider == "gcs" else "Azure"
            return f"{who} · soft delete {self.soft_delete_days} d"
        if self.mode == "s3tables":
            return f"AWS S3 Tables · unreferenced {self.unreferenced_days} d + noncurrent {self.noncurrent_days} d"
        return "S3 Object Lock · COMPLIANCE"

    @property
    def bounded(self) -> bool:
        if self.mode in ("unversioned", "soft_delete", "s3tables"):
            return True
        if self.mode == "versioned":
            return self.noncurrent_days is not None
        return self.lock_retain_days is not None

    @property
    def tail_days(self) -> int:
        """Days the store keeps bytes after the table's DELETE, before
        rounding.  Unbounded configurations return 0 (no schedule can meet a
        deadline there; the policy rewrites as soon as possible)."""
        if self.mode in ("versioned", "s3tables"):
            return self.noncurrent_days or 0
        if self.mode == "soft_delete":
            return self.soft_delete_days
        if self.mode == "object_lock":
            return self.lock_retain_days or 0
        return 0

    @property
    def rounding_days(self) -> int:
        """S3's midnight batch can add up to a day; a soft-delete duration cannot."""
        return 1 if self.mode in ("versioned", "s3tables") and self.bounded else 0

    @property
    def managed_unlink(self) -> bool:
        return self.mode == "s3tables"

    def as_dict(self) -> dict:
        return {"mode": self.mode, "noncurrent_days": self.noncurrent_days,
                "soft_delete_days": self.soft_delete_days,
                "soft_delete_provider": self.soft_delete_provider,
                "lock_retain_days": self.lock_retain_days,
                "unreferenced_days": self.unreferenced_days, "label": self.label,
                "bounded": self.bounded, "tail_days": self.tail_days}

    @classmethod
    def from_dict(cls, d: dict) -> "StoreSpec":
        return cls(**{k: v for k, v in d.items() if k not in ("bounded", "tail_days")})


PRESETS = {
    "s3-unversioned": lambda: StoreSpec("unversioned"),
    "s3-versioned-no-rule": lambda: StoreSpec("versioned", noncurrent_days=None),
    "s3-versioned-7d": lambda: StoreSpec("versioned", noncurrent_days=7),
    "s3-versioned-1d": lambda: StoreSpec("versioned", noncurrent_days=1),
    "minio-count-only": lambda: StoreSpec("versioned", noncurrent_days=0),
    "gcs-default": lambda: StoreSpec("soft_delete", soft_delete_days=7, soft_delete_provider="gcs"),
    "azure-portal-default": lambda: StoreSpec("soft_delete", soft_delete_days=7, soft_delete_provider="azure"),
    "s3tables-default": lambda: StoreSpec("s3tables"),
    "s3-object-lock-compliance": lambda: StoreSpec("object_lock"),
}


def preset(name: str) -> StoreSpec:
    try:
        return PRESETS[name]()
    except KeyError:
        raise KeyError(f"unknown store preset {name!r}; choose from {sorted(PRESETS)}") from None


class ObjectStore:
    def __init__(self, root: str, spec: StoreSpec):
        self.root = os.path.abspath(root)
        self.spec = spec
        self.versions: dict[str, Version] = {}
        self.markers: dict[str, DeleteMarker] = {}
        self._seq = 0
        self.stats = {"puts": 0, "deletes": 0, "expired": 0, "bytes_freed": 0}

    # ------------------------------------------------------------------ keys
    def key_of(self, path: str) -> str:
        p = path[len("file://"):] if path.startswith("file://") else path
        p = os.path.abspath(p)
        if not p.startswith(self.root + os.sep):
            raise ValueError(f"{path} is outside the bucket root {self.root}")
        return p[len(self.root) + 1:]

    def path_of(self, key: str) -> str:
        return os.path.join(self.root, key)

    def _vid(self) -> str:
        self._seq += 1
        return f"v{self._seq:06d}"

    # ------------------------------------------------------------------ PUT
    def sync(self, now: int) -> list[str]:
        """Register every file on disk the ledger has not seen as a PUT at
        ``now``.  Iceberg never overwrites a path (UUID file names), so a key
        has one object version; that is checked."""
        new = []
        for dirpath, _dirs, files in os.walk(self.root):
            for fn in files:
                p = os.path.join(dirpath, fn)
                key = p[len(self.root) + 1:]
                if key in self.versions:
                    continue
                self.versions[key] = Version(key, self._vid(), os.path.getsize(p), now)
                self.stats["puts"] += 1
                new.append(key)
        for key, v in self.versions.items():
            if v.on_disk and not os.path.exists(self.path_of(key)):
                raise RuntimeError(f"{key} vanished from disk without a DELETE")
        return new

    # --------------------------------------------------------------- DELETE
    def delete(self, key: str, now: int) -> str:
        """A simple DELETE (no version id): what snapshot expiry, orphan
        cleanup and VACUUM issue.  Returns what happened."""
        v = self.versions.get(key)
        if v is None:
            raise KeyError(key)
        if not v.on_disk:
            return "gone"
        if not v.is_latest:
            return "already-noncurrent"
        self.stats["deletes"] += 1
        mode = self.spec.mode
        if mode == "unversioned" or (mode == "soft_delete" and self.spec.soft_delete_days == 0):
            self._free(v, now)
            return "freed"
        v.is_latest = False
        v.noncurrent_since = now
        if mode in ("versioned", "object_lock", "s3tables"):
            self.markers[key] = DeleteMarker(key, self._vid(), now)
            return "delete-marker"
        return "soft-deleted"

    def _free(self, v: Version, now: int):
        p = self.path_of(v.key)
        if os.path.exists(p):
            os.remove(p)
        v.expired_at = now
        self.stats["expired"] += 1
        self.stats["bytes_freed"] += v.size

    # ------------------------------------------------------------ lifecycle
    def run_lifecycle(self, now: int) -> list[str]:
        """Apply the store's own expiry at virtual hour ``now``.  Versioned
        (and S3 Tables) act in the midnight batch with rounding; soft delete
        is a duration evaluated every hour; Object Lock frees only after the
        retain-until date."""
        freed = []
        mode = self.spec.mode
        if mode in ("versioned", "s3tables"):
            if self.spec.noncurrent_days is None or hour_of_day(now) != 0:
                return freed
            n_h = self.spec.noncurrent_days * HOURS_PER_DAY
            for v in self.versions.values():
                if v.is_latest or not v.on_disk:
                    continue
                if now >= ceil_to_midnight(v.noncurrent_since + n_h):
                    self._free(v, now)
                    freed.append(v.key)
            for key, m in self.markers.items():
                if m.removed_at is None and not self.versions[key].on_disk:
                    m.removed_at = now          # expired object delete marker
        elif mode == "soft_delete":
            d_h = self.spec.soft_delete_days * HOURS_PER_DAY
            for v in self.versions.values():
                if v.is_latest or not v.on_disk:
                    continue
                if now >= v.noncurrent_since + d_h:
                    self._free(v, now)
                    freed.append(v.key)
        elif mode == "object_lock":
            if self.spec.lock_retain_days is None:
                return freed
            r_h = self.spec.lock_retain_days * HOURS_PER_DAY
            for v in self.versions.values():
                if v.is_latest or not v.on_disk:
                    continue
                if now >= v.put_at + r_h and now >= v.noncurrent_since:
                    self._free(v, now)
                    freed.append(v.key)
        return freed

    # ---------------------------------------------------------------- reads
    def live_keys(self, prefix: str = "") -> list[str]:
        return [k for k, v in self.versions.items()
                if v.is_latest and v.on_disk and k.startswith(prefix)]

    def bytes_on_disk(self, prefix: str = "") -> int:
        return sum(v.size for k, v in self.versions.items() if v.on_disk and k.startswith(prefix))

    def list_object_versions(self, Bucket: str = "", Prefix: str = "") -> dict:
        vs, dms = [], []
        for k, v in self.versions.items():
            if not k.startswith(Prefix) or not v.on_disk:
                continue
            vs.append({"Key": k, "VersionId": v.version_id, "Size": v.size,
                       "IsLatest": v.is_latest, "LastModified": v.put_at})
        for k, m in self.markers.items():
            if not k.startswith(Prefix) or m.removed_at is not None:
                continue
            dms.append({"Key": k, "VersionId": m.version_id, "IsLatest": True,
                        "LastModified": m.created_at})
        return {"Versions": vs, "DeleteMarkers": dms}

    def summary(self) -> dict:
        live = sum(1 for v in self.versions.values() if v.is_latest and v.on_disk)
        noncurrent = [v for v in self.versions.values() if not v.is_latest and v.on_disk]
        return {"objects_current": live, "objects_noncurrent_on_disk": len(noncurrent),
                "bytes_noncurrent_on_disk": sum(v.size for v in noncurrent), **self.stats}
