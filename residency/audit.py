"""Bucket-posture audit for S3 / GCS / Azure (duck-typed clients).

Pure functions over client-shaped objects, so the tests run with fakes and
the same code runs against boto3 / google-cloud-storage / azure-storage-blob.
Every rule is the documented behaviour quoted in ``stores.py``.
"""
from __future__ import annotations

from .feasibility import check, table_floor_days as _table_floor


def audit_s3(bucket: str, client, prefix: str = "") -> dict:
    findings = []
    versioning = (client.get_bucket_versioning(Bucket=bucket) or {}).get("Status")
    try:
        rules = client.get_bucket_lifecycle_configuration(Bucket=bucket).get("Rules", [])
    except Exception as e:
        if "NoSuchLifecycleConfiguration" in str(e) or "NoSuchLifecycleConfiguration" in type(e).__name__:
            rules = []
        else:
            raise
    lock = None
    try:
        lock = client.get_object_lock_configuration(Bucket=bucket).get("ObjectLockConfiguration")
    except Exception as e:
        if "ObjectLockConfigurationNotFound" not in str(e) and "NotFound" not in type(e).__name__:
            raise
    noncurrent = None
    newer_kept = None
    for r in rules:
        if r.get("Status") != "Enabled":
            continue
        f = r.get("Filter", {}) or {}
        rp = f.get("Prefix", r.get("Prefix", "")) or ""
        if "And" in f:
            rp = f["And"].get("Prefix", "") or ""
        if prefix and rp and not prefix.startswith(rp):
            continue
        nve = r.get("NoncurrentVersionExpiration")
        if not nve:
            continue
        n = nve.get("NoncurrentDays")
        if n is not None:
            noncurrent = n if noncurrent is None else min(noncurrent, n)
        if nve.get("NewerNoncurrentVersions"):
            newer_kept = nve["NewerNoncurrentVersions"]
            findings.append(f"rule {r.get('ID')} keeps {newer_kept} newer noncurrent versions regardless of age "
                            f"-- the age rule alone does not bound residency")
    if versioning != "Enabled":
        expiry, bound = 0.0, "bounded"
        findings.append("versioning is not enabled: a DELETE frees the bytes")
    elif lock and lock.get("ObjectLockEnabled") == "Enabled":
        rule = (lock.get("Rule") or {}).get("DefaultRetention") or {}
        mode = rule.get("Mode")
        days = rule.get("Days") or (rule.get("Years", 0) * 365 if rule.get("Years") else None)
        if mode == "COMPLIANCE":
            expiry, bound = None, "forbidden"
            findings.append(f"Object Lock COMPLIANCE default retention {days} d: no principal, including root, "
                            f"can delete a locked version before it expires")
        else:
            expiry = None if noncurrent is None else float(noncurrent + 1)
            bound = "bounded" if noncurrent is not None else "unbounded"
            findings.append(f"Object Lock enabled ({mode or 'no default retention'}); governance mode can be "
                            f"bypassed with s3:BypassGovernanceRetention")
    elif noncurrent is None:
        expiry, bound = None, "unbounded"
        findings.append("versioned bucket with no NoncurrentVersionExpiration rule: noncurrent versions are never expired")
    else:
        expiry, bound = float(noncurrent + 1), "bounded"
        findings.append(f"NoncurrentVersionExpiration {noncurrent} d (+1 d midnight rounding)")
    return {"provider": "s3", "bucket": bucket, "versioning": versioning, "noncurrent_days": noncurrent,
            "newer_noncurrent_versions": newer_kept, "object_lock": lock, "store_expiry_days": expiry,
            "residency_bound": bound, "findings": findings}


def audit_gcs(bucket_obj) -> dict:
    findings = []
    pol = getattr(bucket_obj, "soft_delete_policy", None)
    secs = getattr(pol, "retention_duration_seconds", None) if pol is not None else None
    soft_days = (secs or 0) / 86400.0
    versioning = bool(getattr(bucket_obj, "versioning_enabled", False))
    expiry, bound = soft_days, "bounded"
    findings.append(f"soft delete retains deleted objects {soft_days:g} d (on by default for new buckets, 7 d)"
                    if soft_days else "soft delete disabled (retention 0)")
    if versioning:
        nc_days = None
        for rule in getattr(bucket_obj, "lifecycle_rules", []) or []:
            action = rule.get("action", {}) if isinstance(rule, dict) else {}
            cond = rule.get("condition", {}) if isinstance(rule, dict) else {}
            if action.get("type") == "Delete" and cond.get("daysSinceNoncurrentTime") is not None:
                d = cond["daysSinceNoncurrentTime"]
                nc_days = d if nc_days is None else min(nc_days, d)
        if nc_days is None:
            expiry, bound = None, "unbounded"
            findings.append("object versioning enabled with no noncurrent-delete lifecycle rule")
        else:
            expiry = soft_days + nc_days
            findings.append(f"noncurrent versions deleted after {nc_days} d, then soft delete {soft_days:g} d")
    return {"provider": "gcs", "bucket": getattr(bucket_obj, "name", ""), "soft_delete_days": soft_days,
            "versioning": versioning, "store_expiry_days": expiry, "residency_bound": bound, "findings": findings}


def audit_azure(service_props, container_props=None) -> dict:
    findings = []
    drp = (service_props or {}).get("delete_retention_policy") or {}
    soft_days = float(drp.get("days") or 0) if drp.get("enabled") else 0.0
    versioning = bool((service_props or {}).get("is_versioning_enabled"))
    expiry, bound = soft_days, "bounded"
    findings.append(f"blob soft delete {soft_days:g} d (portal-created accounts enable it by default; "
                    f"PowerShell/CLI-created accounts do not)" if soft_days else "blob soft delete off")
    if versioning:
        expiry, bound = None, "unbounded"
        findings.append("blob versioning on: previous versions are retained until explicitly deleted or removed "
                        "by a lifecycle policy (not supported on hierarchical namespace)")
    cp = container_props or {}
    if cp.get("has_legal_hold") or cp.get("has_immutability_policy"):
        expiry, bound = None, "forbidden"
        findings.append("container immutability policy or legal hold: blobs cannot be deleted while it is in effect")
    return {"provider": "azure", "soft_delete_days": soft_days, "versioning": versioning,
            "store_expiry_days": expiry, "residency_bound": bound, "findings": findings}


def verdict(posture: dict, window_days: int = 30, table_floor_days: float | None = None) -> dict:
    if table_floor_days is None:
        table_floor_days = _table_floor()
    c = check(window_days, table_floor_days, posture.get("store_expiry_days"))
    return {**posture, "window_days": window_days, "table_floor_days": table_floor_days,
            "feasible": c["feasible"], "headroom_days": c["headroom_days"],
            "verdict": c["reason"] or f"{c['headroom_days']:g} days of headroom for every rewrite, expiry run and retry"}
