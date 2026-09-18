from types import SimpleNamespace

from residency.audit import audit_azure, audit_gcs, audit_s3, verdict


class NoSuchLifecycleConfiguration(Exception):
    pass


class ObjectLockConfigurationNotFoundError(Exception):
    pass


class FakeS3:
    def __init__(self, versioning=None, rules=None, lock=None):
        self._v, self._r, self._l = versioning, rules, lock

    def get_bucket_versioning(self, Bucket):
        return {"Status": self._v} if self._v else {}

    def get_bucket_lifecycle_configuration(self, Bucket):
        if self._r is None:
            raise NoSuchLifecycleConfiguration("The lifecycle configuration does not exist.")
        return {"Rules": self._r}

    def get_object_lock_configuration(self, Bucket):
        if self._l is None:
            raise ObjectLockConfigurationNotFoundError("not found")
        return {"ObjectLockConfiguration": self._l}


def test_s3_postures():
    assert verdict(audit_s3("b", FakeS3()), 30, 8)["headroom_days"] == 22
    s3 = FakeS3("Enabled", [{"ID": "r", "Status": "Enabled", "Filter": {"Prefix": ""},
                             "NoncurrentVersionExpiration": {"NoncurrentDays": 7}}])
    v = verdict(audit_s3("b", s3, "warehouse/"), 30, 8)
    assert v["store_expiry_days"] == 8 and v["feasible"]
    assert verdict(audit_s3("b", FakeS3("Enabled")), 30, 8)["residency_bound"] == "unbounded"
    off = FakeS3("Enabled", [{"ID": "r", "Status": "Disabled", "NoncurrentVersionExpiration": {"NoncurrentDays": 7}}])
    assert audit_s3("b", off)["residency_bound"] == "unbounded"
    newer = FakeS3("Enabled", [{"ID": "r", "Status": "Enabled", "Filter": {},
                                "NoncurrentVersionExpiration": {"NoncurrentDays": 7, "NewerNoncurrentVersions": 5}}])
    out = audit_s3("b", newer)
    assert out["newer_noncurrent_versions"] == 5 and any("regardless of age" in f for f in out["findings"])
    lock = FakeS3("Enabled", [], {"ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 365}}})
    assert verdict(audit_s3("b", lock), 30, 8)["residency_bound"] == "forbidden"
    other_prefix = FakeS3("Enabled", [{"ID": "r", "Status": "Enabled", "Filter": {"Prefix": "logs/"},
                                       "NoncurrentVersionExpiration": {"NoncurrentDays": 7}}])
    assert audit_s3("b", other_prefix, "warehouse/")["residency_bound"] == "unbounded"


def test_gcs_and_azure():
    b = SimpleNamespace(name="g", soft_delete_policy=SimpleNamespace(retention_duration_seconds=7 * 86400),
                        versioning_enabled=False, lifecycle_rules=[])
    assert verdict(audit_gcs(b), 30, 8)["store_expiry_days"] == 7
    bv = SimpleNamespace(name="g", soft_delete_policy=SimpleNamespace(retention_duration_seconds=0),
                         versioning_enabled=True, lifecycle_rules=[])
    assert audit_gcs(bv)["residency_bound"] == "unbounded"
    bvr = SimpleNamespace(name="g", soft_delete_policy=SimpleNamespace(retention_duration_seconds=7 * 86400), versioning_enabled=True,
                          lifecycle_rules=[{"action": {"type": "Delete"}, "condition": {"daysSinceNoncurrentTime": 3}}])
    assert audit_gcs(bvr)["store_expiry_days"] == 10
    props = {"delete_retention_policy": {"enabled": True, "days": 7}, "is_versioning_enabled": False}
    assert verdict(audit_azure(props), 30, 8)["store_expiry_days"] == 7
    assert audit_azure(props, {"has_immutability_policy": True})["residency_bound"] == "forbidden"
    assert audit_azure({**props, "is_versioning_enabled": True})["residency_bound"] == "unbounded"
