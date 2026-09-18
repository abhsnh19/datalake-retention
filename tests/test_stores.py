"""The emulated bucket must do exactly what the documentation says."""
import os

import pytest

from residency.stores import PRESETS, ObjectStore, StoreSpec, preset
from residency.vclock import at


def _bucket(tmp_path, spec):
    root = tmp_path / "wh"
    root.mkdir(parents=True)
    (root / "data").mkdir()
    return ObjectStore(str(root), spec), root


def _put(root, name, size=100):
    p = root / "data" / name
    p.write_bytes(b"x" * size)
    return str(p)


def test_spec_validation_and_presets():
    with pytest.raises(ValueError):
        StoreSpec("glacier")
    with pytest.raises(ValueError):
        StoreSpec("versioned", noncurrent_days=-1)
    with pytest.raises(ValueError):
        StoreSpec("soft_delete", soft_delete_days=3, soft_delete_provider="gcs")      # 7..90 or 0
    with pytest.raises(ValueError):
        StoreSpec("soft_delete", soft_delete_days=400, soft_delete_provider="azure")  # 1..365
    with pytest.raises(ValueError):
        StoreSpec("soft_delete", soft_delete_provider="ibm")
    with pytest.raises(ValueError):
        StoreSpec("object_lock", lock_retain_days=0)
    assert StoreSpec("soft_delete", soft_delete_days=0).bounded
    assert StoreSpec("s3tables").noncurrent_days == 10 and StoreSpec("s3tables").tail_days == 10
    assert StoreSpec("versioned").bounded is False and StoreSpec("versioned").tail_days == 0
    assert StoreSpec("versioned", noncurrent_days=0).label.startswith("MinIO")
    assert StoreSpec("versioned", noncurrent_days=7).rounding_days == 1
    assert StoreSpec("soft_delete").rounding_days == 0
    for name in PRESETS:
        s = preset(name)
        assert StoreSpec.from_dict(s.as_dict()) == s
    with pytest.raises(KeyError):
        preset("nope")


def test_unversioned_delete_frees_bytes_immediately(tmp_path):
    store, root = _bucket(tmp_path, StoreSpec("unversioned"))
    p = _put(root, "a.parquet")
    store.sync(at(1, 3))
    key = store.key_of(p)
    assert store.delete(key, at(2, 4)) == "freed"
    assert not os.path.exists(p)
    assert store.delete(key, at(2, 5)) == "gone" and store.stats["deletes"] == 1
    with pytest.raises(KeyError):
        store.delete("data/missing.parquet", 0)


def test_versioned_delete_inserts_marker_and_rounds_to_midnight(tmp_path):
    store, root = _bucket(tmp_path, StoreSpec("versioned", noncurrent_days=3))
    p = _put(root, "a.parquet")
    store.sync(at(0, 1))
    key = store.key_of(p)
    assert store.delete(key, at(15, 10)) == "delete-marker"
    assert os.path.exists(p)
    resp = store.list_object_versions(Prefix=key)
    assert len(resp["DeleteMarkers"]) == 1 and resp["Versions"][0]["IsLatest"] is False
    for h in range(at(15, 10), at(19, 0)):
        assert store.run_lifecycle(h) == [] and os.path.exists(p)
    assert store.run_lifecycle(at(19, 0)) == [key]
    assert not os.path.exists(p)
    assert store.list_object_versions(Prefix=key) == {"Versions": [], "DeleteMarkers": []}   # expired object delete marker gone


def test_exact_midnight_is_a_ceiling_and_n_zero_frees_next_batch(tmp_path):
    store, root = _bucket(tmp_path, StoreSpec("versioned", noncurrent_days=2))
    p = _put(root, "a.parquet")
    store.sync(0)
    key = store.key_of(p)
    store.delete(key, at(10, 0))
    assert store.run_lifecycle(at(11, 0)) == [] and store.run_lifecycle(at(12, 0)) == [key]
    store2, root2 = _bucket(tmp_path / "b", StoreSpec("versioned", noncurrent_days=0))
    q = _put(root2, "b.parquet")
    store2.sync(0)
    k2 = store2.key_of(q)
    store2.delete(k2, at(3, 5))
    assert store2.run_lifecycle(at(3, 6)) == []              # only the midnight batch acts
    assert store2.run_lifecycle(at(4, 0)) == [k2]


def test_versioned_without_rule_never_frees(tmp_path):
    store, root = _bucket(tmp_path, StoreSpec("versioned", noncurrent_days=None))
    p = _put(root, "a.parquet")
    store.sync(0)
    store.delete(store.key_of(p), at(1, 0))
    for d in range(0, 400, 7):
        assert store.run_lifecycle(at(d, 0)) == []
    assert os.path.exists(p)


def test_soft_delete_is_a_duration_for_gcs_and_azure(tmp_path):
    for provider, days in (("gcs", 7), ("azure", 1)):
        store, root = _bucket(tmp_path / provider, StoreSpec("soft_delete", soft_delete_days=days, soft_delete_provider=provider))
        p = _put(root, "a.parquet")
        store.sync(0)
        key = store.key_of(p)
        assert store.delete(key, at(3, 11)) == "soft-deleted"
        assert store.run_lifecycle(at(3 + days, 10)) == []
        assert store.run_lifecycle(at(3 + days, 11)) == [key]
        assert not os.path.exists(p)


def test_soft_delete_zero_behaves_like_a_plain_delete(tmp_path):
    store, root = _bucket(tmp_path, StoreSpec("soft_delete", soft_delete_days=0))
    p = _put(root, "a.parquet")
    store.sync(0)
    assert store.delete(store.key_of(p), at(1, 1)) == "freed" and not os.path.exists(p)


def test_s3tables_marks_noncurrent_then_frees_after_noncurrent_days(tmp_path):
    store, root = _bucket(tmp_path, StoreSpec("s3tables"))
    p = _put(root, "a.parquet")
    store.sync(0)
    key = store.key_of(p)
    assert store.delete(key, at(5, 3)) == "delete-marker"
    assert store.run_lifecycle(at(15, 0)) == []
    assert store.run_lifecycle(at(16, 0)) == [key]         # 5 d 03:00 + 10 d -> 15 d 03:00 -> 16 d 00:00


def test_object_lock_compliance(tmp_path):
    store, root = _bucket(tmp_path, StoreSpec("object_lock"))
    p = _put(root, "a.parquet")
    store.sync(0)
    store.delete(store.key_of(p), at(1, 0))
    for d in range(0, 400, 5):
        assert store.run_lifecycle(at(d, 0)) == []
    assert os.path.exists(p)
    store2, root2 = _bucket(tmp_path / "r", StoreSpec("object_lock", lock_retain_days=10))
    q = _put(root2, "b.parquet")
    store2.sync(at(0, 0))
    k = store2.key_of(q)
    store2.delete(k, at(2, 0))
    assert store2.run_lifecycle(at(9, 23)) == [] and store2.run_lifecycle(at(10, 0)) == [k]


def test_sync_detects_files_removed_behind_its_back_and_rejects_outside_paths(tmp_path):
    store, root = _bucket(tmp_path, StoreSpec("versioned", noncurrent_days=7))
    p = _put(root, "a.parquet")
    store.sync(0)
    os.remove(p)
    with pytest.raises(RuntimeError):
        store.sync(1)
    with pytest.raises(ValueError):
        store.key_of("/definitely/elsewhere.parquet")


def test_summary_and_bytes_accounting(tmp_path):
    store, root = _bucket(tmp_path, StoreSpec("versioned", noncurrent_days=1))
    a = _put(root, "a.parquet", 100)
    b = _put(root, "b.parquet", 50)
    store.sync(0)
    store.delete(store.key_of(a), at(0, 5))
    s = store.summary()
    assert s["objects_current"] == 1 and s["objects_noncurrent_on_disk"] == 1 and s["bytes_noncurrent_on_disk"] == 100
    assert store.bytes_on_disk("data/") == 150
    store.run_lifecycle(at(2, 0))
    assert store.bytes_on_disk("data/") == 50 and store.stats["bytes_freed"] == 100
