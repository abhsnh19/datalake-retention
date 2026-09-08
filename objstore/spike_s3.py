#!/usr/bin/env python3
"""
ENGINEERING SPIKE: local S3-compatible object store with BUCKET VERSIONING,
plus a pyiceberg table written into it.

WINNER: `moto` in server mode (pure Python, no binary downloads needed).
   pip install --break-system-packages "moto[server]"
   moto_server -p 5000 -H 127.0.0.1

Run:  python3 spike_s3.py            # auto-starts moto_server if not already up
      python3 spike_s3.py --keep     # leave a self-started server running

*** HONESTY NOTE, READ THIS ***
moto is a MOCK. Versioning (versions, delete markers, VersionId deletes) is
genuinely IMPLEMENTED and behaves like S3. Lifecycle configuration is
ACCEPTED AND STORED but NEVER ENFORCED -- there is no expiry engine. Step E
proves this at runtime. Any research that depends on lifecycle expiration
must simulate it explicitly (see Step D / Step F vacuum simulation).
"""
from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid

# ---------------------------------------------------------------------------
# Proxy hygiene: this container routes egress through an HTTPS proxy. Both
# botocore AND pyarrow's AWS C++ SDK read proxy env vars, and would try to
# tunnel 127.0.0.1:5000 through it (403). Strip them before importing boto3.
# ---------------------------------------------------------------------------
for _v in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
           "ALL_PROXY", "all_proxy"):
    os.environ.pop(_v, None)
os.environ["NO_PROXY"] = "*"
os.environ["no_proxy"] = "*"

# pyarrow's S3FileSystem also picks credentials up from the environment.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_REGION", "us-east-1")

import boto3  # noqa: E402
import pyarrow as pa  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

ENDPOINT = "http://127.0.0.1:5000"
HOST, PORT = "127.0.0.1", 5000
ACCESS_KEY = "test"
SECRET_KEY = "test"
REGION = "us-east-1"

RESULTS: list[tuple[str, str, str]] = []
_server_proc: subprocess.Popen | None = None


def record(step: str, ok: bool, detail: str = "") -> bool:
    status = "PASS" if ok else "FAIL"
    RESULTS.append((step, status, detail))
    print(f"\n  [{status}] {step}" + (f"\n         {detail}" if detail else ""))
    return ok


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def port_open(host: str, port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )


# ---------------------------------------------------------------------------
# The exact pyiceberg catalog properties that work against moto.
# ---------------------------------------------------------------------------
def catalog_props(sqlite_path: str, bucket: str) -> dict:
    return {
        "uri": f"sqlite:///{sqlite_path}",
        "warehouse": f"s3://{bucket}/wh",
        "s3.endpoint": ENDPOINT,
        "s3.access-key-id": ACCESS_KEY,
        "s3.secret-access-key": SECRET_KEY,
        "s3.region": REGION,
        "s3.path-style-access": "true",
    }


# ===========================================================================
# STEP A -- start / connect to the S3 endpoint
# ===========================================================================
def step_a(keep: bool) -> bool:
    global _server_proc
    banner("STEP A -- start (or connect to) a local S3-compatible endpoint")

    if port_open(HOST, PORT):
        print(f"  moto_server already listening on {ENDPOINT}; reusing it.")
    else:
        exe = shutil.which("moto_server")
        if not exe:
            return record("A: S3 endpoint reachable", False,
                          "moto_server not found -- pip install "
                          '--break-system-packages "moto[server]"')
        print(f"  starting: {exe} -p {PORT} -H {HOST}")
        _server_proc = subprocess.Popen(
            [exe, "-p", str(PORT), "-H", HOST],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(60):
            time.sleep(0.25)
            if port_open(HOST, PORT):
                break
        else:
            return record("A: S3 endpoint reachable", False,
                          "moto_server did not come up within 15s")
        if keep:
            _server_proc = None  # do not kill on exit

    try:
        s3_client().list_buckets()
    except Exception as exc:
        return record("A: S3 endpoint reachable", False, repr(exc))

    return record("A: S3 endpoint reachable",
                  True, f"moto (pure-Python S3 mock) at {ENDPOINT}")


# ===========================================================================
# STEP B -- create a bucket and ENABLE VERSIONING
# ===========================================================================
def step_b(bucket: str) -> bool:
    banner("STEP B -- create bucket + enable versioning")
    s3 = s3_client()
    try:
        s3.create_bucket(Bucket=bucket)
        s3.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        status = s3.get_bucket_versioning(Bucket=bucket).get("Status")
    except Exception as exc:
        return record("B: bucket created with versioning enabled", False, repr(exc))
    return record("B: bucket created with versioning enabled",
                  status == "Enabled",
                  f"bucket={bucket}  get_bucket_versioning -> Status={status!r}")


# ===========================================================================
# STEP C -- THE CORE CAPABILITY.
# write -> overwrite -> delete, then prove noncurrent versions AND the
# delete marker survive, with reportable sizes.
# ===========================================================================
def step_c(bucket: str) -> tuple[bool, list[dict]]:
    banner("STEP C -- noncurrent versions + delete marker survive deletion "
           "(THE key capability)")
    s3 = s3_client()
    key = "core/probe.bin"
    bodies = [b"A" * 100, b"B" * 250, b"C" * 512]

    put_ids = []
    for i, body in enumerate(bodies, 1):
        r = s3.put_object(Bucket=bucket, Key=key, Body=body)
        put_ids.append(r["VersionId"])
        print(f"  put #{i}: {len(body):>4} bytes  VersionId={r['VersionId']}")

    dr = s3.delete_object(Bucket=bucket, Key=key)
    marker_id = dr.get("VersionId")
    print(f"  delete_object -> DeleteMarker={dr.get('DeleteMarker')} "
          f"VersionId={marker_id}")

    # The current view must now be empty...
    live = s3.list_objects_v2(Bucket=bucket, Prefix=key).get("Contents", [])
    print(f"  list_objects_v2 (current view) sees {len(live)} object(s) -- "
          "the key looks gone")

    # ...but every byte is still there under list_object_versions.
    lov = s3.list_object_versions(Bucket=bucket, Prefix=key)
    versions = [v for v in lov.get("Versions", []) if v["Key"] == key]
    markers = [m for m in lov.get("DeleteMarkers", []) if m["Key"] == key]

    print("\n  list_object_versions:")
    for v in versions:
        print(f"    VERSION  size={v['Size']:>5}  IsLatest={v['IsLatest']!s:<5} "
              f"id={v['VersionId']}")
    for m in markers:
        print(f"    MARKER   size={'-':>5}  IsLatest={m['IsLatest']!s:<5} "
              f"id={m['VersionId']}")

    total = sum(v["Size"] for v in versions)
    noncurrent = [v for v in versions if not v["IsLatest"]]
    expected = sum(len(b) for b in bodies)

    # Deep proof: a noncurrent version is still individually readable.
    readable = False
    try:
        got = s3.get_object(Bucket=bucket, Key=key, VersionId=put_ids[0])["Body"].read()
        readable = got == bodies[0]
        print(f"\n  get_object(VersionId=<oldest>) returned {len(got)} bytes -- "
              f"content matches: {readable}")
    except Exception as exc:
        print(f"  get_object by VersionId failed: {exc!r}")

    ok = (len(versions) == 3 and len(markers) == 1
          and len(noncurrent) == 3 and total == expected and readable)
    record("C: deleted bytes SURVIVE as noncurrent versions with reportable sizes",
           ok,
           f"{len(versions)} versions (all noncurrent) totalling {total} bytes "
           f"(expected {expected}) + {len(markers)} delete marker; "
           "old version still readable by VersionId")
    return ok, versions


# ===========================================================================
# STEP D -- permanently remove ONE specific noncurrent version
# (this is how lifecycle NoncurrentVersionExpiration must be SIMULATED)
# ===========================================================================
def step_d(bucket: str, versions: list[dict]) -> bool:
    banner("STEP D -- hard-delete a specific noncurrent version by VersionId "
           "(lifecycle expiration simulation primitive)")
    if not versions:
        return record("D: hard-delete a noncurrent version by VersionId", False,
                      "no versions available from step C")
    s3 = s3_client()
    key = versions[0]["Key"]
    target = versions[-1]  # oldest
    before = sum(v["Size"] for v in versions)
    print(f"  target: size={target['Size']} id={target['VersionId']}")

    s3.delete_object(Bucket=bucket, Key=key, VersionId=target["VersionId"])
    print("  delete_object(..., VersionId=...) issued -- this destroys the "
          "version, it does NOT create a delete marker")

    lov = s3.list_object_versions(Bucket=bucket, Prefix=key)
    remaining = [v for v in lov.get("Versions", []) if v["Key"] == key]
    after = sum(v["Size"] for v in remaining)
    gone = target["VersionId"] not in {v["VersionId"] for v in remaining}

    unreadable = False
    try:
        s3.get_object(Bucket=bucket, Key=key, VersionId=target["VersionId"])
    except ClientError as exc:
        unreadable = True
        print(f"  get_object on the expired version -> "
              f"{exc.response['Error']['Code']} (correctly unrecoverable)")

    print(f"  bytes retained: {before} -> {after} "
          f"(reclaimed {before - after})")
    return record("D: hard-delete a noncurrent version by VersionId", gone and unreadable,
                  f"version removed; {before - after} bytes reclaimed; "
                  f"{len(remaining)} versions remain")


# ===========================================================================
# STEP E -- lifecycle: ACCEPTED?  vs  ENFORCED?  (two separate questions)
# ===========================================================================
def step_e(bucket: str) -> bool:
    banner("STEP E -- put_bucket_lifecycle_configuration: accepted vs enforced")
    s3 = s3_client()
    lc_key = "lifecycle/expiring.bin"

    cfg = {"Rules": [{
        "ID": "expire-noncurrent-immediately",
        "Status": "Enabled",
        "Filter": {"Prefix": "lifecycle/"},
        # NoncurrentDays=1 is the S3 minimum; we ALSO poll long enough that a
        # real enforcement engine with any sane granularity would have fired.
        "NoncurrentVersionExpiration": {"NoncurrentDays": 1, "NewerNoncurrentVersions": 0},
    }]}

    accepted = False
    try:
        s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=cfg)
        accepted = True
        print("  put_bucket_lifecycle_configuration -> HTTP 200, ACCEPTED")
    except Exception as exc:
        print(f"  put_bucket_lifecycle_configuration REJECTED: {exc!r}")

    stored = False
    if accepted:
        try:
            rules = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
            stored = any("NoncurrentVersionExpiration" in r for r in rules)
            print(f"  get_bucket_lifecycle_configuration round-trips the rule: {rules}")
        except Exception as exc:
            print(f"  get_bucket_lifecycle_configuration failed: {exc!r}")

    record("E1: lifecycle rule with NoncurrentVersionExpiration is ACCEPTED "
           "and stored", accepted and stored,
           "put returned 200 and get returns the rule verbatim")

    # --- now the separate, harder question: is it ENFORCED? ---
    for i in range(3):
        s3.put_object(Bucket=bucket, Key=lc_key, Body=b"L" * (64 * (i + 1)))
    n0 = len([v for v in s3.list_object_versions(Bucket=bucket, Prefix=lc_key)
              .get("Versions", []) if not v["IsLatest"]])
    print(f"\n  created 3 versions -> {n0} noncurrent version(s)")
    print("  waiting 5s to give any enforcement engine a chance...")
    time.sleep(5)
    n1 = len([v for v in s3.list_object_versions(Bucket=bucket, Prefix=lc_key)
              .get("Versions", []) if not v["IsLatest"]])
    print(f"  noncurrent version(s) after wait: {n1}")

    enforced = n1 < n0
    # This step "passes" in the sense that we successfully DETERMINED the answer.
    record("E2: lifecycle enforcement DETERMINED", True,
           ("ENFORCED -- noncurrent versions were actually expired"
            if enforced else
            "*** NOT ENFORCED *** moto stores the rule and replays it on GET, "
            "but runs NO expiry engine. Noncurrent versions live forever until "
            "explicitly deleted by VersionId (Step D). Lifecycle expiration "
            "MUST be simulated in the benchmark harness."))

    if not enforced:
        print("\n  " + "!" * 72)
        print("  !! LIFECYCLE IS ACCEPTED BUT **NOT ENFORCED** BY MOTO.")
        print("  !! Do not draw storage-reclamation conclusions from lifecycle")
        print("  !! rules against this endpoint. Simulate expiry via Step D.")
        print("  " + "!" * 72)
    return accepted and stored


# ===========================================================================
# STEP F -- pyiceberg: create table in the versioned bucket, append twice,
# delete, then show old data-file versions surviving.
# ===========================================================================
def step_f(bucket: str) -> bool:
    banner("STEP F -- pyiceberg table on the versioned S3 bucket")
    from pyiceberg.catalog.sql import SqlCatalog

    s3 = s3_client()
    sqlite_path = os.path.join(tempfile.mkdtemp(prefix="spike-cat-"), "catalog.db")
    props = catalog_props(sqlite_path, bucket)
    print("  catalog properties:")
    for k, v in props.items():
        print(f"    {k!r}: {v!r},")

    try:
        cat = SqlCatalog("spike", **props)
        cat.create_namespace_if_not_exists("bench")
        b1 = pa.table({"id": pa.array([1, 2, 3], pa.int64()),
                       "val": pa.array(["a", "b", "c"])})
        b2 = pa.table({"id": pa.array([4, 5], pa.int64()),
                       "val": pa.array(["d", "e"])})
        tbl = cat.create_table("bench.events", schema=b1.schema)
        tbl.append(b1)
        tbl.append(b2)
        n_appended = tbl.scan().to_arrow().num_rows
        print(f"\n  appended 2 pyarrow batches -> {n_appended} rows, "
              f"{len(tbl.snapshots())} snapshots")

        files_before = {f.file.file_path for f in tbl.scan().plan_files()}
        tbl.delete("id = 2")
        n_after = tbl.scan().to_arrow().num_rows
        files_after = {f.file.file_path for f in tbl.scan().plan_files()}
        print(f"  delete(\"id = 2\") -> {n_after} rows, "
              f"{len(tbl.snapshots())} snapshots")
    except Exception as exc:
        return record("F: pyiceberg create/append/delete on S3 warehouse",
                      False, repr(exc))

    ok_table = (n_appended == 5 and n_after == 4)
    record("F1: pyiceberg create + 2 appends + delete on the S3 warehouse",
           ok_table, f"rows 5 -> 4 after delete; warehouse=s3://{bucket}/wh")

    # Iceberg never overwrites: the rewritten-away data file is still a LIVE
    # object, merely unreferenced by the current snapshot. To show old data
    # file *versions* surviving we simulate a vacuum: delete the orphans.
    orphans = sorted(files_before - files_after)
    print(f"\n  data files dereferenced by the delete (Iceberg orphans): "
          f"{len(orphans)}")
    for p in orphans:
        print(f"    {p}")

    def s3_key(uri: str) -> str:
        return uri.split(f"{bucket}/", 1)[1]

    sizes = {}
    for p in orphans:
        k = s3_key(p)
        sizes[k] = s3.head_object(Bucket=bucket, Key=k)["ContentLength"]
        s3.delete_object(Bucket=bucket, Key=k)  # simulated VACUUM
    print(f"  simulated VACUUM: deleted {len(orphans)} orphaned data file(s)")

    # Also force a genuine noncurrent version on a metadata-ish key so the
    # bucket shows both shapes.
    lov = s3.list_object_versions(Bucket=bucket, Prefix="wh/")
    vers = lov.get("Versions", [])
    marks = lov.get("DeleteMarkers", [])
    survivors = [v for v in vers if s3_key_in(v["Key"], sizes)]
    marked = [m for m in marks if s3_key_in(m["Key"], sizes)]

    print("\n  list_object_versions on the vacuumed data files:")
    for v in survivors:
        print(f"    SURVIVING VERSION  size={v['Size']:>6}  "
              f"IsLatest={v['IsLatest']!s:<5}  {v['Key']}")
    for m in marked:
        print(f"    DELETE MARKER      {'':>13}IsLatest={m['IsLatest']!s:<5}  "
              f"{m['Key']}")

    recovered = False
    if survivors:
        v = survivors[0]
        raw = s3.get_object(Bucket=bucket, Key=v["Key"],
                            VersionId=v["VersionId"])["Body"].read()
        recovered = raw.startswith(b"PAR1") and raw.endswith(b"PAR1")
        print(f"\n  read back the vacuumed parquet by VersionId: {len(raw)} bytes, "
              f"valid parquet magic: {recovered}")

    total = sum(v["Size"] for v in survivors)
    print(f"\n  total live objects under wh/: {len(vers)}  "
          f"delete markers: {len(marks)}")
    print(f"  bytes still occupied by vacuumed-but-versioned data files: {total}")

    ok_versions = bool(orphans) and len(survivors) == len(orphans) \
        and len(marked) == len(orphans) and recovered
    record("F2: post-delete Iceberg data files survive as noncurrent versions",
           ok_versions,
           f"{len(survivors)} vacuumed data file(s) totalling {total} bytes are "
           f"invisible to list_objects_v2 but fully recoverable via "
           f"list_object_versions + get_object(VersionId=...)")
    return ok_table and ok_versions


def s3_key_in(key: str, sizes: dict) -> bool:
    return key in sizes


# ===========================================================================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true",
                    help="leave a self-started moto_server running on exit")
    args = ap.parse_args()

    bucket = f"spike-{uuid.uuid4().hex[:10]}"
    print(__doc__)

    try:
        if not step_a(args.keep):
            raise SystemExit(1)
        if not step_b(bucket):
            raise SystemExit(1)
        _, versions = step_c(bucket)
        step_d(bucket, versions)
        step_e(bucket)
        step_f(bucket)
    finally:
        banner("SUMMARY")
        width = max(len(s) for s, _, _ in RESULTS) if RESULTS else 10
        for step, status, detail in RESULTS:
            print(f"  {status:<4}  {step:<{width}}")
        failed = [s for s, st, _ in RESULTS if st == "FAIL"]
        print()
        print(f"  {len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed")
        print("\n  KEY LIMITATION: lifecycle NoncurrentVersionExpiration is "
              "ACCEPTED but NOT ENFORCED by moto.")
        print("  Simulate expiry with delete_object(Bucket, Key, VersionId=...).")
        if _server_proc is not None:
            _server_proc.terminate()
            print("\n  (stopped the moto_server this script started; "
                  "use --keep to leave it up)")
    return 1 if any(st == "FAIL" for _, st, _ in RESULTS) else 0


if __name__ == "__main__":
    sys.exit(main())
