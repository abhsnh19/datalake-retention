# Local S3 + bucket versioning + pyiceberg — working recipe

**Verified 2026-08-24 in this container. 8/8 checks pass.**
Run it yourself: `python3 /home/claude/lhbench/objstore/spike_s3.py`

## TL;DR

`moto` in **server mode** is the winner. Pure Python, no binary download, no Java,
no Maven. pyiceberg 0.11.1 + pyarrow 25.0.1 write to it with no patches.

> ### ⚠️ THE ONE LIMITATION THAT MATTERS
> **`put_bucket_lifecycle_configuration` is ACCEPTED and stored, but NEVER
> ENFORCED.** moto returns HTTP 200, `get_bucket_lifecycle_configuration`
> replays the rule verbatim — and then nothing ever happens. There is no expiry
> engine and no background reaper. Noncurrent versions and delete markers live
> forever until explicitly destroyed.
>
> **Consequence for the research:** you cannot measure storage reclamation by
> setting a lifecycle rule and waiting. Lifecycle expiration must be *simulated*
> by the harness — enumerate versions with `list_object_versions`, decide which
> ones a real rule would have expired, and `delete_object(..., VersionId=...)`
> them yourself. Step D of the spike script is that primitive.

## Install

Both were already present here; these are the commands that provide them.

```bash
pip install --break-system-packages "moto[server]"   # brings flask/werkzeug
pip install --break-system-packages boto3            # already present
# pyiceberg 0.11.1 + pyarrow 25.0.1 already installed
```

Note the `--break-system-packages` flag is mandatory in this container.
`pip download` does **not** accept it.

## Start the server

```bash
moto_server -p 5000 -H 127.0.0.1
```

Endpoint: `http://127.0.0.1:5000`. Any credentials work; the spike uses
`test`/`test` in `us-east-1`. `spike_s3.py` auto-starts the server if port 5000
is closed and reuses it if already open (`--keep` leaves a self-started one up).

## The pyiceberg catalog properties that worked

```python
from pyiceberg.catalog.sql import SqlCatalog

props = {
    "uri": f"sqlite:///{sqlite_path}",          # e.g. /tmp/cat/catalog.db  (4 slashes total for abs path)
    "warehouse": f"s3://{bucket}/wh",
    "s3.endpoint": "http://127.0.0.1:5000",
    "s3.access-key-id": "test",
    "s3.secret-access-key": "test",
    "s3.region": "us-east-1",
    "s3.path-style-access": "true",
}
cat = SqlCatalog("spike", **props)
cat.create_namespace_if_not_exists("bench")
tbl = cat.create_table("bench.events", schema=batch.schema)
tbl.append(batch); tbl.append(batch2); tbl.delete("id = 2")
```

That is the whole configuration. No FileIO override, no fsspec, no custom
`py-io-impl` — pyiceberg's default pyarrow `S3FileSystem` path works as-is.

## Gotchas (each of these cost time; none are obvious)

- **Proxy env vars break everything.** This container sets `HTTPS_PROXY`/
  `HTTP_PROXY`. Both botocore *and* pyarrow's bundled AWS C++ SDK honor them and
  will try to CONNECT-tunnel `127.0.0.1:5000` through the proxy, which answers
  `403`. Pop `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` (and lowercase variants)
  and set `NO_PROXY=*` **before importing boto3/pyarrow**. `spike_s3.py` does
  this at the top of the module; do the same in the harness.
- **Credentials must also be in the environment**, not just in the catalog
  props. pyarrow's `S3FileSystem` reads `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` from the process environment for parts
  of its init. Set them to the same values as the catalog props.
- **`s3.path-style-access: "true"` is required.** moto is reached by IP, so
  virtual-host addressing (`bucket.127.0.0.1`) cannot resolve.
- **`s3.region` must be set** (`us-east-1`). Without a region the AWS SDK
  attempts a region-discovery call that moto does not satisfy cleanly.
- **Bucket names must be ≥3 characters** — moto enforces real S3 naming rules
  and returns `InvalidBucketName` for short names.
- **`NewerNoncurrentVersions` is silently dropped.** Send it in a lifecycle rule
  and `get_bucket_lifecycle_configuration` returns the rule *without* that key.
  Another reason not to trust lifecycle here.
- **Iceberg never overwrites an object**, so a `delete()` alone produces **zero**
  noncurrent versions — it writes new files and simply stops referencing the old
  ones, which remain *current, live* objects. To see old data-file versions you
  must actually delete the orphans (a VACUUM). Step F does this: diff
  `plan_files()` before/after, `delete_object` the dereferenced parquet, then
  `list_object_versions` shows it as a noncurrent version + delete marker, still
  byte-for-byte readable by `VersionId`.
- **pyiceberg 0.11.1 has `tbl.maintenance.expire_snapshots()`** but it only
  rewrites table metadata — it does **not** delete data files from storage. Any
  physical reclamation must be done by the harness against S3 directly.
- MinIO was tested and is **not** obtainable: `dl.min.io` returns
  `CONNECT tunnel failed, response 403`. Do not retry.

## What moto genuinely supports vs. only accepts

### Genuinely implemented (safe to build research on)
- `put_bucket_versioning` / `get_bucket_versioning` — real Enabled/Suspended state
- Versioned `put_object` — every PUT gets a distinct `VersionId`
- `list_object_versions` — returns `Versions[]` and `DeleteMarkers[]` with
  correct `Key`, `VersionId`, `Size`, `IsLatest`, `LastModified`
- **Delete markers**: an unversioned `delete_object` creates a marker, hides the
  key from `list_objects_v2`, and leaves all prior versions intact and readable
- `get_object(VersionId=...)` on a noncurrent version — returns the original bytes
- `delete_object(Key, VersionId=...)` — destroys exactly that version, creates no
  marker, subsequent reads give `NoSuchVersion`
- Per-version `Size`, so **deleted-byte accounting is accurate**
- Multipart upload, `head_object`, prefix listing, standard S3 error codes
- Real bucket-name validation

### Accepted but silently ignored (do NOT build research on)
- `put_bucket_lifecycle_configuration` — **stored and echoed, never enforced.**
  No `Expiration`, no `NoncurrentVersionExpiration`, no
  `AbortIncompleteMultipartUpload` ever fires. Simulate it.
- `NewerNoncurrentVersions` inside a lifecycle rule — dropped on round-trip
- Storage classes / transitions — recorded as a string, no tiering behavior
- Object Lock / retention, replication, inventory, analytics — mock-level at best

### Also worth knowing
- moto is **in-memory**: restarting `moto_server` wipes every bucket. Fine for a
  benchmark run, fatal if you expected persistence across runs.
- Werkzeug dev server, single process, 2 CPUs here — adequate for correctness
  work, useless for throughput numbers. **Do not report I/O timings from moto.**

## Step map of `spike_s3.py`

| Step | What it proves |
|---|---|
| A | moto_server starts / is reachable at `http://127.0.0.1:5000` |
| B | bucket created, versioning `Enabled` and confirmed by GET |
| C | write → overwrite → overwrite → delete; all 3 versions (862 B) + delete marker survive with reportable sizes; oldest still readable by `VersionId` |
| D | `delete_object(VersionId=...)` permanently expires one noncurrent version; 100 B reclaimed; version becomes `NoSuchVersion` |
| E | lifecycle rule **accepted + round-trips** (E1) and separately **proven not enforced** (E2) |
| F | pyiceberg table on `s3://bucket/wh`, 2 appends → 5 rows, `delete("id = 2")` → 4 rows; simulated vacuum shows the dereferenced 918 B parquet surviving as a noncurrent version, recoverable with valid `PAR1` magic |
