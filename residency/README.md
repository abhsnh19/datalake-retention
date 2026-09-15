# residency — the combined harness

One harness for the question both decks ask: **how long do a deleted row's
bytes survive in a lakehouse, and which job decides it?**

It is the union of two harnesses that measured the same thing on real
PyIceberg tables in virtual time and reported it differently:

| | `lhbench` (Abhishek Sinha) | `erasure_harness` (Nilanjan Chatterjee) |
|---|---|---|
| clock | day | hour, with S3's midnight-UTC rounding |
| retention / orphan job | 20 d / every 15 d, no age gate | Iceberg defaults: 5 d / `older_than` 3 d |
| snapshot expiry | rewrites metadata only (PyIceberg native) | deletes what it unreferences (the Java action) |
| rewrite | file-granular, daily byte budget, dirty threshold | subject-level, no budget |
| policies | baseline (cadence) · deadline (EDF + free rides + pull expiry/cleanup forward) | A–F calendars: weekly window, daily, deferred-to-deadline, copy-on-write |
| bucket | plain FS or versioned + D_nc | unversioned / versioned / soft delete / Object Lock, boto3-shaped |
| checks | `verify.py` (ordering, budget, cohort, censoring, claims) | ListObjectVersions + Parquet read-back probe, exact backtest |

Every one of those rows is now a **parameter or a preset**, so one run can
answer both decks and they cannot disagree with each other. The ledger
records every stage instant and `summary` reports both views:

    arrival → commit (t0) → rewritten (t1) → unreferenced → unlinked (t2) → physical (t3)
    lhbench view : median days-to-stage, DRT p50/p95, deadline_met_fraction, amplification
    erasure view : per-stage durations (commit / rewrite / unlink / bucket), miss rate

## Layout

    residency/
      vclock.py       hour clock, midnight rounding
      rng.py          LCG (seed-stable), power-law weights, Poisson
      workload.py     ingest (three layouts, commits/day), bursty erasure demand, cohort
      table.py        real Iceberg: append, copy-on-write delete, file-granular rewrite,
                      expiry by virtual age, manifest-level reference tracking
      stores.py       the bucket: S3 versioned/unversioned, GCS/Azure soft delete,
                      Object Lock, AWS S3 Tables, MinIO count-only ILM — quoted semantics
      ledger.py       obligations, two summary views
      policies.py     one Policy class, ten presets (lhbench-baseline, lhbench-deadline,
                      A-default … F-cow-daily-expiry, combined-coordinated, s3tables-managed)
      runner.py       the hour loop: ingest, nightly commit batch, daily decision, expiry,
                      orphan cleanup, bucket lifecycle, probe
      probe.py        "are the bytes still there?" against any boto3-shaped store
      feasibility.py  floor_days / max_noncurrent_days / plan / lifecycle documents
      formats.py      documented floors (Iceberg, S3 Tables, Delta, Hudi, S3, GCS, Azure)
      costmodel.py    cost at scale from a measured run, assumptions printed, rounded
      verify.py       correctness + claim checks on a sweep
      reconcile.py    the two decks against the two result sets → RECONCILIATION.md
      cli.py          run / cell / verify / reconcile / backtest / demo / plan / cost
    tests/            46 tests, ~12 s, no network
    residency_data/   erasure_harness.json + erasure_model.json (the other deck's numbers)
    results_residency/<suite>/  one JSON per cell + combined.json + summary.csv

## Run

    pip install -r requirements-residency.txt
    python -m pytest -q tests
    python -m residency.cli run --suite smoke                       # 4 cells, ~10 s
    python -m residency.cli run --suite lhbench-main --seeds 20260823   # E1/E2 grid on the current code
    python -m residency.cli run --suite combined --workers 4         # both harnesses' policies × 3 buckets × 3 seeds
    python -m residency.cli run --suite ladder                       # every bucket configuration, three policies
    python -m residency.cli verify --results results_residency/combined
    python -m residency.cli reconcile --out RECONCILIATION.md
    python -m residency.cli cost --results results_residency/combined --policy lhbench-baseline
    python -m residency.cli plan --window 30 --retention 20 --expire-cadence 7 --orphan-cadence 15

`run` ends by running `verify` on what it produced. `backtest --results DIR`
re-runs one seed and compares p50, p95, met fraction, miss rate and commit
counts exactly.

## What is real, emulated, modelled

Real: every snapshot, manifest and data file; copy-on-write deletes; the
file-granular rewrite; snapshot expiry; which files are referenced.
Emulated with documented semantics: the bucket (`stores.py` quotes the
sentences); merge-on-read as a deferred rewrite (PyIceberg writes no deletion
vectors); virtual time. Modelled: Delta and Hudi floors; cost at scale.

Residencies are lower bounds: jobs take zero time and the emulated bucket
frees bytes exactly when the documented rule matures; AWS documents removal
as asynchronous.
