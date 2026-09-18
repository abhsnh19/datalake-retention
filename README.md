# datalake-retention

A measurement harness for **how long deleted data actually survives in a
lakehouse**, and an experimental maintenance scheduler that bounds it.

When you delete a row from an Apache Iceberg table, the table reports it gone
immediately. The bytes do not go anywhere. Physical erasure requires four more
stages, each driven by a separate job on its own cadence:

| stage | what still holds the bytes |
|---|---|
| logical delete | nothing — a delete file or deletion vector masks the rows |
| compaction | the data file, rewritten without the rows |
| snapshot expiry | older snapshots still reference the old file (time travel) |
| orphan cleanup | the unreferenced object, until `remove_orphan_files` runs |
| object-store expiry | the noncurrent version, under bucket versioning |

This repository measures the total, decomposes it by stage, and tests a
scheduler that coordinates all four against a deadline.

## What is real and what is simulated

This matters more than any number here, so it is stated up front.

**Real.** Every run drives an actual Iceberg table — real manifests, real
snapshots, real Parquet files on disk. Persistence is **observed**, by checking
whether the bytes are still present in storage, not derived from a model. The
Puffin deletion-vector writer in `dvgarbage/` is spec-compliant and validated
against pyiceberg's independent reader (exact positional round-trip, including
the 64-bit key path), so its byte accounting is measured rather than assumed.

**Simulated.** Time is virtual: one iteration is one simulated day, and snapshot
ages are tracked in virtual days, which is what makes a 160-day horizon run in
minutes. The workload is synthetic (power-law subject distribution, bursty
erasure arrivals) rather than a production trace.

**Modelled, with known limits.**
- *Merge-on-read* is modelled as a deferred rewrite. pyiceberg performs
  copy-on-write deletes only. For **persistence** this is faithful — MoR is
  exactly "the rewrite happens at compaction time instead of delete time" — but
  it measures no read-path cost from delete-file accumulation.
- *Object-store lifecycle expiry* is simulated by the harness. The spike in
  `objstore/` establishes the real S3 semantics against a live endpoint
  (versioning, delete markers, per-version sizes, version-specific deletes), but
  that endpoint accepts `NoncurrentVersionExpiration` rules and never enforces
  them, so the expiry clock is driven by the harness. `objstore/spike_s3.py`
  demonstrates this at runtime rather than asserting it.
- *DV container garbage* uses real Puffin files and real roaring bitmaps; the
  table lifecycle around them is modelled.

**Not covered at all.** Backups, DR replicas, cross-region copies and downstream
extracts. Object Lock and legal hold. Read-path overhead. A second table format.

## Findings

All numbers below come from the JSON in `results/` and are re-derivable with the
scripts in this repo. Configuration unless noted: 20-day snapshot retention,
15-day orphan-cleanup interval, 30-day compliance window, merge-on-read.

**1. Persistence is a property of maintenance scheduling, not of the table
format.** Median persistence is 33–34 days against a 30-day window. It is
essentially unchanged across copy-on-write vs merge-on-read and across three
data layouts (arrival order, sorted-on-ingest, bucketed on the erasure key).
It moves when retention or cleanup cadence moves, and barely otherwise.

**2. Sorting on ingest does nothing.** The `clustered` layout is a deliberate
negative control: sorting each day's batch by subject cannot reduce cross-file
scatter, because a subject still appears in every day's file. It is
indistinguishable from arrival order throughout.

**3. Bucketing helps, but only while a batch touches few buckets.** At 2 erasure
requests/day, amplification falls from 2.4x to 0.9x as bucket count goes
16 → 256. At 10 requests/day it flattens near 1.4x — batches span every bucket
and the pruning evaporates. Partitioning on the erasure key is not a standalone
fix.

**4. Coordinating the four stages against the deadline works, and is budget-
efficient.** Under the same daily rewrite-byte cap, the deadline-aware policy
sustains full compliance down to a **25 KB/day** budget, where the cadence-driven
baseline manages 3.2%. Below that it degrades gracefully rather than collapsing.
Across five seeds the deadline policy shows **zero variance** (median 20 days,
100% compliance, every seed); the baseline averages 42% ± 10% compliance.

**5. It cannot beat a floor of `retention + D_nc`.** Enabling bucket versioning
adds the noncurrent-version expiration period **exactly and additively** —
verified to the day across 10 configurations and both policies. At the 7-day
default that several cloud providers converge on, baseline compliance falls to
zero while the scheduler still meets the window; at 30 days neither does. This
gives a condition an operator can check directly:

> **retention + D_nc ≤ compliance window**

**6. Dead deletion-vector blobs accumulate in shared Puffin containers.** With no
reclaimer — which is the status quo, since Iceberg ships no
`rewrite_deletion_vectors` — DV storage runs 4–6x the live DV bytes and grows
without bound. Because reclamation is whole-file, **even an oracle that deletes a
Puffin file the instant every blob in it is dead still leaves 48–75% garbage** at
a packing factor of 4 or more. Only one-DV-per-file is fully reclaimable, at the
cost of 5.5x more Puffin files.

## Layout

```
lhbench/               the simulator
  config.py            every parameter, with its justification
  workload.py          power-law subjects, bursty erasure demand, three layouts
  sim.py               virtual-clock driver over a real Iceberg table
  ledger.py            per-obligation stage timestamps
  policies.py          baseline vs deadline-aware maintenance
  warehouse.py         subject index, orphan removal, file-granular rewrite
run_experiments.py     experiment grids (main / sensitivity / objstore / validation)
budget_sweep.py        the budget stress test
make_figures.py        the figures
verify.py              correctness and fairness checks — run this before believing anything
analyse_validation.py  budget, seed-variance and censoring analysis
tools/                 metadata-only analyzer for REAL tables (see tools/README.md)
dvgarbage/             spec-compliant Puffin writer + the DV garbage experiment
objstore/              S3 versioning spike (what is real vs mocked)
results/               one JSON per run: config, summary, per-day timeline, per-obligation stages
results_budget/        the budget sweep
figures/               generated figures
```

## Reproducing

```bash
pip install -r requirements.txt

python3 run_experiments.py --suite main --jobs 2     # core comparisons
python3 run_experiments.py --suite all  --jobs 2     # + sensitivity, object store, validation
python3 budget_sweep.py
python3 verify.py
python3 make_figures.py
```

Set `--jobs` to your core count, not higher — the runs are CPU- and IO-bound and
oversubscription makes everything slower. Copy-on-write configurations cost
several times a merge-on-read one, so the runner sorts cheapest-first and partial
results are usable immediately.

Each run writes `results/<tag>.json` with the full config, summary, per-day
timeline and per-obligation stage timestamps — enough to re-plot or re-analyse
anything without re-running.

## Reading the results honestly

`verify.py` exists because several intermediate numbers in this project were
wrong in ways that looked plausible. It checks stage ordering, budget-cap
compliance, cohort sizes, censoring rates, and each headline claim
per-configuration rather than by eye. Three specific traps it guards:

- **Censoring.** Obligations issued near the end of a run cannot complete, so
  percentiles computed over them are optimistic. Headline numbers use a cohort
  of obligations issued in `[warmup_days, horizon_days − tail_days]`. Runs with
  high censoring are flagged and should not be quoted — at 30-day noncurrent
  expiration a 100-day horizon censored 69% of the cohort and understated
  persistence by six days.
- **Budget accounting.** The daily rewrite cap is a **cap, not equal spend**. The
  deadline policy legitimately uses more of its allowance because it works most
  days while the baseline works on a 7-day cadence. Utilisation is reported
  alongside, and `verify.py` fails any run that exceeds its cap.
- **Baseline strength.** The baseline's delete-ratio gate (`dirty_threshold`)
  defaults to 0.0, which makes it as strong as possible. Real systems ship a
  non-zero threshold, which makes the baseline markedly worse (at 0.10 its
  compliance drops to 2.1%). Headline comparisons use the strongest baseline.

If you are sweeping a parameter, put it in the output tag. Several sweeps in this
project silently overwrote themselves before that was fixed, leaving one
surviving file that looked like a legitimate result.

## Status

Research code, not a product. Iceberg only. Single engine. Synthetic workload.
The scheduler is an experiment in what a deadline-aware maintenance policy could
do, not a drop-in replacement for anyone's maintenance jobs.

## `residency/` — the combined harness (branch `nilanjan_b`)

`lhbench/` above and Nilanjan Chatterjee's `erasure_harness` (the Monster Scale
2027 deck) measured the same thing on real PyIceberg tables and reported it
differently: day clock vs hour clock, 20-day retention vs Iceberg's 5-day
default, metadata-only expiry vs the Java action that deletes files, a byte
budget vs none. `residency/` is the union: every one of those differences is a
parameter or a preset, one ledger reports both decks' views, `verify` runs both
harnesses' checks, and `reconcile` writes [`RECONCILIATION.md`](RECONCILIATION.md)
from this repository's `results/` and the other deck's numbers in
`residency_data/`. See [`residency/README.md`](residency/README.md).

    pip install -r requirements-residency.txt
    python -m pytest -q tests
    python -m residency.cli run --suite combined
