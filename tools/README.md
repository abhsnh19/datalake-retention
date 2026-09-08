# Deletion-scatter analyzer for Iceberg lakehouses

Week-one instrumentation for the paper *"deletion in the lakehouse is a
multi-stage pipeline with unbounded latency."* It answers the question that
decides whether the thesis holds:

> **When you erase one subject's rows, how much of the table has to be rewritten
> — and how long does the data survive before that happens?**

Everything is computed from **Iceberg metadata alone**. The tool reads manifests
and snapshot history. It never opens a Parquet data file and never sees a column
value.

That property is the point. It means you can run this against production
*before* a data-access review completes, which is the difference between having
your opening figure in week one and having it in month three.

---

## Files

| File | Purpose |
|---|---|
| `scatter.py` | The analyzer. Point it at a table, get the four metrics. |
| `make_test_table.py` | Builds local Iceberg tables with known scatter, for validation. |
| `validate.py` | Checks analyzer estimates against exact row-level ground truth. |

```
pip install "pyiceberg[sql-sqlite,pyarrow]" pyarrow
```

---

## Try it locally first

Do this before touching production — it proves the tool works and shows you the
shape of the result you're looking for.

```bash
python3 make_test_table.py --warehouse ./_wh --fresh
python3 validate.py --warehouse "$PWD/_wh"
```

Two tables are built with **identical rows** and different physical layouts:
`events_scattered` (arrival order) and `events_clustered` (sorted by `user_id`).

Validation output:

```
events_scattered  (30 data files, 500 subjects)
  exact files touched      p50 = 30
  estimated files touched  p50 = 30
  UNDER-counts (must be 0) ... 0
  PASS: estimate is a valid upper bound for every subject

events_clustered  (30 data files, 500 subjects)
  exact files touched      p50 = 1
  estimated files touched  p50 = 1
  UNDER-counts (must be 0) ... 0
  PASS
```

And the headline contrast, from the analyzer itself:

| | scattered | clustered |
|---|---|---|
| files touched per subject (p50) | 30 / 30 (100%) | 1 / 30 (3.3%) |
| deletion amplification (p50) | **500×** | **17×** |

Same data, same row count. A 29× swing in erasure cost from layout alone. That
is the mechanism argument in one table — and it is why the layout contribution
belongs in the paper.

---

## Run it against a real table

```bash
# Catalog defined in ~/.pyiceberg.yaml
python3 scatter.py --catalog prod --table analytics.events \
                   --key user_id --n-subjects 40000000 --json events.json

# Explicit REST catalog
python3 scatter.py --catalog prod \
    --catalog-uri https://catalog.internal/api \
    --property type=rest --property warehouse=s3://lake/wh \
    --table analytics.events --key user_id
```

Key flags:

- `--key` — the erasure key column (`user_id`, `subject_id`, `account_id`).
- `--n-subjects` — distinct subjects in the table. Needed for amplification; a
  rough count is fine, it only scales the denominator.
- `--keys FILE` — optional file of real subject keys, one per line, to refine
  the estimate. **These can be hashed or opaque** — the tool only compares them
  against min/max bounds, so you never need to hand it real identifiers.
- `--max-probes` — probe count when no key file is given (default 200).
- `--json` — full results for plotting.

If you use `--catalog-uri` with a SQL catalog, `--catalog` must match the
catalog *name* the tables were registered under, not an arbitrary label.

---

## The four metrics

**1. Deletion scatter.** Data files whose `[min, max]` bounds on the erasure key
could contain a given subject — i.e. files a rewrite planner must assume it has
to touch. Reported as a distribution over subjects, and as a share of the table.

**2. Deletion amplification.** Bytes rewritten per byte of the subject's own
data actually erased. This is the number that makes practitioners wince, and the
one that carries the abstract. Anything in the hundreds means erasure cost is
decoupled from erasure volume, which is the core pathology.

**3. Residency floor.** Age of the oldest retained snapshot. A row deleted today
stays readable via time travel until that snapshot ages out — so this is a
*lower bound* on residency, before compaction, snapshot expiry, orphan cleanup,
or object-store version expiry have even been scheduled. If this reads 90 days,
no amount of compaction tuning gets you to a 30-day compliance window.

**4. Delete-file debt.** Accumulated position/equality delete files and v3
deletion vectors. Every one is a row that is logically gone and physically
present — stage 1 of the residency stack, quantified. The `delete:data` ratio
also proxies read-path overhead in merge-on-read tables.

Plus **layout diagnostics**: whether the erasure key is a partition or sort key,
and how many files span the entire key range (those can never be pruned for
anyone — usually the smoking gun).

---

## Interpreting what you get

The thesis holds if you see **high scatter and a high residency floor together**.
That combination means maintenance cost per erasure is large *and* the deadline
clock started long ago — so erasure demand outruns maintenance capacity and
residency grows without bound. That's the paper.

Some outcomes and what to do:

- **Scatter near 100%, amplification in the hundreds+.** The strong case. This
  is your opening figure. Go.
- **Scatter low because the table is already clustered on the erasure key.**
  Interesting in a different way — check whether that clustering costs query
  pruning elsewhere. That tension *is* the layout contribution.
- **High scatter but a short residency floor.** Weaker headline, but check
  stages 4–6 (orphan cleanup cadence, object-store versioning) before concluding
  — those are usually where the real tail hides, and this tool doesn't see them.
- **Scatter varies wildly across tables.** Best outcome for a measurement paper.
  A characterization across a fleet of tables beats a single deep dive.

Run it across **many** production tables, not one. The distribution across a
real fleet is a stronger result than any single number, and it's the kind of
thing only someone with your access can produce.

---

## Limitations — state these in the paper

- **Upper bounds, not exact.** Min/max overlap proves a file *may* contain a
  key. Only a row read proves it does. For an erasure-cost argument the upper
  bound is the right number (a planner must assume it), but say so explicitly.
  Validation on the test tables shows the over-count is small when bounds are
  informative and zero when clustering is tight.
- **Stages 5–6 are invisible here.** Object-store versioning, lifecycle rules,
  backups, DR replicas, and downstream extracts are outside Iceberg's metadata.
  Measuring those needs bucket inventory reports and is the natural phase two.
- **Bounds must exist.** Files without stats for the key column are counted as
  unprunable; the report warns when this happens. If it's a large fraction, your
  writers aren't collecting stats and that's itself a finding.
- **Iceberg only.** Delta Lake's transaction log carries equivalent per-file
  statistics; a Delta backend is a contained port and worth doing for
  generality.
- Probe-based estimation assumes subjects are roughly uniform over the key
  space. Pass `--keys` with a real (hashed) sample to remove that assumption —
  worth doing once for the paper even if approvals take a while.

---

## Suggested first run

Pick your largest production table and the one you believe is worst-laid-out.
Run both. If the amplification figure comes back in the hundreds, you have the
paper's opening figure and the rest of the plan follows. If it comes back in the
single digits on every table you try, that's a genuine signal to pivot to the
compaction-economics topic instead — and finding that out in week one, cheaply,
is exactly what this tool is for.
