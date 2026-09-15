# Reconciliation: lhbench (Abhishek) vs erasure harness (Nilanjan)

Both harnesses drive a real PyIceberg table in virtual time with a synthetic workload. They agree on the physics; they measured different operating points and reported different decompositions. Verdicts: **agree** / **operating** (same physics, different operating point) / **stale** / **conflict**.

## Headline residency, cadence-driven maintenance, against a 30-day window

- **Verdict:** operating
- **lhbench:** 34 d median (E1, retention 20 d, orphan job 15 d, weekly compaction/expiry, dirty threshold 0.05, 100-day horizon); 32 d for the same configuration on a 160-day horizon (O-store); seeds at threshold 0.0: [34, 33, 31, 31, 29]
- **erasure harness:** 30.6 d median (A-default, retention 5 d, weekly window + monthly full, NoncurrentDays 7, 5 seeds: 28.1-33.8); 22.0 d on an unversioned bucket
- **Resolution:** Same physics. lhbench's days sit in snapshot retention (20 d); the erasure harness's in the compaction calendar (5 d retention, lazy weekly window). Put the operating point on the slide; the combined harness runs both as presets (lhbench-baseline, A-default).

## Where the days go (stage decomposition)

- **Verdict:** operating
- **lhbench:** rewrite 4 / expiry 21 / orphan 9 = 34 d (E1 medians to stage); 3 / 21 / 8 = 32 d (O-store)
- **erasure harness:** commit 0.5 / rewrite 15.8 / unlink 7.2 / bucket 7.8 d (A-default stage means)
- **Resolution:** Two decompositions of the same chain: lhbench splits expiry from orphan cleanup because its expiry rewrites metadata only (PyIceberg native) and the orphan job reclaims bytes; the erasure harness applies the Java action (expiry deletes what it unreferences), so orphan cleanup removed 0 data files there. The combined ledger records both instants; `expiry_deletes_files` is a policy flag.

## Compaction's share of the wait

- **Verdict:** operating
- **lhbench:** 12% (4 of 34 d): 'compaction is 12%, not 34%'
- **erasure harness:** 51% under the lazy calendar; 0% once the rewrite runs daily (B, E)
- **Resolution:** Both are true for their calendar. lhbench's baseline compacts every file holding a pending subject weekly; the erasure A-default touches only the last 7 partitions weekly and everything monthly.

## Delete mode: copy-on-write vs merge-on-read (deletion vectors)

- **Verdict:** agree
- **lhbench:** 33 d CoW vs 34 d MoR (1 day); bytes rewritten CoW/MoR = 2.89x (E1 scattered) -- the 2.9x on the slide is measured, not the model's 13.2/4.6
- **erasure harness:** 16.5 d CoW vs 16.5 d MoR at equal daily cadence (tie); bytes CoW/lazy-MoR = 1.72x, CoW/daily-MoR = 1.00x
- **Resolution:** Delete mode does not move the deadline; the byte saving comes from the lazier calendar MoR ships with (2.9x under lhbench's weekly file-FIFO, 1.7x under the erasure weekly window). Say 'MoR under a weekly calendar' on the slide, not 'MoR'.

## Data layout (scattered / clustered / bucketed)

- **Verdict:** agree
- **lhbench:** 0 days of residency difference; bytes differ (bucketed 4.1x scattered under CoW)
- **erasure harness:** not varied (one layout)
- **Resolution:** Layout is a cost lever, not a time lever. Combined harness carries all three layouts.

## Bucket versioning adds NoncurrentDays additively

- **Verdict:** agree
- **lhbench:** +7 -> 39 d, +10 -> 42 d from 32 d: exact on a day clock
- **erasure harness:** N=7: 30.6, 14: 37.6, 21: 44.6, 30: 53.6 d; bucket stage = N + 0.83 d (midnight-UTC rounding)
- **Resolution:** Additive in both. The hour clock shows the rounding AWS documents; a day clock cannot.

## Coordinated scheduling

- **Verdict:** operating
- **lhbench:** 34 -> 20 d (the retention floor), 100% met; zero variance across 5 seeds ([20, 20, 20, 20, 20]); holds at 25 KB/day (met 100% vs baseline 3%); amplification 1.58x the baseline's
- **erasure harness:** C-coordinated (defer the rewrite to the latest safe pass): 27.1 d vs E-asap-daily 13.4 d, same zero-miss edge at NoncurrentDays 23, bytes within 0.4%
- **Resolution:** Two different mechanisms share a name. lhbench's deadline policy is EDF file selection under a byte budget plus pulling expiry/cleanup forward; the erasure C policy defers the rewrite and runs the tail daily. Both find the tail (retention + D_nc) is the floor. The combined preset 'combined-coordinated' is EDF + budget + daily tail + Iceberg defaults.

## Feasibility test

- **Verdict:** agree
- **lhbench:** retention + D_nc <= window; slide: 8 d floor (5 + 3) + 7 d + 15 d headroom = 30
- **erasure harness:** commit + retention + expiry cadence + N + 1 <= window; max N = 22 d with daily expiry, 16 with weekly; empirical edge 23 d
- **Resolution:** Same inequality; the erasure form carries the expiry cadence and the rounding day, and the combined `feasibility.floor_days` adds the orphan cadence when expiry does not delete files. Slide 3 (20 d measured) vs slide 14 (5 + 3 d floors) is not a contradiction once the slide says 'documented minimums; our runs used 20 d'.

## Cost of having no lifecycle rule (modelled)

- **Verdict:** stale
- **lhbench:** slides 12/14 quote $135,048 / $12,646 / $8,643 (2.4 PiB, 4% churn)
- **erasure harness:** current results/model.json: no rule $72,427 year one, $12,423 with a 7-day rule, $197,224 in year two; residency 25.02 d, write amp 4.6
- **Resolution:** The slide numbers come from an earlier model revision. `residency.costmodel` takes residency and amplification from a measured run, prints every assumption, and rounds to two significant figures.

## Delta Lake and Apache Hudi floors

- **Verdict:** agree
- **lhbench:** modelled: 7 d VACUUM; 10 commits (0.4 d hourly, 10 d daily)
- **erasure harness:** modelled from the same documentation; Hudi KEEP_LATEST_BY_HOURS as the fix
- **Resolution:** Neither harness runs Delta or Hudi. `formats.py` quotes the sources.

## Storage ladder (which bucket configurations are on the slide)

- **Verdict:** conflict
- **lhbench:** S3 off / S3 + rule / GCS / AWS S3 Tables 10 d / S3 no rule / Object Lock (figure); table lists Azure
- **erasure harness:** S3 off / S3 + 7 / GCS 7 / S3 no rule / Object Lock (executed); Azure in the table only
- **Resolution:** Pick one set. `stores.PRESETS` has all of them (plus MinIO count-only and Azure soft delete) so figure and table can be generated from the same list.

## Provenance of the headline runs

- **Verdict:** conflict
- **lhbench:** E1/E2, S-* and O-store carry dirty_threshold 0.05 and predate the file-granular executor: V-dirty at 0.05 gives 38 d / met 11% while E1 at 0.05 gives 34 d / met 25%
- **erasure harness:** 154 runs from one code version; backtest reproduces one seed exactly
- **Resolution:** Re-run the lhbench main suite with the committed code (or the combined harness) before the deck quotes 34 d; state the threshold used.
