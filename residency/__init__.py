"""residency — one harness for how long a deleted row's bytes survive in a lakehouse.

Joint work: Abhishek Sinha (``lhbench``, this repository) and Nilanjan
Chatterjee (``erasure_harness``, the Monster Scale 2027 deck).  Each harness
drove a REAL Apache Iceberg table (PyIceberg) through a synthetic erasure
workload in VIRTUAL time and observed, not modelled, when the bytes left
storage.  They disagreed on nothing physical; they measured different
operating points and reported different decompositions.  This package is the
union, so one run answers both decks:

    lhbench                              erasure_harness
    -------                              ---------------
    day clock                            hour clock, S3 midnight-UTC rounding
    20-day retention, 15-day orphan job  Iceberg defaults: 5 d retention, 3 d orphan age
    expiry rewrites metadata only        expiry deletes what it unreferences (Java action)
    file-granular rewrite, daily budget  subject-level rewrite, no budget
    baseline / deadline (free rides,     A..F calendars: weekly window, daily,
      pull expiry+cleanup forward)         deferred-to-deadline, copy-on-write
    plain FS or versioned + D_nc         unversioned / versioned / soft delete /
                                           Object Lock, boto3-shaped listing
    cohort (warmup, tail) + censoring    never-censor horizon + probe read-back
    verify.py claim checks               backtest, ListObjectVersions probe

Stage vocabulary (both kept, one ledger):

    arrival      the erasure request is issued                    (lhbench: issued)
    commit       the delete lands in the nightly batch  = t0      (lhbench: logical)
    rewritten    no CURRENT data file holds the rows   = t1      (lhbench: rewritten)
    unreferenced no LIVE snapshot references such a file           (lhbench: unreferenced)
    unlinked     every such file received its DELETE   = t2      (lhbench: delete_marker)
    physical     the bucket freed the last byte        = t3      (lhbench: physical)

Deletion residency time (DRT) = physical - arrival.  ``Ledger.summary`` reports
the lhbench view (median days-to-stage) and the erasure view (per-stage
durations) from the same timestamps.

What is real, emulated, modelled
--------------------------------
REAL      every snapshot, manifest, data file; copy-on-write deletes; the
          file-granular rewrite; snapshot expiry; which files are referenced.
EMULATED  the object store (``stores.py`` quotes the AWS / Google / Microsoft
          sentences it implements); merge-on-read as a deferred rewrite
          (PyIceberg writes no deletion vectors); virtual time.
MODELLED  Delta Lake and Apache Hudi floors (``formats.py``, documented
          values); cost at scale (``costmodel.py``, every assumption printed).
"""

__version__ = "0.1.0"

DEFAULT_SEEDS = (20260823, 20260904, 20260905)
