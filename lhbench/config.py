"""Experiment configuration.

Every parameter that shapes a result lives here, so the sensitivity sweep is
mechanical and nothing is hidden in code. Defaults are annotated with their
justification -- for a synthetic-workload paper, a parameter you cannot defend
is a parameter a reviewer will attack.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Literal

Layout = Literal["scattered", "clustered", "bucketed"]
ObjectStore = Literal["none", "versioned"]
DeleteMode = Literal["cow", "mor"]
PolicyName = Literal["baseline", "deadline"]


@dataclass
class Config:
    # ---- identity -------------------------------------------------------
    name: str = "default"
    seed: int = 20260823

    # ---- horizon --------------------------------------------------------
    horizon_days: int = 120
    """Simulated days. Must exceed retention + orphan interval, or the tail of
    the residency distribution is truncated and every result is optimistic."""

    warmup_days: int = 15
    tail_days: int = 45
    """Analysis cohort. Obligations issued during warmup see an
    unrepresentatively small table; those issued in the final `tail_days`
    cannot complete before the horizon, so counting them as deadline misses
    would overstate the problem. Headline numbers use the cohort only."""

    # ---- data / ingest --------------------------------------------------
    rows_per_day: int = 4_000
    n_subjects: int = 4_000
    subject_alpha: float = 1.2
    """Power-law exponent for rows-per-subject. Real user-activity
    distributions are heavy-tailed; alpha near 1.0-1.5 is the usual range.
    Swept in the sensitivity analysis."""

    layout: Layout = "scattered"
    """scattered  rows written in arrival order (the common case).
    clustered  each day's batch sorted by subject before writing. Included
               deliberately as a NEGATIVE control: sorting within a batch
               cannot reduce cross-file scatter, because a subject still
               appears in every day's file. Reporting this is worth more than
               omitting it -- it rules out the cheap fix a reader will propose.
    bucketed   table partitioned by bucket(subject_id). A subject's rows land
               in one partition, so a rewrite touches ~1/n_buckets of the
               table. This is the layout the mechanism argument depends on."""

    n_buckets: int = 16
    """Bucket count for the `bucketed` layout. Trades erasure blast radius
    against small-file pressure -- worth a sweep of its own."""

    # ---- erasure demand -------------------------------------------------
    dsar_rate_per_day: float = 3.0
    """Mean erasure requests per day. Ground this against published privacy-
    request volumes rather than picking it; swept in sensitivity analysis."""

    dsar_burst_prob: float = 0.05
    dsar_burst_multiplier: float = 20.0
    """Erasure demand is bursty -- requests cluster after breaches, press
    coverage, and policy changes. A Poisson-only model understates the tail
    and flatters any scheduler."""

    deadline_days: int = 30
    """Compliance window. GDPR Art. 12(3) is one month. This is a parameter,
    not a legal claim -- the paper should never argue law."""

    # ---- table format behaviour ----------------------------------------
    delete_mode: DeleteMode = "cow"
    """cow: the rewrite happens at delete time.
    mor: the logical delete lands immediately but bytes survive until
    compaction. Physically this is exactly a deferred rewrite, which is what
    the simulator models."""

    # ---- maintenance ----------------------------------------------------
    policy: PolicyName = "baseline"

    retention_days: int = 30
    """Time-travel retention. Iceberg's shipped default is far longer than
    most compliance windows -- that tension is a finding, not an assumption."""

    compact_interval_days: int = 7
    expire_interval_days: int = 7
    object_store: ObjectStore = "none"
    """none      the warehouse is a plain filesystem; a delete frees the bytes
              immediately. This is what stages 1-4 assumed.
    versioned an S3-style store with bucket versioning ON: deleting an object
              writes a delete marker and the bytes survive as a NONCURRENT
              VERSION until a lifecycle rule expires them. Stage 5."""

    noncurrent_expiration_days: int = 7
    """D_nc -- days from delete marker to physical removal of the noncurrent
    version. Default 7 is not a guess: four independent provider defaults
    converge on it. GCS soft delete is ON BY DEFAULT at 7 days and cannot be
    bypassed; Azure portal-provisioned accounts default to 7; Databricks'
    documented fallback when S3 versioning cannot be disabled is a lifecycle
    policy retaining "versions for 7 days or less". -1 means never (no
    lifecycle rule at all, which is the S3 default -- noncurrent versions
    persist indefinitely and are billed forever)."""

    orphan_interval_days: int = 30
    """Orphan cleanup is the step operators most often defer or skip. Its
    cadence usually dominates residency, which is the counterintuitive result
    worth reporting."""

    dirty_threshold: float = 0.0
    """Baseline compacts a file once this fraction of its rows are pending
    deletion -- a file-level analogue of the delete-ratio heuristics shipped by
    real maintenance jobs.

    DEFAULT IS 0.0 DELIBERATELY: it makes the baseline as STRONG as possible
    (rewrite any file holding a pending erasure, gated only by cadence and
    budget). A non-zero threshold is what real systems ship, and it makes the
    baseline markedly worse -- 0.05 costs it 4 days of residency and half its
    compliance. Reporting the headline against the strongest baseline is the
    honest construction; the threshold sweep belongs in sensitivity, where it
    shows the baseline is even worse in practice than we credit it."""

    daily_rewrite_budget_bytes: int = 2_000_000
    """THE FAIRNESS KNOB. Both policies get the same budget, so the mechanism
    must win by scheduling better, not by spending more. Any comparison
    without this is worthless."""

    def tag(self) -> str:
        return (
            f"{self.name}-{self.layout}-{self.delete_mode}-{self.policy}"
            f"-r{self.retention_days}-o{self.orphan_interval_days}"
            f"-b{self.n_buckets}"
            f"-{self.object_store}{self.noncurrent_expiration_days}"
        )

    def to_dict(self) -> dict:
        return asdict(self)
