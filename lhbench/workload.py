"""Workload generation: subjects, ingest, and erasure demand.

Three things must be defensible to a reviewer, so each is explicit and
parameterised rather than baked in:

  1. rows are distributed over subjects by a power law, not uniformly;
  2. erasure requests arrive in bursts, not as smooth Poisson;
  3. physical layout (arrival order vs. clustered) is a knob, because it is
     the variable the mechanism argument turns on.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from .config import Config

SCHEMA = pa.schema(
    [
        pa.field("subject_id", pa.int64(), nullable=False),
        pa.field("event_day", pa.int64(), nullable=False),
        pa.field("payload", pa.string(), nullable=False),
    ]
)

#: Declared explicitly (rather than inferred from the Arrow schema) so that
#: field IDs are stable and a partition spec can reference subject_id by ID.
ICEBERG_SCHEMA = Schema(
    NestedField(1, "subject_id", LongType(), required=True),
    NestedField(2, "event_day", LongType(), required=True),
    NestedField(3, "payload", StringType(), required=True),
)
SUBJECT_FIELD_ID = 1

_PAYLOAD = "x" * 48  # keeps file sizes realistic relative to row counts


@dataclass
class Workload:
    cfg: Config

    def __post_init__(self) -> None:
        self.rng = random.Random(self.cfg.seed)
        self._weights = self._subject_weights()
        self._population = list(range(self.cfg.n_subjects))
        self._erased: set[int] = set()

    # -- subjects ---------------------------------------------------------

    def _subject_weights(self) -> list[float]:
        """Zipf-like weights: subject i gets weight 1/(i+1)^alpha."""
        a = self.cfg.subject_alpha
        return [1.0 / ((i + 1) ** a) for i in range(self.cfg.n_subjects)]

    # -- ingest -----------------------------------------------------------

    def ingest_batch(self, day: int) -> tuple[pa.Table, list[int]]:
        """One day of events. Returns (arrow table, subject ids in row order)."""
        n = self.cfg.rows_per_day
        subjects = self.rng.choices(self._population, weights=self._weights, k=n)
        # Rows for already-erased subjects must not reappear, or residency is
        # unmeasurable: the subject would be perpetually re-created.
        subjects = [s for s in subjects if s not in self._erased]
        if not subjects:
            subjects = [self.rng.randrange(self.cfg.n_subjects)]

        if self.cfg.layout == "clustered":
            subjects.sort()

        tbl = pa.table(
            {
                "subject_id": subjects,
                "event_day": [day] * len(subjects),
                "payload": [_PAYLOAD] * len(subjects),
            },
            schema=SCHEMA,
        )
        return tbl, subjects

    # -- erasure demand ---------------------------------------------------

    def dsars(self, day: int, live_subjects: set[int]) -> list[int]:
        """Erasure requests arriving on `day`.

        Bursty by construction: with probability `dsar_burst_prob` the day's
        rate is multiplied, modelling incident- and policy-driven clustering.
        """
        rate = self.cfg.dsar_rate_per_day
        if self.rng.random() < self.cfg.dsar_burst_prob:
            rate *= self.cfg.dsar_burst_multiplier

        # Poisson draw via Knuth's algorithm (stdlib has no poisson).
        n = self._poisson(rate)

        candidates = list(live_subjects - self._erased)
        if not candidates:
            return []
        n = min(n, len(candidates))
        chosen = self.rng.sample(candidates, n)
        self._erased.update(chosen)
        return chosen

    def _poisson(self, lam: float) -> int:
        if lam <= 0:
            return 0
        import math

        limit = math.exp(-lam)
        k, p = 0, 1.0
        while True:
            p *= self.rng.random()
            if p <= limit:
                return k
            k += 1
            if k > 10_000:  # numerical guard
                return k
