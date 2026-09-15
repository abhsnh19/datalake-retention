"""Cost at scale, with every assumption on the page.

Abhishek's review of the deck: "six-figure precision from an unvalidated
model is the easiest thing for an audience to poke at."  So this module
(a) takes residency and write amplification FROM A MEASURED RUN when given
one, (b) lists every assumption it multiplies them by, and (c) rounds the
result to two significant figures for the slide.

Two quantities, deliberately separated:
  dead bytes        logically deleted rows still resident (Little's law:
                    deletion stream x residency)  -- the compliance quantity
  noncurrent bytes  superseded object versions a versioned bucket keeps
                    until a lifecycle rule expires them -- the money.  With
                    no rule the pile grows for the life of the table.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict


@dataclass
class Assumptions:
    table_tb: float = 2400.0            # regulated fact table, TiB
    annual_churn: float = 0.04          # share of rows erased per year
    storage_usd_tb_month: float = 23.55  # S3 Standard, us-east-1 list ($0.023/GB-month)
    compute_usd_tb_rewritten: float = 18.50  # blended engine cost to rewrite 1 TiB
    residency_days: float = 30.0        # from a run (lhbench p50 or erasure median)
    write_amp: float = 3.0              # from a run (bytes rewritten / subject bytes)
    residency_source: str = "assumed"
    write_amp_source: str = "assumed"

    def __post_init__(self):
        for k in ("table_tb", "annual_churn", "storage_usd_tb_month", "compute_usd_tb_rewritten",
                  "residency_days", "write_amp"):
            if getattr(self, k) < 0:
                raise ValueError(f"{k} must be non-negative")
        if self.annual_churn > 1:
            raise ValueError("annual_churn is a fraction of the table")


def from_run(result: dict, **overrides) -> Assumptions:
    """Residency and amplification from a residency-harness run."""
    s = result["summary"]
    res = s["lhbench"]["drt_days"]["p50"] if s.get("lhbench") else None
    amp = s["lhbench"].get("deletion_amplification") if s.get("lhbench") else None
    if res is None or amp is None:
        raise ValueError("run has no cohort summary to take residency / amplification from")
    return Assumptions(residency_days=float(res), write_amp=float(amp),
                       residency_source=f"run {result.get('policy')} / {result['store'].get('label')} seed {result.get('seed')}",
                       write_amp_source="same run", **overrides)


def annual_deleted_tb(a: Assumptions) -> float:
    return a.table_tb * a.annual_churn


def dead_bytes_tb(a: Assumptions) -> float:
    return annual_deleted_tb(a) / 365.0 * a.residency_days


def noncurrent_tb(a: Assumptions, expire_days: float | None, year: int = 1) -> float:
    """Standing superseded-version volume.  With no rule the pile grows
    linearly; its mean over year ``year`` is (year - 0.5) x annual rewrite."""
    if year < 1:
        raise ValueError("year starts at 1")
    per_day = annual_deleted_tb(a) * a.write_amp / 365.0
    if expire_days is None:
        return per_day * 365.0 * (year - 0.5)
    if expire_days < 0:
        raise ValueError("expire_days must be non-negative or None")
    return per_day * expire_days


def annual_cost(a: Assumptions, expire_days: float | None = 7.0, year: int = 1) -> dict:
    dead = dead_bytes_tb(a)
    nc = noncurrent_tb(a, expire_days, year)
    carry = (dead + nc) * a.storage_usd_tb_month * 12.0
    rewritten = annual_deleted_tb(a) * a.write_amp
    compute = rewritten * a.compute_usd_tb_rewritten
    return {"dead_tb": round(dead, 2), "noncurrent_tb": round(nc, 2), "carry_usd": round(carry),
            "rewritten_tb": round(rewritten, 2), "compute_usd": round(compute), "total_usd": round(carry + compute)}


def rounded(usd: float, sig: int = 2) -> str:
    """$135,048 -> $140,000: what a slide should print."""
    if usd == 0:
        return "$0"
    from math import floor, log10
    mag = 10 ** (floor(log10(abs(usd))) - sig + 1)
    return f"${round(usd / mag) * mag:,.0f}"


def rule_value(a: Assumptions, rule_days: float = 7.0) -> dict:
    no_rule = annual_cost(a, None, 1)
    y2 = annual_cost(a, None, 2)
    rule = annual_cost(a, rule_days)
    return {"assumptions": asdict(a), "rule_days": rule_days,
            "no_rule_first_year": no_rule, "no_rule_year_two": y2, "rule": rule,
            "usd_saved_first_year": round(no_rule["total_usd"] - rule["total_usd"]),
            "pct_saved_first_year": round(100 * (no_rule["total_usd"] - rule["total_usd"]) / no_rule["total_usd"], 1)
            if no_rule["total_usd"] else 0.0,
            "slide": {"no_rule_first_year": rounded(no_rule["total_usd"]), "rule": rounded(rule["total_usd"]),
                      "no_rule_year_two": rounded(y2["total_usd"])}}
