#!/usr/bin/env python3
"""
Deletion-scatter analyzer for Apache Iceberg tables.

Answers, from TABLE METADATA ALONE (no row reads, no PII access):

  1. DELETION SCATTER      How many data files must be rewritten to erase one
                           subject's rows?
  2. DELETION AMPLIFICATION How many bytes get rewritten per byte actually
                           deleted?
  3. RESIDENCY FLOOR       How long does deleted data remain readable via time
                           travel, before any cleanup job can possibly run?
  4. DELETE-FILE DEBT      How much merge-on-read delete state has accumulated,
                           and how much read work does it impose?

Why metadata-only matters: every number here comes from Iceberg manifests and
snapshot history. The tool never opens a Parquet data file and never sees a
column value. That keeps it outside the scope of most data-access review
processes, which is what makes it runnable in week one rather than month three.

  Estimates are UPPER BOUNDS. Min/max bounds tell you a file *may* contain a
  key; only a row read proves it does. For an erasure-cost argument the upper
  bound is the honest number -- it is what a rewrite planner must assume.

Usage
-----
  # Against a catalog defined in ~/.pyiceberg.yaml
  python3 scatter.py --catalog prod --table analytics.events --key user_id

  # Against an explicit SQL/REST catalog
  python3 scatter.py --catalog-uri sqlite:///_wh/catalog.db \\
                     --warehouse file://$PWD/_wh \\
                     --table lake.events_scattered --key user_id

  # Refine with real subject keys (a plain text file, one key per line).
  # These can be opaque/hashed values -- the tool only compares them.
  python3 scatter.py ... --keys subjects.txt

  # Machine-readable output for plotting
  python3 scatter.py ... --json out.json
"""

from __future__ import annotations

import argparse
import bisect
import json
import statistics
import sys
from datetime import datetime, timezone
from typing import Any

CONTENT_DATA = 0
CONTENT_POSITION_DELETES = 1
CONTENT_EQUALITY_DELETES = 2


# --------------------------------------------------------------------------
# catalog / table loading
# --------------------------------------------------------------------------


def load_table(args: argparse.Namespace):
    from pyiceberg.catalog import load_catalog

    if args.catalog_uri:
        props: dict[str, str] = {"uri": args.catalog_uri}
        if args.warehouse:
            props["warehouse"] = args.warehouse
        for kv in args.property or []:
            k, _, v = kv.partition("=")
            props[k] = v
        catalog = load_catalog(args.catalog or "adhoc", **props)
    else:
        catalog = load_catalog(args.catalog)
    return catalog.load_table(args.table)


# --------------------------------------------------------------------------
# metadata extraction
# --------------------------------------------------------------------------


def decode_bound(raw: bytes, field_type) -> Any:
    from pyiceberg import conversions

    try:
        return conversions.from_bytes(field_type, raw)
    except Exception:
        return None


def collect_files(table, key_column: str) -> dict[str, Any]:
    """Pull per-file stats and the erasure key's [lower, upper] interval."""
    schema = table.schema()
    field = schema.find_field(key_column)
    field_id, field_type = field.field_id, field.field_type

    rows = table.inspect.files().to_pylist()

    data_files: list[dict[str, Any]] = []
    delete_files: list[dict[str, Any]] = []
    missing_bounds = 0

    for r in rows:
        rec = {
            "path": r["file_path"],
            "content": r["content"],
            "records": r["record_count"],
            "bytes": r["file_size_in_bytes"],
            "format": str(r.get("file_format") or ""),
        }
        if r["content"] != CONTENT_DATA:
            delete_files.append(rec)
            continue

        lo = hi = None
        # Prefer already-decoded metrics when the writer supplied them.
        readable = r.get("readable_metrics") or {}
        col = readable.get(key_column) if isinstance(readable, dict) else None
        if isinstance(col, dict):
            lo, hi = col.get("lower_bound"), col.get("upper_bound")
        if lo is None or hi is None:
            lb = dict(r.get("lower_bounds") or [])
            ub = dict(r.get("upper_bounds") or [])
            if field_id in lb and field_id in ub:
                lo = decode_bound(lb[field_id], field_type)
                hi = decode_bound(ub[field_id], field_type)

        if lo is None or hi is None:
            missing_bounds += 1
        rec["lo"], rec["hi"] = lo, hi
        data_files.append(rec)

    return {
        "data_files": data_files,
        "delete_files": delete_files,
        "missing_bounds": missing_bounds,
    }


# --------------------------------------------------------------------------
# metric 1+2: scatter and amplification
# --------------------------------------------------------------------------


def candidates_for_key(data_files: list[dict], key: Any) -> tuple[int, int]:
    """Files whose [lo,hi] interval could contain `key`. Returns (count, bytes)."""
    n = 0
    b = 0
    for f in data_files:
        lo, hi = f.get("lo"), f.get("hi")
        if lo is None or hi is None:
            # No bounds -> cannot be pruned -> must be treated as a candidate.
            n += 1
            b += f["bytes"]
            continue
        try:
            if lo <= key <= hi:
                n += 1
                b += f["bytes"]
        except TypeError:
            n += 1
            b += f["bytes"]
    return n, b


def probe_points(data_files: list[dict], max_probes: int) -> list[Any]:
    """Sample the key space to estimate scatter without any real subject keys.

    Two probe families are combined:
      * every distinct file boundary (lower and upper bound) -- these are the
        points where coverage depth changes, so they capture the extremes;
      * for ordered numeric keys, an even sweep across the global key range --
        this approximates a uniformly-random subject, which is the number you
        actually want to report.

    Type-agnostic: falls back to boundary-only probing for strings and UUIDs.
    """
    bounds: list[Any] = []
    for f in data_files:
        if f.get("lo") is not None:
            bounds.append(f["lo"])
        if f.get("hi") is not None:
            bounds.append(f["hi"])
    if not bounds:
        return []

    try:
        uniq = sorted(set(bounds))
    except TypeError:
        return []

    probes = list(uniq)

    # Even sweep across the range for numeric keys.
    lo, hi = uniq[0], uniq[-1]
    if isinstance(lo, (int, float)) and not isinstance(lo, bool) and hi > lo:
        n_sweep = max(0, max_probes - len(probes))
        if n_sweep > 0:
            step = (hi - lo) / (n_sweep + 1)
            sweep = [lo + step * (i + 1) for i in range(n_sweep)]
            if isinstance(lo, int) and isinstance(hi, int):
                sweep = [int(round(v)) for v in sweep]
            probes.extend(sweep)
        try:
            probes = sorted(set(probes))
        except TypeError:
            pass

    if len(probes) <= max_probes:
        return probes
    step_i = len(probes) / max_probes
    return [probes[int(i * step_i)] for i in range(max_probes)]


def scatter_analysis(
    data_files: list[dict],
    keys: list[Any] | None,
    max_probes: int,
    n_subjects: int | None,
) -> dict[str, Any]:
    total_files = len(data_files)
    total_bytes = sum(f["bytes"] for f in data_files)
    total_records = sum(f["records"] for f in data_files)

    if keys:
        sample, source = keys, "supplied subject keys"
    else:
        sample, source = probe_points(data_files, max_probes), "key-space probes"

    counts: list[int] = []
    byte_counts: list[int] = []
    for k in sample:
        c, b = candidates_for_key(data_files, k)
        counts.append(c)
        byte_counts.append(b)

    if not counts:
        return {"error": "no probes could be evaluated"}

    def pct(vals: list[int], p: float) -> float:
        s = sorted(vals)
        return s[min(len(s) - 1, int(p * len(s)))]

    # Amplification: bytes rewritten per byte of the subject's own data.
    amp = None
    subject_bytes = None
    if n_subjects and n_subjects > 0 and total_records:
        subject_records = total_records / n_subjects
        bytes_per_record = total_bytes / total_records
        subject_bytes = subject_records * bytes_per_record
        if subject_bytes > 0:
            amp = statistics.median(byte_counts) / subject_bytes

    return {
        "probe_source": source,
        "probes_evaluated": len(counts),
        "total_data_files": total_files,
        "total_bytes": total_bytes,
        "total_records": total_records,
        "files_touched": {
            "min": min(counts),
            "p50": pct(counts, 0.50),
            "p95": pct(counts, 0.95),
            "max": max(counts),
            "mean": statistics.fmean(counts),
        },
        "fraction_of_table_touched": {
            "p50": pct(counts, 0.50) / total_files if total_files else None,
            "p95": pct(counts, 0.95) / total_files if total_files else None,
        },
        "bytes_touched": {
            "p50": pct(byte_counts, 0.50),
            "p95": pct(byte_counts, 0.95),
            "max": max(byte_counts),
        },
        "estimated_subject_bytes": subject_bytes,
        "deletion_amplification_p50": amp,
    }


# --------------------------------------------------------------------------
# metric 3: residency floor
# --------------------------------------------------------------------------


def residency_floor(table) -> dict[str, Any]:
    snaps = table.inspect.snapshots().to_pylist()
    if not snaps:
        return {"error": "no snapshots"}

    def as_dt(v) -> datetime:
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        return datetime.fromtimestamp(v / 1000, tz=timezone.utc)

    times = sorted(as_dt(s["committed_at"]) for s in snaps)
    now = datetime.now(timezone.utc)
    oldest_age_days = (now - times[0]).total_seconds() / 86400
    span_days = (times[-1] - times[0]).total_seconds() / 86400

    gaps = [
        (times[i + 1] - times[i]).total_seconds() / 3600 for i in range(len(times) - 1)
    ]
    ops: dict[str, int] = {}
    for s in snaps:
        ops[str(s.get("operation") or "unknown")] = (
            ops.get(str(s.get("operation") or "unknown"), 0) + 1
        )

    return {
        "snapshot_count": len(snaps),
        "oldest_snapshot_age_days": oldest_age_days,
        "history_span_days": span_days,
        "median_commit_interval_hours": statistics.median(gaps) if gaps else None,
        "operations": ops,
        "note": (
            "Any row deleted today stays readable via time travel until the "
            "oldest retained snapshot ages out. That is the FLOOR on residency "
            "-- physical erasure additionally requires compaction, snapshot "
            "expiry, orphan cleanup, and object-store version expiry."
        ),
    }


# --------------------------------------------------------------------------
# metric 4: delete-file debt
# --------------------------------------------------------------------------


def delete_debt(files: dict[str, Any]) -> dict[str, Any]:
    dels = files["delete_files"]
    data = files["data_files"]
    pos = [d for d in dels if d["content"] == CONTENT_POSITION_DELETES]
    eq = [d for d in dels if d["content"] == CONTENT_EQUALITY_DELETES]
    # Iceberg v3 deletion vectors are position deletes carried in Puffin files.
    dv = [d for d in pos if "PUFFIN" in d["format"].upper()]

    return {
        "data_file_count": len(data),
        "delete_file_count": len(dels),
        "position_delete_files": len(pos),
        "equality_delete_files": len(eq),
        "deletion_vectors_puffin": len(dv),
        "delete_bytes": sum(d["bytes"] for d in dels),
        "delete_to_data_file_ratio": (len(dels) / len(data)) if data else None,
        "note": (
            "Every delete file here is a row that is logically gone but "
            "physically present. This is stage 1 of the residency stack; the "
            "bytes survive until compaction rewrites the data file."
        ),
    }


# --------------------------------------------------------------------------
# layout diagnostics
# --------------------------------------------------------------------------


def layout_diagnostics(table, key_column: str, data_files: list[dict]) -> dict:
    spec = table.spec()
    partition_fields = [f.name for f in spec.fields]
    sort_order = table.sort_order()
    sort_fields = []
    try:
        schema = table.schema()
        for sf in sort_order.fields:
            sort_fields.append(schema.find_field(sf.source_id).name)
    except Exception:
        pass

    bounded = [f for f in data_files if f.get("lo") is not None]
    overlap_note = None
    if bounded:
        try:
            global_lo = min(f["lo"] for f in bounded)
            global_hi = max(f["hi"] for f in bounded)
            full_span = sum(
                1 for f in bounded if f["lo"] == global_lo and f["hi"] == global_hi
            )
            overlap_note = (
                f"{full_span}/{len(bounded)} data files span the entire observed "
                f"key range -- those can never be pruned for any subject."
            )
        except TypeError:
            pass

    return {
        "partitioned_by": partition_fields or ["<unpartitioned>"],
        "sorted_by": sort_fields or ["<unsorted>"],
        "erasure_key_is_partition_key": key_column in partition_fields,
        "erasure_key_is_sort_key": key_column in sort_fields,
        "full_range_files": overlap_note,
        "files_missing_bounds": sum(1 for f in data_files if f.get("lo") is None),
    }


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------


def human_bytes(n: float | None) -> str:
    if n is None:
        return "n/a"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} EiB"


def report(result: dict[str, Any]) -> str:
    s = result["scatter"]
    r = result["residency"]
    d = result["delete_debt"]
    lay = result["layout"]
    L: list[str] = []
    add = L.append

    add("=" * 72)
    add(f"DELETION SCATTER REPORT  --  {result['table']}")
    add(f"erasure key: {result['key_column']}")
    add("=" * 72)

    add("")
    add("LAYOUT")
    add(f"  partitioned by ......... {', '.join(lay['partitioned_by'])}")
    add(f"  sorted by .............. {', '.join(lay['sorted_by'])}")
    add(f"  key is partition key ... {lay['erasure_key_is_partition_key']}")
    add(f"  key is sort key ........ {lay['erasure_key_is_sort_key']}")
    if lay["full_range_files"]:
        add(f"  {lay['full_range_files']}")
    if lay["files_missing_bounds"]:
        add(
            f"  WARNING: {lay['files_missing_bounds']} files have no bounds for this "
            "key and are counted as unprunable."
        )

    add("")
    add("1. DELETION SCATTER  (files that must be rewritten to erase one subject)")
    add(f"  probe source ........... {s['probe_source']} (n={s['probes_evaluated']})")
    add(f"  table size ............. {s['total_data_files']:,} files, "
        f"{human_bytes(s['total_bytes'])}, {s['total_records']:,} rows")
    ft = s["files_touched"]
    add(f"  files touched .......... min={ft['min']:,}  p50={ft['p50']:,}  "
        f"p95={ft['p95']:,}  max={ft['max']:,}")
    frac = s["fraction_of_table_touched"]
    if frac["p50"] is not None:
        add(f"  share of table ......... p50={frac['p50']:.1%}  p95={frac['p95']:.1%}")
    bt = s["bytes_touched"]
    add(f"  bytes rewritten ........ p50={human_bytes(bt['p50'])}  "
        f"p95={human_bytes(bt['p95'])}")

    add("")
    add("2. DELETION AMPLIFICATION")
    if s.get("deletion_amplification_p50"):
        add(f"  subject's own data ..... ~{human_bytes(s['estimated_subject_bytes'])}")
        add(f"  amplification (p50) .... {s['deletion_amplification_p50']:,.0f}x")
        add("  -> bytes rewritten per byte of the subject's data actually erased.")
    else:
        add("  pass --n-subjects <N> (distinct subjects in the table) to compute this.")

    add("")
    add("3. RESIDENCY FLOOR  (how long a deleted row stays readable, at minimum)")
    if "error" in r:
        add(f"  {r['error']}")
    else:
        add(f"  retained snapshots ..... {r['snapshot_count']:,}")
        add(f"  oldest snapshot age .... {r['oldest_snapshot_age_days']:,.1f} days")
        add(f"  history span ........... {r['history_span_days']:,.1f} days")
        if r["median_commit_interval_hours"] is not None:
            add(f"  median commit interval . {r['median_commit_interval_hours']:,.2f} h")
        add(f"  operations ............. {r['operations']}")
        add(f"  -> floor on time-travel residency: {r['oldest_snapshot_age_days']:,.1f} days")

    add("")
    add("4. DELETE-FILE DEBT  (logically deleted, physically present)")
    add(f"  data files ............. {d['data_file_count']:,}")
    add(f"  delete files ........... {d['delete_file_count']:,}  "
        f"(position={d['position_delete_files']:,}, equality={d['equality_delete_files']:,}, "
        f"deletion vectors={d['deletion_vectors_puffin']:,})")
    add(f"  delete bytes ........... {human_bytes(d['delete_bytes'])}")
    if d["delete_to_data_file_ratio"] is not None:
        add(f"  delete:data ratio ...... {d['delete_to_data_file_ratio']:.3f}")

    add("")
    add("-" * 72)
    add("Reading this: high scatter + high amplification means erasing one")
    add("subject rewrites a large share of the table, so maintenance cannot")
    add("keep up with erasure demand and residency grows without bound.")
    add("All figures are metadata-derived UPPER BOUNDS -- no rows were read.")
    add("-" * 72)
    return "\n".join(L)


# --------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--table", required=True, help="namespace.table")
    ap.add_argument("--key", required=True, dest="key_column",
                    help="erasure key column (e.g. user_id, subject_id)")
    ap.add_argument("--catalog", default="default")
    ap.add_argument("--catalog-uri")
    ap.add_argument("--warehouse")
    ap.add_argument("--property", action="append",
                    help="extra catalog property, k=v (repeatable)")
    ap.add_argument("--keys", help="file of real subject keys, one per line")
    ap.add_argument("--max-probes", type=int, default=200)
    ap.add_argument("--n-subjects", type=int,
                    help="distinct subjects in the table; enables amplification")
    ap.add_argument("--json", dest="json_out", help="write full results as JSON")
    args = ap.parse_args()

    table = load_table(args)
    files = collect_files(table, args.key_column)

    keys = None
    if args.keys:
        raw = [ln.strip() for ln in open(args.keys) if ln.strip()]
        # Match the physical type of the bounds so comparisons work.
        sample_bound = next(
            (f["lo"] for f in files["data_files"] if f.get("lo") is not None), None
        )
        if isinstance(sample_bound, int):
            keys = [int(x) for x in raw]
        elif isinstance(sample_bound, float):
            keys = [float(x) for x in raw]
        else:
            keys = raw

    result = {
        "table": args.table,
        "key_column": args.key_column,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "layout": layout_diagnostics(table, args.key_column, files["data_files"]),
        "scatter": scatter_analysis(
            files["data_files"], keys, args.max_probes, args.n_subjects
        ),
        "residency": residency_floor(table),
        "delete_debt": delete_debt(files),
    }

    print(report(result))

    if args.json_out:
        with open(args.json_out, "w") as fh:
            json.dump(result, fh, indent=2, default=str)
        print(f"\nJSON written to {args.json_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
