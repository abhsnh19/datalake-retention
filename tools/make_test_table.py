#!/usr/bin/env python3
"""
Build local Iceberg tables with KNOWN deletion-scatter characteristics.

Purpose: validate `scatter.py` against exact, row-level ground truth before you
ever point it at a production table. Two tables are created with identical rows
but different physical layouts:

  events_scattered  -- rows in arrival order (user_id randomly distributed).
                       A single user's rows land in nearly every data file.
  events_clustered  -- same rows sorted by user_id before writing.
                       A single user's rows land in one or two data files.

The contrast between them is exactly the effect the paper argues about, so it
doubles as a sanity check that the metric is measuring what you think it is.

Usage:
    python3 make_test_table.py --warehouse ./_wh
"""

from __future__ import annotations

import argparse
import os
import random
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog

N_USERS = 500
N_ROWS = 60_000
N_CHUNKS = 30  # separate appends -> at least this many data files
SEED = 20260823


def build_catalog(warehouse: Path) -> SqlCatalog:
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{warehouse / 'catalog.db'}",
            "warehouse": f"file://{warehouse.resolve()}",
        },
    )


def synth_rows() -> tuple[list[int], list[str], list[int]]:
    """Generate (user_id, event_date, amount) columns."""
    rng = random.Random(SEED)
    user_ids = [rng.randrange(N_USERS) for _ in range(N_ROWS)]
    # 90 days of events; date is uncorrelated with user, as in most event tables
    dates = [f"2026-{5 + (i % 3):02d}-{1 + (i % 28):02d}" for i in range(N_ROWS)]
    amounts = [rng.randrange(1, 10_000) for _ in range(N_ROWS)]
    return user_ids, dates, amounts


ARROW_SCHEMA = pa.schema(
    [
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("event_date", pa.string(), nullable=False),
        pa.field("amount", pa.int64(), nullable=False),
    ]
)


def write_table(catalog: SqlCatalog, name: str, tbl: pa.Table) -> None:
    ident = f"lake.{name}"
    if catalog.table_exists(ident):
        catalog.drop_table(ident)
    iceberg_tbl = catalog.create_table(ident, schema=ARROW_SCHEMA)

    # Append in chunks so we get many data files with distinct value ranges.
    rows_per_chunk = tbl.num_rows // N_CHUNKS
    for c in range(N_CHUNKS):
        start = c * rows_per_chunk
        end = tbl.num_rows if c == N_CHUNKS - 1 else start + rows_per_chunk
        iceberg_tbl.append(tbl.slice(start, end - start))
    print(f"  wrote {ident}: {tbl.num_rows} rows in {N_CHUNKS} appends")


def ground_truth(iceberg_tbl) -> dict:
    """Exact per-user file sets, computed by reading every data file.

    This is the oracle. It reads row values, which is precisely what the
    metadata-only analyzer must AVOID doing -- that is the whole point of the
    comparison.
    """
    files = iceberg_tbl.inspect.files().to_pylist()
    per_user: dict[int, set[str]] = {}
    file_bytes: dict[str, int] = {}
    for f in files:
        path = f["file_path"]
        file_bytes[path] = f["file_size_in_bytes"]
        local = path.replace("file://", "")
        users = pq.read_table(local, columns=["user_id"])["user_id"].to_pylist()
        for u in set(users):
            per_user.setdefault(u, set()).add(path)
    return {"per_user": per_user, "file_bytes": file_bytes, "n_files": len(files)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default="./_wh")
    ap.add_argument("--fresh", action="store_true", help="wipe the warehouse first")
    args = ap.parse_args()

    wh = Path(args.warehouse)
    if args.fresh and wh.exists():
        shutil.rmtree(wh)

    catalog = build_catalog(wh)
    try:
        catalog.create_namespace("lake")
    except Exception:
        pass

    user_ids, dates, amounts = synth_rows()
    base = pa.table(
        {"user_id": user_ids, "event_date": dates, "amount": amounts},
        schema=ARROW_SCHEMA,
    )

    print("Building test tables...")
    write_table(catalog, "events_scattered", base)
    write_table(catalog, "events_clustered", base.sort_by([("user_id", "ascending")]))

    # Emit ground truth for the validator.
    import json

    out = {}
    for name in ("events_scattered", "events_clustered"):
        t = catalog.load_table(f"lake.{name}")
        gt = ground_truth(t)
        out[name] = {
            "n_files": gt["n_files"],
            "total_bytes": sum(gt["file_bytes"].values()),
            "per_user_file_count": {
                str(u): len(paths) for u, paths in sorted(gt["per_user"].items())
            },
            "per_user_bytes": {
                str(u): sum(gt["file_bytes"][p] for p in paths)
                for u, paths in sorted(gt["per_user"].items())
            },
        }
        counts = [len(p) for p in gt["per_user"].values()]
        print(
            f"  {name}: {gt['n_files']} files, "
            f"user touches min={min(counts)} median={sorted(counts)[len(counts)//2]} max={max(counts)}"
        )

    (wh / "ground_truth.json").write_text(json.dumps(out, indent=2))
    print(f"\nGround truth written to {wh / 'ground_truth.json'}")
    print(f"Catalog URI: sqlite:///{(wh / 'catalog.db').resolve()}")


if __name__ == "__main__":
    main()
