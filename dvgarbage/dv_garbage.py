"""DV garbage: how much dead deletion-vector data is pinned inside live Puffin files?

THE MECHANISM UNDER TEST
------------------------
A Puffin file is a container: "Magic Blob1 Blob2 ... BlobN Footer" (puffin-spec).
Deletion vectors are individual blobs, addressed from the manifest by
(file_path, content_offset, content_size_in_bytes). Two consequences follow from
the container structure alone:

  1. A DV dies whenever its data file is superseded -- either because the file
     was rewritten by compaction, or because a NEW DV was written for it. The
     Iceberg spec permits at most one DV per data file per snapshot and requires
     a new DV to merge and replace prior deletes, so every subsequent delete
     against the same data file orphans the previous DV blob.

  2. Storage is reclaimed a FILE at a time, not a blob at a time. One live blob
     pins the entire Puffin file, including every dead blob beside it.

So DV garbage is a classic container-fragmentation problem, and its magnitude is
governed by the packing factor: how many DVs a writer puts in one Puffin file.

WHAT IS REAL AND WHAT IS MODELLED
---------------------------------
REAL: every Puffin file is actually serialized with a spec-compliant writer
(validated against pyiceberg's independent reader), so all byte counts come from
real roaring bitmaps over the simulated delete distribution. Bitmap size as a
function of delete clustering is not modelled -- it is measured.

MODELLED: the table lifecycle (which files receive deletes, when compaction
runs). No Iceberg engine here writes DVs -- pyiceberg is copy-on-write only and
has no Puffin writer -- so the lifecycle is driven by this harness.

We compare two reclaim policies:
  none  -- nobody rewrites Puffin files (what the spec permits and what no
           Iceberg maintenance procedure currently does: there is no
           `rewrite_deletion_vectors`).
  ideal -- an oracle that deletes a Puffin file the instant every blob in it is
           dead. This is the BEST any file-granular reclaimer can do, and the
           gap it still leaves is the part only blob-granular rewriting
           could recover.
"""

from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass, field

from puffin_writer import DV, write_puffin


@dataclass
class Blob:
    data_file: int
    nbytes: int
    dead: bool = False


@dataclass
class PuffinRec:
    pid: int
    blobs: list[Blob]
    nbytes: int
    removed: bool = False

    @property
    def all_dead(self) -> bool:
        return all(b.dead for b in self.blobs)

    def live_bytes(self) -> int:
        return sum(b.nbytes for b in self.blobs if not b.dead)

    def dead_bytes(self) -> int:
        return sum(b.nbytes for b in self.blobs if b.dead)


@dataclass
class DataFileRec:
    fid: int
    n_rows: int
    deleted: set[int] = field(default_factory=set)
    dv: Blob | None = None


def run(
    n_files: int = 200,
    rows_per_file: int = 50_000,
    rounds: int = 60,
    files_touched_per_round: int = 20,
    deletes_per_touch: int = 200,
    packing: int = 16,
    compact_threshold: float = 0.30,
    compact_every: int = 10,
    reclaim: str = "ideal",
    seed: int = 11,
) -> dict:
    rng = random.Random(seed)
    files = {i: DataFileRec(i, rows_per_file) for i in range(n_files)}
    puffins: list[PuffinRec] = []
    next_fid = n_files
    next_pid = 0
    timeline = []
    total_dv_written = 0
    compactions_fired = 0

    for rnd in range(rounds):
        # --- 1. deletes arrive against a random subset of live data files ----
        live_ids = list(files)
        touched = rng.sample(live_ids, min(files_touched_per_round, len(live_ids)))
        new_dvs: list[tuple[int, DV]] = []
        for fid in touched:
            f = files[fid]
            room = f.n_rows - len(f.deleted)
            if room <= 0:
                continue
            k = min(deletes_per_touch, room)
            # GDPR-style point deletes: scattered positions, the worst case for
            # roaring container selection.
            add = set()
            while len(add) < k:
                p = rng.randrange(f.n_rows)
                if p not in f.deleted:
                    add.add(p)
            f.deleted |= add
            # A new DV supersedes the old one -> the old blob is now garbage.
            if f.dv is not None:
                f.dv.dead = True
            new_dvs.append((fid, DV(f"data/{fid}.parquet", sorted(f.deleted))))

        # --- 2. pack the round's DVs into Puffin files -----------------------
        for i in range(0, len(new_dvs), packing):
            chunk = new_dvs[i : i + packing]
            payload = write_puffin([dv for _, dv in chunk])
            # Attribute per-blob bytes by serialized vector size; the footer and
            # file magic are shared overhead, split evenly.
            sizes = []
            for _, dv in chunk:
                sizes.append(len(write_puffin([dv])))
            overhead = max(0, len(payload) - sum(sizes))
            share = overhead // max(1, len(chunk))
            blobs = []
            for (fid, _), sz in zip(chunk, sizes):
                b = Blob(fid, sz + share)
                files[fid].dv = b
                blobs.append(b)
            puffins.append(PuffinRec(next_pid, blobs, len(payload)))
            next_pid += 1
            total_dv_written += len(chunk)

        # --- 3. compaction: rewrite heavily-deleted files --------------------
        if rnd and rnd % compact_every == 0:
            for fid in list(files):
                f = files[fid]
                if f.n_rows and len(f.deleted) / f.n_rows >= compact_threshold:
                    if f.dv is not None:
                        f.dv.dead = True          # data file gone -> DV dead
                    compactions_fired += 1
                    del files[fid]
                    files[next_fid] = DataFileRec(next_fid, rows_per_file)
                    next_fid += 1

        # --- 4. reclaim ------------------------------------------------------
        if reclaim == "ideal":
            for p in puffins:
                if not p.removed and p.all_dead:
                    p.removed = True

        live_p = [p for p in puffins if not p.removed]
        on_disk = sum(p.nbytes for p in live_p)
        live_b = sum(p.live_bytes() for p in live_p)
        dead_b = sum(p.dead_bytes() for p in live_p)
        pinned = sum(1 for p in live_p if p.dead_bytes() > 0 and p.live_bytes() > 0)
        timeline.append({
            "round": rnd,
            "puffin_files_on_disk": len(live_p),
            "puffin_bytes_on_disk": on_disk,
            "live_dv_bytes": live_b,
            "dead_dv_bytes": dead_b,
            "garbage_ratio": (dead_b / on_disk) if on_disk else 0.0,
            "pinned_files": pinned,
        })

    last = timeline[-1]
    return {
        "params": {
            "n_files": n_files, "rounds": rounds, "packing": packing,
            "reclaim": reclaim, "deletes_per_touch": deletes_per_touch,
            "files_touched_per_round": files_touched_per_round,
            "compact_threshold": compact_threshold, "compact_every": compact_every,
        },
        "total_dvs_written": total_dv_written,
        "compactions_fired": compactions_fired,
        "final": last,
        "timeline": timeline,
    }


REGIMES = {
    # Large files, sparse erasure: files never reach the compaction threshold,
    # so ALL DV death comes from supersession (a new DV replacing an old one).
    # This is the append-heavy table with occasional subject erasure.
    "supersession": dict(rows_per_file=50_000, deletes_per_touch=200),
    # Small files, heavy churn: files cross the 30% threshold and are rewritten,
    # so DV death is driven by compaction as well. High-churn CDC-style table.
    "churn": dict(rows_per_file=2_000, deletes_per_touch=100),
}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="dv_garbage_results.json")
    ap.add_argument("--rounds", type=int, default=60)
    args = ap.parse_args()

    results = []
    for regime, kw in REGIMES.items():
        print(f"\n=== regime: {regime} ===")
        for reclaim in ("none", "ideal"):
            for packing in (1, 4, 16, 64):
                r = run(packing=packing, reclaim=reclaim, rounds=args.rounds, **kw)
                r["regime"] = regime
                results.append(r)
                f = r["final"]
                amp = f["puffin_bytes_on_disk"] / max(1, f["live_dv_bytes"])
                print(
                    f"  reclaim={reclaim:5s} packing={packing:3d}  "
                    f"garbage {f['garbage_ratio']:6.1%}  amp {amp:5.1f}x  "
                    f"pinned {f['pinned_files']:4d}/{f['puffin_files_on_disk']:4d}  "
                    f"compactions={r['compactions_fired']}",
                    flush=True,
                )
    json.dump(results, open(args.out, "w"), indent=1)
    print(f"\n-> {args.out}")
