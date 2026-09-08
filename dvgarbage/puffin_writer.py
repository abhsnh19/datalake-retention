"""A spec-compliant Puffin writer for deletion-vector-v1 blobs.

pyiceberg ships a Puffin *reader* but no writer, so this fills the gap and --
more importantly -- lets the DV-garbage experiment build real Puffin files whose
correctness is checked by an INDEPENDENT implementation (pyiceberg's reader).

Layout, from format/puffin-spec.md:

  file   := "PFA1" blob* footer_json footer_len(4B LE) flags(4B) "PFA1"
  blob   := length(4B BE, = 4 + len(vector))
            magic(4B, D1 D3 39 64)
            vector
            crc32(4B BE, over magic + vector)
  vector := n_bitmaps(8B LE) ( key(4B LE) roaring32 )*

Footer blob metadata carries `offset` = absolute offset of the blob start and
`length` = total blob size. `snapshot-id` and `sequence-number` must be -1 for
Puffin v1, `compression-codec` must be omitted, and `referenced-data-file` and
`cardinality` are required properties for deletion vectors.
"""

from __future__ import annotations

import json
import zlib
from dataclasses import dataclass

from pyroaring import BitMap

FILE_MAGIC = b"PFA1"
BLOB_MAGIC = bytes([0xD1, 0xD3, 0x39, 0x64])


def serialize_vector(positions: list[int]) -> bytes:
    """64-bit roaring 'portable' form: LE count, then (key, 32-bit bitmap) pairs."""
    by_key: dict[int, BitMap] = {}
    for p in positions:
        by_key.setdefault(p >> 32, BitMap()).add(p & 0xFFFFFFFF)
    out = len(by_key).to_bytes(8, "little")
    for key in sorted(by_key):
        out += key.to_bytes(4, "little") + by_key[key].serialize()
    return out


def make_blob(positions: list[int]) -> bytes:
    vec = serialize_vector(positions)
    body = BLOB_MAGIC + vec
    return (len(body)).to_bytes(4, "big") + body + \
        (zlib.crc32(body) & 0xFFFFFFFF).to_bytes(4, "big")


@dataclass
class DV:
    referenced_data_file: str
    positions: list[int]


def write_puffin(dvs: list[DV]) -> bytes:
    """Serialize one Puffin file holding `len(dvs)` deletion vectors."""
    out = bytearray(FILE_MAGIC)
    blobs_meta = []
    for dv in dvs:
        blob = make_blob(dv.positions)
        blobs_meta.append({
            "type": "deletion-vector-v1",
            "fields": [],
            "snapshot-id": -1,
            "sequence-number": -1,
            "offset": len(out),
            "length": len(blob),
            "properties": {
                "referenced-data-file": dv.referenced_data_file,
                "cardinality": str(len(dv.positions)),
            },
        })
        out += blob

    footer = json.dumps({"blobs": blobs_meta, "properties": {}}).encode()
    out += footer
    out += len(footer).to_bytes(4, "little")
    out += b"\x00\x00\x00\x00"          # flags: footer uncompressed
    out += FILE_MAGIC
    return bytes(out)
