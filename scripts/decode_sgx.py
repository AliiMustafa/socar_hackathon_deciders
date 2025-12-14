#!/usr/bin/env python3
import argparse
import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

HEADER_STRUCT = struct.Struct("<8sII") 
RECORD_STRUCT = struct.Struct("<IffB") 
MAGIC = b"CPETRO01"


def decode_one(path: Path) -> pa.Table:
    data = path.read_bytes()
    if len(data) < HEADER_STRUCT.size:
        raise ValueError("File too small")

    magic, survey_type_id, trace_count = HEADER_STRUCT.unpack_from(data, 0)
    if magic != MAGIC:
        raise ValueError(f"Bad magic: {magic!r}")

    expected = HEADER_STRUCT.size + trace_count * RECORD_STRUCT.size
    if len(data) != expected:
        raise ValueError(f"Size mismatch: got {len(data)}, expected {expected}")

    well_ids = []
    depth_fts = []
    amplitudes = []
    qualities = []

    offset = HEADER_STRUCT.size
    for _ in range(trace_count):
        well_id, depth_ft, amp, q = RECORD_STRUCT.unpack_from(data, offset)
        well_ids.append(well_id)
        depth_fts.append(depth_ft)
        amplitudes.append(amp)
        qualities.append(int(q))
        offset += RECORD_STRUCT.size

    return pa.table(
        {
            "survey_type_id": [survey_type_id] * trace_count,
            "well_id": well_ids,
            "depth_ft": depth_fts,
            "amplitude": amplitudes,
            "quality_flag": qualities,
        }
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out-dir", default="processed_data/sgx_parquet")
    args = ap.parse_args()

    data_dir = Path(args.data_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    sgx_files = list(data_dir.rglob("*.sgx"))
    if not sgx_files:
        print(f"No .sgx files found under {data_dir}")
        return

    for src in sgx_files:
        rel = src.relative_to(data_dir)
        try:
            table = decode_one(src)
            # Keep filenames safe (avoid nested folders issues)
            safe_base = rel.as_posix().replace("/", "_")
            safe_stem = Path(safe_base).stem
            dst = out_dir / f"{safe_stem}_decoded.parquet"
            pq.write_table(table, dst)
            print(f"{rel} -> {dst.name}")
        except Exception as e:
            print(f"{rel}: {e}")


if __name__ == "__main__":
    main()

