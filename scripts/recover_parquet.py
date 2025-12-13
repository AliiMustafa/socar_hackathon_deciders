#!/usr/bin/env python3
import argparse
import shutil
from pathlib import Path

import pyarrow.parquet as pq


def is_readable_parquet(path: Path) -> bool:
    try:
        pq.ParquetFile(str(path))
        return True
    except Exception:
        return False


def recover_one(src: Path, dst: Path) -> bool:
    data = src.read_bytes()
    last_par1 = data.rfind(b"PAR1")
    if last_par1 == -1:
        return False

    clean = data[: last_par1 + 4]

    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(clean)

    return is_readable_parquet(dst)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out-dir", default="processed_data/parquet_recovered")
    args = ap.parse_args()

    data_dir = Path(args.data_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = list(data_dir.rglob("*.parquet"))
    if not parquet_files:
        print(f"No parquet files found under {data_dir}")
        return

    for src in parquet_files:
        rel = src.relative_to(data_dir)
        dst = out_dir / rel

        if is_readable_parquet(src):
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            print(f"readable copy: {rel}")
            continue

        ok = recover_one(src, dst)
        if ok:
            print(f"{rel}")
        else:
            print(f"{rel}")


if __name__ == "__main__":
    main()

