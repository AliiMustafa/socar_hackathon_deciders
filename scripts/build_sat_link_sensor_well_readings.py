#!/usr/bin/env python3

import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import hashlib

# Project root (robust paths)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

SGX_DIR = PROJECT_ROOT / "processed_data/sgx_parquet"
RECOVERED_DIR = PROJECT_ROOT / "processed_data/parquet_recovered"

OUT_DIR = PROJECT_ROOT / "processed_data/raw_vault/sats"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_SAT = OUT_DIR / "sat_link_sensor_well_readings.csv"

PARENT_KEYS = ["sensor_id", "well_id"]
EVENT_TS_COL = "timestamp"

# Helpers
def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def clean_str_series(s):
    return s.astype("string").str.strip()

# Main
def main():
    parquet_files = sorted(list(SGX_DIR.glob("*.parquet")) + list(RECOVERED_DIR.glob("*.parquet")))

    if not parquet_files:
        raise FileNotFoundError("No parquet files found in sgx_parquet or parquet_recovered")

    print("Parquet files found:", [p.name for p in parquet_files])

    parts = []
    load_dts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    skipped = []

    for fp in parquet_files:
        df = pd.read_parquet(fp)
        src = fp.name

        missing = [c for c in (PARENT_KEYS + [EVENT_TS_COL]) if c not in df.columns]
        if missing:
            skipped.append((src, f"missing {missing}"))
            continue

        for k in PARENT_KEYS:
            df[k] = clean_str_series(df[k])

        df = df.dropna(subset=PARENT_KEYS + [EVENT_TS_COL])
        for k in PARENT_KEYS:
            df = df[df[k] != ""]

        payload_cols = [c for c in df.columns if c not in PARENT_KEYS]

        sat = df[PARENT_KEYS + payload_cols].copy()

        sat["load_dts"] = load_dts
        sat["record_source"] = src
        sat["source_file_checksum"] = sha256_of_file(fp)

        parts.append(sat)

    if not parts:
        print("No satellite data created. All files were skipped.")
        if skipped:
            print("\nSkipped files:")
            for name, reason in skipped[:20]:
                print(f"- {name}: {reason}")
        return

    sat_all = pd.concat(parts, ignore_index=True).drop_duplicates()
    sat_all.to_csv(OUT_SAT, index=False)

    print(f"Saved satellite: {OUT_SAT}")
    print("Rows:", len(sat_all))
    print("Columns:", len(sat_all.columns))

    if skipped:
        print("\n⚠️Skipped files (showing up to 20):")
        for name, reason in skipped[:20]:
            print(f"- {name}: {reason}")

if __name__ == "__main__":
    main()

