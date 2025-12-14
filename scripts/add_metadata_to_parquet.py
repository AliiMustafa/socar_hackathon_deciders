#!/usr/bin/env python3

import pandas as pd
import numpy as np
import re
from pathlib import Path

YEAR_REGEX = re.compile(r"19\d{2}")

def extract_year(filename: str) -> int:
    match = YEAR_REGEX.search(filename)
    if not match:
        raise ValueError(f"No 1900s year found in filename: {filename}")
    return int(match.group())

def main():
    sgx_dir = Path("processed_data/sgx_parquet")

    if not sgx_dir.exists():
        raise FileNotFoundError(f"Directory not found: {sgx_dir}")

    parquet_files = list(sgx_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"No parquet files found in {sgx_dir}")
        return

    for fp in parquet_files:
        print(f"Processing {fp.name}")

        year = extract_year(fp.name)
        df = pd.read_parquet(fp)

        # overwrite / add metadata columns
        df["timestamp"] = pd.Timestamp(year=year, month=1, day=1)
        df["sensor_id"] = np.nan

        df.to_parquet(fp, index=False)

    print(f"Finished processing SGX decoded parquet files")

if __name__ == "__main__":
    main()

