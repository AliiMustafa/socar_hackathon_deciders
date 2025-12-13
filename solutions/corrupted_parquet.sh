#!/usr/bin/env bash
set -euo pipefail

DATA_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir) DATA_DIR="${2:-}"; shift 2;;
    *) echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

if [[ -z "$DATA_DIR" ]]; then
  echo "Usage: $0 --data-dir <path>" >&2
  exit 2
fi

mkdir -p processed_data/parquet_recovered
python3 scripts/recover_parquet.py --data-dir "$DATA_DIR" --out-dir processed_data/parquet_recovered
