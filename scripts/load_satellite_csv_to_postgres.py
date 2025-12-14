#!/usr/bin/env python3

import csv
import argparse
from pathlib import Path
import psycopg2

def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def infer_type(col: str) -> str:
    c = col.lower()
    # Treat common datetime columns as timestamptz
    if c in {"load_dts", "timestamp"} or c.endswith("_dts"):
        return "timestamptz"
    return "text"

def main():
    ap = argparse.ArgumentParser(description="Create table from satellite CSV header and load via COPY.")
    ap.add_argument("--dsn", required=True, help="postgresql://user:pass@host:5432/db")
    ap.add_argument("--schema", default="raw_vault")
    ap.add_argument("--table", required=True)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    # Read header
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

    schema_q = quote_ident(args.schema)
    table_q = quote_ident(args.table)
    full_name = f"{schema_q}.{table_q}"

    col_defs = ",\n  ".join(f"{quote_ident(c)} {infer_type(c)}" for c in header)
    create_sql = f"CREATE TABLE IF NOT EXISTS {full_name} (\n  {col_defs}\n);"

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_q};")
    cur.execute(create_sql)

    if args.truncate:
        cur.execute(f"TRUNCATE TABLE {full_name};")

    cols_list = ", ".join(quote_ident(c) for c in header)
    copy_sql = f"COPY {full_name} ({cols_list}) FROM STDIN WITH (FORMAT csv, HEADER true);"

    with csv_path.open("r", encoding="utf-8") as f:
        cur.copy_expert(copy_sql, f)

    cur.close()
    conn.close()

    print(f"✅ Loaded {csv_path} into {args.schema}.{args.table}")

if __name__ == "__main__":
    main()

