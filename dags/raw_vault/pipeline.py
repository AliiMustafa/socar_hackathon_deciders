import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

from .config import PARQUET_DIRS, MANIFEST_TABLE, REJECT_TABLE
from .utils import sha256_of_file, utc_now_iso
from .db import get_hook, ensure_support_tables
from .rules import apply_all_rules

def scan_files() -> list[dict]:
    out = []
    for group, d in PARQUET_DIRS:
        if not d.exists():
            continue
        for fp in sorted(d.glob("*.parquet")):
            stat = fp.stat()
            out.append({
                "file_path": str(fp),
                "file_name": fp.name,
                "source_group": group,
                "sha256": sha256_of_file(fp),
                "file_mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            })
    return out

def diff_against_manifest(files: list[dict]) -> dict:
    ensure_support_tables()
    hook = get_hook()

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT file_path, sha256 FROM {MANIFEST_TABLE};")
            existing = {row[0]: row[1] for row in cur.fetchall()}

    current_paths = set()
    to_process = []
    for f in files:
        current_paths.add(f["file_path"])
        old_sha = existing.get(f["file_path"])
        if old_sha is None or old_sha != f["sha256"]:
            to_process.append(f)

    missing = [p for p in existing.keys() if p not in current_paths]
    return {"to_process": to_process, "missing": missing}

def mark_missing(missing_paths: list[str]) -> None:
    if not missing_paths:
        return
    hook = get_hook()
    now = utc_now_iso()

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for p in missing_paths:
                cur.execute(
                    f"UPDATE {MANIFEST_TABLE} SET status='missing', last_seen_dts=%s WHERE file_path=%s;",
                    (now, p),
                )
        conn.commit()

def _insert_rejects(rejected_rows: list[dict]) -> None:
    if not rejected_rows:
        return
    hook = get_hook()
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for r in rejected_rows:
                cur.execute(
                    f"INSERT INTO {REJECT_TABLE} (rejected_dts, rule_name, reason, record_source, payload) "
                    f"VALUES (%s,%s,%s,%s,%s::jsonb);",
                    (r["rejected_dts"], r["rule_name"], r["reason"], r["record_source"], json.dumps(r["payload"])),
                )
        conn.commit()

def _upsert_manifest(f: dict) -> None:
    hook = get_hook()
    now = utc_now_iso()
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            INSERT INTO {MANIFEST_TABLE} (file_path, file_name, source_group, sha256, file_mtime, last_seen_dts, status)
            VALUES (%s,%s,%s,%s,%s,%s,'active')
            ON CONFLICT (file_path) DO UPDATE SET
              sha256 = EXCLUDED.sha256,
              file_mtime = EXCLUDED.file_mtime,
              last_seen_dts = EXCLUDED.last_seen_dts,
              status = 'active';
            """, (f["file_path"], f["file_name"], f["source_group"], f["sha256"], f["file_mtime"], now))
        conn.commit()

def process_and_load(to_process: list[dict]) -> None:
    if not to_process:
        return

    ensure_support_tables()
    hook = get_hook()

    target_table = "raw_vault.sat_link_sensor_well_readings"

    for f in to_process:
        fp = Path(f["file_path"])
        record_source = f["file_name"]

        df = pd.read_parquet(fp)

        valid_df, rejected = apply_all_rules(df, record_source)
        _insert_rejects(rejected)

        if valid_df.empty:
            _upsert_manifest(f)
            continue
        if "load_dts" not in valid_df.columns:
            valid_df["load_dts"] = utc_now_iso()
        if "record_source" not in valid_df.columns:
            valid_df["record_source"] = record_source

        cols = list(valid_df.columns)
        col_sql = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join(["%s"] * len(cols))

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for row in valid_df.itertuples(index=False, name=None):
                    cur.execute(
                        f'INSERT INTO {target_table} ({col_sql}) VALUES ({placeholders});',
                        row
                    )
            conn.commit()

        _upsert_manifest(f)

