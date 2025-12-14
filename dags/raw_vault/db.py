from airflow.providers.postgres.hooks.postgres import PostgresHook

from .config import POSTGRES_CONN_ID, MANIFEST_TABLE, REJECT_TABLE

def get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

def ensure_support_tables():
    hook = get_hook()
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw_vault;
            """)
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
              file_path text PRIMARY KEY,
              file_name text,
              source_group text,
              sha256 text,
              file_mtime timestamptz,
              last_seen_dts timestamptz,
              status text
            );
            """)
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {REJECT_TABLE} (
              rejected_dts timestamptz,
              rule_name text,
              reason text,
              record_source text,
              payload jsonb
            );
            """)
        conn.commit()

