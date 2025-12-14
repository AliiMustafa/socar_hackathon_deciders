from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from raw_vault.pipeline import scan_files, diff_against_manifest, mark_missing, process_and_load

with DAG(
    dag_id="raw_vault_incremental_ingest",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["raw_vault"],
) as dag:

    @task
    def t_scan():
        return scan_files()

    @task
    def t_diff(files):
        return diff_against_manifest(files)

    @task
    def t_mark_missing(diff):
        return mark_missing(diff["missing"])

    @task
    def t_process(diff):
        return process_and_load(diff["to_process"])

    files = t_scan()
    diff = t_diff(files)
    t_mark_missing(diff)
    t_process(diff)

